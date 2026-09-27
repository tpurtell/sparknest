//! Moving bytes through the API for the web UI: download a file (streamed),
//! download a directory as a tar (streamed, built on the fly), upload a
//! file (streamed into a new file), make a directory. Nothing is buffered
//! whole, so multi-gigabyte files are fine.

use crate::{Api, ApiError, R, bad};
use axum::Json;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use nest_data::Vfs;
use nest_meta::query;
use nest_place::selector;
use nest_types::{FileId, FileKind, NestError};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

const CHUNK: u32 = 4 << 20;

#[derive(Deserialize)]
pub(crate) struct PathQ {
    path: String,
    #[serde(default)]
    overwrite: bool,
}

fn ascii_name(n: &str) -> String {
    n.chars()
        .map(|c| {
            if c.is_ascii_graphic() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn disposition(name: &str) -> HeaderValue {
    // RFC 6266: a plain fallback plus the UTF-8 name.
    let enc: String = name
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{enc}",
        ascii_name(name)
    ))
    .unwrap_or(HeaderValue::from_static("attachment"))
}

type Tx = tokio::sync::mpsc::Sender<Result<Vec<u8>, std::io::Error>>;

fn body_from(rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, std::io::Error>>) -> Body {
    Body::from_stream(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (b, rx))
    }))
}

/// Send `file`'s bytes into `tx`, `CHUNK` at a time.
async fn pump(vfs: &Vfs, file: FileId, size: u64, tx: &Tx) -> Result<(), NestError> {
    let (fh, _) = vfs.open(file, 0).await?;
    let mut off = 0u64;
    let r = async {
        while off < size {
            let b = vfs.read(fh, off, CHUNK).await?;
            if b.is_empty() {
                break;
            }
            off += b.len() as u64;
            if tx.send(Ok(b)).await.is_err() {
                break; // the client went away
            }
        }
        Ok(())
    }
    .await;
    vfs.release(fh, None).await;
    r
}

pub(crate) async fn download(State(api): State<Api>, Query(q): Query<PathQ>) -> Response {
    match download_inner(api, q).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn download_inner(api: Api, q: PathQ) -> Result<Response, ApiError> {
    let path = api.ns(&q.path);
    let c = api.conn()?;
    let (id, _) = selector::resolve_path(&c, &path)?;
    let a = query::getattr(&c, id)
        .map_err(|e| bad(e.to_string()))?
        .ok_or(ApiError(StatusCode::NOT_FOUND, path.clone()))?;
    drop(c);
    let name = path
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or("sparknest")
        .to_string();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let vfs = api.vfs.clone();
    let mut resp = match a.kind {
        FileKind::Regular => {
            let size = a.size;
            tokio::spawn(async move {
                if let Err(e) = pump(&vfs, id, size, &tx).await {
                    let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                }
            });
            let mut r = body_from(rx).into_response();
            r.headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from(size));
            r.headers_mut()
                .insert(header::CONTENT_DISPOSITION, disposition(&name));
            r
        }
        FileKind::Directory => {
            tokio::spawn(async move {
                if let Err(e) = tar_dir(&vfs, id, &name, &tx).await {
                    let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                }
            });
            let mut r = body_from(rx).into_response();
            r.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                disposition(&format!(
                    "{}.tar",
                    path.rsplit('/')
                        .find(|s| !s.is_empty())
                        .unwrap_or("sparknest")
                )),
            );
            r.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-tar"),
            );
            r
        }
        FileKind::Symlink => return Err(bad("that is a symlink; download what it points to")),
    };
    if a.kind == FileKind::Regular {
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
    }
    Ok(resp)
}

// ---------------------------------------------------------------- tar

fn octal(field: &mut [u8], v: u64) {
    let s = format!("{:0w$o}", v, w = field.len() - 1);
    if s.len() < field.len() {
        field[..s.len()].copy_from_slice(s.as_bytes());
        field[s.len()] = 0;
    } else {
        // GNU base-256 for values that do not fit (files of 8 GiB and up).
        field.fill(0);
        field[0] = 0x80;
        let b = v.to_be_bytes();
        let n = field.len();
        field[n - 8..].copy_from_slice(&b);
    }
}

fn header(name: &[u8], size: u64, mode: u32, mtime: u64, kind: u8, link: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // GNU long names/links precede the entry that needs them.
    let long = |out: &mut Vec<u8>, data: &[u8], t: u8| {
        let mut data = data.to_vec();
        data.push(0);
        out.extend(header(
            b"././@LongLink",
            data.len() as u64,
            0o644,
            0,
            t,
            b"",
        ));
        let pad = (512 - data.len() % 512) % 512;
        out.extend(data);
        out.extend(std::iter::repeat_n(0, pad));
    };
    if link.len() > 100 {
        long(&mut out, link, b'K');
    }
    if name.len() > 100 {
        long(&mut out, name, b'L');
    }
    let mut h = [0u8; 512];
    let n = name.len().min(100);
    h[..n].copy_from_slice(&name[..n]);
    octal(&mut h[100..108], mode as u64);
    octal(&mut h[108..116], 0);
    octal(&mut h[116..124], 0);
    octal(&mut h[124..136], size);
    octal(&mut h[136..148], mtime);
    h[148..156].fill(b' ');
    h[156] = kind;
    let l = link.len().min(100);
    h[157..157 + l].copy_from_slice(&link[..l]);
    h[257..263].copy_from_slice(b"ustar ");
    h[263..265].copy_from_slice(b" \0");
    let sum: u32 = h.iter().map(|b| *b as u32).sum();
    let s = format!("{sum:06o}\0 ");
    h[148..156].copy_from_slice(s.as_bytes());
    out.extend_from_slice(&h);
    out
}

async fn tar_dir(vfs: &Arc<Vfs>, root: FileId, root_name: &str, tx: &Tx) -> Result<(), NestError> {
    let mut stack = vec![(root, root_name.to_string())];
    while let Some((dir, prefix)) = stack.pop() {
        let a = vfs.getattr(dir).await?;
        let mtime = (a.mtime.0.max(0) / 1_000_000_000) as u64;
        if tx
            .send(Ok(header(
                format!("{prefix}/").as_bytes(),
                0,
                a.perm,
                mtime,
                b'5',
                b"",
            )))
            .await
            .is_err()
        {
            return Ok(());
        }
        let mut after = 0;
        loop {
            let batch = vfs.readdir(dir, after, 1024)?;
            if batch.is_empty() {
                break;
            }
            for (cookie, e) in batch {
                after = cookie;
                if e.name == b"." || e.name == b".." {
                    continue;
                }
                let name = format!("{prefix}/{}", String::from_utf8_lossy(&e.name));
                let a = vfs.getattr(e.id).await?;
                let mtime = (a.mtime.0.max(0) / 1_000_000_000) as u64;
                match a.kind {
                    FileKind::Directory => stack.push((e.id, name)),
                    FileKind::Symlink => {
                        let t = vfs.readlink(e.id)?;
                        if tx
                            .send(Ok(header(name.as_bytes(), 0, 0o777, mtime, b'2', &t)))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    FileKind::Regular => {
                        if tx
                            .send(Ok(header(
                                name.as_bytes(),
                                a.size,
                                a.perm,
                                mtime,
                                b'0',
                                b"",
                            )))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                        pump(vfs, e.id, a.size, tx).await?;
                        let pad = ((512 - a.size % 512) % 512) as usize;
                        if pad > 0 && tx.send(Ok(vec![0; pad])).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    let _ = tx.send(Ok(vec![0; 1024])).await;
    Ok(())
}

// ---------------------------------------------------------------- upload

/// PUT /v1/upload?path=/dir/name: the request body becomes that file.
pub(crate) async fn upload(
    State(api): State<Api>,
    Query(q): Query<PathQ>,
    body: Body,
) -> R<serde_json::Value> {
    use futures::StreamExt;
    let path = api.ns(&q.path);
    let (dir, name) = path
        .rsplit_once('/')
        .filter(|(_, n)| !n.is_empty())
        .ok_or_else(|| bad("give the file's full path"))?;
    let parent = {
        let c = api.conn()?;
        selector::resolve_path(&c, if dir.is_empty() { "/" } else { dir })?.0
    };
    let vfs = api.vfs.clone();
    let flags = nest_data::vfs::oflags::WRONLY;
    let existing = {
        let c = api.conn()?;
        query::lookup(&c, parent, name.as_bytes()).map_err(|e| bad(e.to_string()))?
    };
    let fh = match existing {
        Some(_) if !q.overwrite => {
            return Err(ApiError(StatusCode::CONFLICT, format!("{path} exists")));
        }
        Some(id) => vfs.open(id, flags | nest_data::vfs::oflags::TRUNC).await?.0,
        None => match vfs.create(parent, name.as_bytes(), 0o644, flags).await {
            Ok((_, fh, _)) => fh,
            Err(NestError::Exists) => {
                return Err(ApiError(StatusCode::CONFLICT, format!("{path} exists")));
            }
            Err(e) => return Err(e.into()),
        },
    };
    let mut off = 0u64;
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK as usize);
    let r: Result<(), ApiError> = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| bad(format!("upload interrupted: {e}")))?;
            buf.extend_from_slice(&chunk);
            if buf.len() >= CHUNK as usize {
                let n = buf.len() as u64;
                vfs.write(fh, off, std::mem::take(&mut buf)).await?;
                off += n;
            }
        }
        if !buf.is_empty() {
            let n = buf.len() as u64;
            vfs.write(fh, off, std::mem::take(&mut buf)).await?;
            off += n;
        }
        Ok(())
    }
    .await;
    vfs.release(fh, None).await;
    r?;
    tracing::info!(%path, bytes = off, "uploaded");
    Ok(Json(json!({ "path": path, "bytes": off })))
}

#[derive(Deserialize)]
pub(crate) struct MkdirReq {
    path: String,
}

pub(crate) async fn mkdir(State(api): State<Api>, Json(r): Json<MkdirReq>) -> R<serde_json::Value> {
    let path = api.ns(&r.path);
    let mut id = FileId::ROOT;
    for comp in path.split('/').filter(|c| !c.is_empty()) {
        let existing = {
            let c = api.conn()?;
            query::lookup(&c, id, comp.as_bytes()).map_err(|e| bad(e.to_string()))?
        };
        id = match existing {
            Some(i) => i,
            None => api.vfs.mkdir(id, comp.as_bytes(), 0o755).await?.id,
        };
    }
    Ok(Json(json!({ "path": path })))
}
