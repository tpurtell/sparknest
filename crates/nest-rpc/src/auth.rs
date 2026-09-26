//! Mutual challenge-response over the cluster secret.
//!
//! ```text
//! client -> server  Hello   { cluster, node, nonce_c }
//! server -> client  Chal    { node, nonce_s, mac_s = H(secret, "S" | cluster | nonce_c | nonce_s | server) }
//! client -> server  Proof   { mac_c = H(secret, "C" | cluster | nonce_s | nonce_c | client) }
//! server -> client  Ready
//! ```

use hmac::{Hmac, Mac};
use nest_types::NodeId;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

#[derive(Serialize, Deserialize)]
struct Hello {
    cluster: String,
    node: NodeId,
    nonce: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct Chal {
    node: NodeId,
    nonce: [u8; 32],
    mac: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Proof {
    mac: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
enum Ready {
    Ok,
    Denied(String),
}

fn mac(secret: &[u8], tag: &[u8], cluster: &str, a: &[u8], b: &[u8], node: NodeId) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    m.update(tag);
    m.update(cluster.as_bytes());
    m.update(a);
    m.update(b);
    m.update(&node.0.to_le_bytes());
    m.finalize().into_bytes().to_vec()
}

fn verify(
    secret: &[u8],
    tag: &[u8],
    cluster: &str,
    a: &[u8],
    b: &[u8],
    node: NodeId,
    got: &[u8],
) -> bool {
    let mut m = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    m.update(tag);
    m.update(cluster.as_bytes());
    m.update(a);
    m.update(b);
    m.update(&node.0.to_le_bytes());
    m.verify_slice(got).is_ok()
}

async fn send<T: Serialize>(s: &mut TcpStream, v: &T) -> std::io::Result<()> {
    let body = postcard::to_stdvec(v).map_err(std::io::Error::other)?;
    s.write_u32_le(body.len() as u32).await?;
    s.write_all(&body).await
}

async fn recv<T: serde::de::DeserializeOwned>(s: &mut TcpStream) -> std::io::Result<T> {
    let n = s.read_u32_le().await? as usize;
    if n > 4096 {
        return Err(std::io::Error::other("handshake frame too large"));
    }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).await?;
    postcard::from_bytes(&buf).map_err(std::io::Error::other)
}

/// Client side. Returns the authenticated server node id.
pub async fn client(
    s: &mut TcpStream,
    secret: &[u8],
    cluster: &str,
    me: NodeId,
) -> std::io::Result<NodeId> {
    let nonce_c: [u8; 32] = rand::random();
    send(
        s,
        &Hello {
            cluster: cluster.into(),
            node: me,
            nonce: nonce_c,
        },
    )
    .await?;
    let chal: Chal = recv(s).await?;
    if !verify(
        secret,
        b"S",
        cluster,
        &nonce_c,
        &chal.nonce,
        chal.node,
        &chal.mac,
    ) {
        return Err(std::io::Error::other("server failed authentication"));
    }
    send(
        s,
        &Proof {
            mac: mac(secret, b"C", cluster, &chal.nonce, &nonce_c, me),
        },
    )
    .await?;
    match recv::<Ready>(s).await? {
        Ready::Ok => Ok(chal.node),
        Ready::Denied(why) => Err(std::io::Error::other(format!("server denied: {why}"))),
    }
}

/// Server side. Returns the authenticated client node id.
pub async fn server(
    s: &mut TcpStream,
    secret: &[u8],
    cluster: &str,
    me: NodeId,
) -> std::io::Result<NodeId> {
    let hello: Hello = recv(s).await?;
    if hello.cluster != cluster {
        send(s, &Ready::Denied("wrong cluster".into())).await.ok();
        return Err(std::io::Error::other(format!(
            "client from cluster {:?}",
            hello.cluster
        )));
    }
    let nonce_s: [u8; 32] = rand::random();
    send(
        s,
        &Chal {
            node: me,
            nonce: nonce_s,
            mac: mac(secret, b"S", cluster, &hello.nonce, &nonce_s, me),
        },
    )
    .await?;
    let proof: Proof = recv(s).await?;
    if !verify(
        secret,
        b"C",
        cluster,
        &nonce_s,
        &hello.nonce,
        hello.node,
        &proof.mac,
    ) {
        send(s, &Ready::Denied("authentication failed".into()))
            .await
            .ok();
        return Err(std::io::Error::other("client failed authentication"));
    }
    send(s, &Ready::Ok).await?;
    Ok(hello.node)
}
