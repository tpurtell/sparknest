// Download, upload, delete and new-folder, shared by the files list, the
// treemap's context menu and anywhere a path is shown.

import { authHeader, downloadUrl, post } from "./api";
import { toast } from "./state.svelte";
import { confirm } from "./ui.svelte";
import { human } from "./format";

export function download(path: string) {
  const a = document.createElement("a");
  a.href = downloadUrl(path);
  a.download = "";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

export async function remove(path: string, dir: boolean): Promise<boolean> {
  let body = "This cannot be undone.";
  if (dir) {
    try {
      const r = await post<{ files: number; dirs: number }>("/v1/rm", { path, recursive: true, dry_run: true });
      body = `${r.files.toLocaleString()} files in ${r.dirs.toLocaleString()} folders, on every host that holds them. This cannot be undone.`;
    } catch {
      /* show the generic warning */
    }
  }
  if (!(await confirm(`Delete ${path}?`, body, "Delete", true))) return false;
  try {
    const r = await post<{ files: number; dirs: number; errors: string[] }>("/v1/rm", { path, recursive: dir });
    toast(`Deleted ${r.files} files${r.dirs ? `, ${r.dirs} folders` : ""}${r.errors.length ? ` (${r.errors.length} errors)` : ""}`, r.errors.length > 0);
    return true;
  } catch (e) {
    toast((e as Error).message, true);
    return false;
  }
}

export async function mkdir(path: string) {
  try {
    await post("/v1/mkdir", { path });
    return true;
  } catch (e) {
    toast((e as Error).message, true);
    return false;
  }
}

export interface Upload {
  id: number;
  name: string;
  size: number;
  sent: number;
  state: "queued" | "sending" | "done" | "failed";
  error?: string;
  xhr?: XMLHttpRequest;
}

export const uploads = $state<Upload[]>([]);
let nextId = 0;
let active = 0;
const PARALLEL = 3;

/** Queue files for `dir`; `rel` gives each file's path below it (folders). */
export function upload(files: File[], dir: string, rel: (f: File) => string = (f) => f.name, overwrite = false, done?: () => void) {
  const base = dir === "/" ? "" : dir.replace(/\/$/, "");
  for (const f of files) {
    const u: Upload = { id: ++nextId, name: base + "/" + rel(f), size: f.size, sent: 0, state: "queued" };
    uploads.push(u);
    queue.push({ u, f, overwrite, done });
  }
  pump();
}

const queue: { u: Upload; f: File; overwrite: boolean; done?: () => void }[] = [];

async function pump() {
  while (active < PARALLEL && queue.length) {
    const job = queue.shift()!;
    active++;
    send(job).finally(() => {
      active--;
      pump();
    });
  }
}

async function send({ u, f, overwrite, done }: (typeof queue)[number]) {
  const live = uploads.find((x) => x.id === u.id)!;
  // Folders first (a folder upload's paths may be new).
  const dir = u.name.slice(0, u.name.lastIndexOf("/"));
  if (dir) await post("/v1/mkdir", { path: dir }).catch(() => {});
  live.state = "sending";
  await new Promise<void>((resolve) => {
    const xhr = new XMLHttpRequest();
    live.xhr = xhr;
    xhr.open("PUT", `/v1/upload?path=${encodeURIComponent(u.name)}${overwrite ? "&overwrite=true" : ""}`);
    xhr.setRequestHeader("Authorization", authHeader());
    xhr.upload.onprogress = (e) => (live.sent = e.loaded);
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        live.state = "done";
        live.sent = u.size;
      } else {
        live.state = "failed";
        try {
          live.error = JSON.parse(xhr.responseText).error;
        } catch {
          live.error = xhr.statusText;
        }
      }
      resolve();
    };
    xhr.onerror = () => {
      live.state = "failed";
      live.error = "network error";
      resolve();
    };
    xhr.onabort = () => {
      live.state = "failed";
      live.error = "cancelled";
      resolve();
    };
    xhr.send(f);
  });
  live.xhr = undefined;
  if (!queue.length && !uploads.some((x) => x.state === "sending")) {
    const ok = uploads.filter((x) => x.state === "done").length;
    const failed = uploads.filter((x) => x.state === "failed").length;
    toast(`Uploaded ${ok} file${ok === 1 ? "" : "s"} (${human(uploads.reduce((a, x) => a + (x.state === "done" ? x.size : 0), 0))})${failed ? `, ${failed} failed` : ""}`, failed > 0);
    done?.();
  }
}

export function clearFinished() {
  for (let i = uploads.length - 1; i >= 0; i--) if (uploads[i].state === "done" || uploads[i].state === "failed") uploads.splice(i, 1);
}
