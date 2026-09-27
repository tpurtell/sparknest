// Actions on a selection (a path or hf:org/name), shared by the treemap's
// context menu, model details and the files list.

import { post, api, type TreeNode } from "./api";
import { app, startJob, targets, toast, go } from "./state.svelte";
import { pick, inform, confirm, openMenu, openPlace, type MenuItem } from "./ui.svelte";
import { human, selLabel } from "./format";
import { download, remove as deletePath } from "./files.svelte";

export async function copyTo(selector: string, hosts?: string[]) {
  const to =
    hosts ??
    (await pick(
      `Copy ${selLabel(selector)} to…`,
      targets().map((t) => ({ name: t.name, kind: t.kind === "host" ? "host" : "archive" })),
    ));
  if (!to?.length) return;
  await startJob("/v1/replicate", { selector, hosts: to }, `Copying ${selLabel(selector)} to ${to.join(", ")}`);
}

/** Delete a Hugging Face repo from the hub, every copy: `hf cache rm` on
 *  the daemon's host (it knows which shared blobs other repos use). */
export async function deleteModel(selector: string) {
  const label = selLabel(selector);
  let d: { repos?: number; revisions?: number; size?: string };
  try {
    d = (await post<{ dry_run: typeof d }>("/v1/hf/remove", { targets: [selector], dry_run: true })).dry_run;
  } catch (e) {
    toast((e as Error).message, true);
    return;
  }
  if (!d?.repos) {
    await inform(`Nothing to delete`, `hf finds no ${label} in the hub.`);
    return;
  }
  const ok = await confirm(
    `Delete ${label}?`,
    `hf removes ${d.revisions ?? 1} revision${d.revisions === 1 ? "" : "s"} (${d.size ?? "?"}) from every host and archive store. This cannot be undone.`,
    "Delete",
    true,
  );
  if (!ok) return;
  await startJob("/v1/hf/remove", { targets: [selector] }, `Deleting ${label}`);
  go("models");
}

export async function removeFrom(selector: string, hosts?: string[]) {
  const from =
    hosts ??
    (await pick(
      `Remove ${selLabel(selector)} from…`,
      targets().map((t) => ({ name: t.name, kind: t.kind === "host" ? "host" : "archive" })),
      true,
      "The last copy of a file is never removed.",
    ));
  if (!from?.length) return;
  // Removing a copy never loses data (the last copy always stays), so it
  // happens at once; what was kept is explained.
  try {
    const v = await post<{ hosts: { host: string; removed: number; refused: [number, string][] }[] }>("/v1/evict", {
      selector,
      hosts: from,
    });
    const kept = v.hosts.filter((r) => r.refused.length);
    const removed = v.hosts.reduce((a, r) => a + r.removed, 0);
    if (removed) toast(`Removed ${removed} file${removed === 1 ? "" : "s"} of ${selLabel(selector)} from ${v.hosts.filter((r) => r.removed).map((r) => r.host).join(", ")}`);
    if (kept.length) {
      const n = kept.reduce((a, r) => a + r.refused.length, 0);
      await inform(
        removed ? `Kept ${n} file${n === 1 ? "" : "s"}` : `Not removed from ${kept.map((r) => r.host).join(", ")}`,
        `${n === 1 ? "That file is" : "Those files are"} the last copy anywhere in sparknest, so ${n === 1 ? "it stays" : "they stay"}. Copy ${selLabel(selector)} somewhere else (or offload it to an archive) first.`,
      );
    }
  } catch (e) {
    toast((e as Error).message, true);
  }
}

export async function keepOn(selector: string) {
  const hosts = await pick(
    `Keep ${selLabel(selector)} on…`,
    [{ name: "@all", kind: "group" }, ...app.groups.map((g) => ({ name: "@" + g.name, kind: "group" })), ...targets().map((t) => ({ name: t.name, kind: t.kind }))],
    true,
    "Makes an automatic rule: copies follow new files, and plans never remove them from these places.",
  );
  if (!hosts?.length) return;
  const name = selLabel(selector).replace(/[^a-zA-Z0-9_-]+/g, "-").replace(/^-|-$/g, "").slice(0, 40) || "rule";
  try {
    await api("PUT", "/v1/rules/" + encodeURIComponent(name), { selector, hosts, auto: true });
    toast(`Rule ${name}: ${selLabel(selector)} → ${hosts.join(", ")}`);
  } catch (e) {
    toast((e as Error).message, true);
  }
}

/** The context menu for a treemap block (or any node with a selector). */
export function nodeMenu(n: TreeNode, x: number, y: number, scope: string | null, zoom?: () => void) {
  const sel = n.selector;
  const sub = `${human(n.bytes)}${n.files > 1 ? ` · ${n.files.toLocaleString()} files` : ""}${n.hosts?.length ? ` · on ${n.hosts.join(", ")}` : ""}`;
  if (n.kind === "free") {
    return openMenu(x, y, n.name, sub, [
      { label: "Plan to make room here", icon: "plans", run: () => go("plans", "", { host: n.name.replace(/^free on /, "") }) },
    ]);
  }
  if (!sel) return openMenu(x, y, n.name, sub, [{ label: "Nothing to act on here", disabled: true }]);
  const isHost = scope && app.status?.nodes.some((h) => h.name === scope);
  const items: MenuItem[] = [];
  if (zoom && (n.children?.length || n.truncated)) items.push({ label: "Zoom in", icon: "zoom", run: zoom });
  if (n.kind === "repo") items.push({ label: "Model details", icon: "info", run: () => go("models", sel) });
  items.push(
    { label: "Place…", icon: "target", hint: "hosts, replicas, archive", run: () => openPlace(sel, n.kind === "repo" ? n.name : selLabel(sel)) },
    { label: "Copy to…", icon: "copy", run: () => copyTo(sel) },
    { label: "Keep on… (rule)", icon: "pin", run: () => keepOn(sel) },
    { sep: true, label: "" },
  );
  if (isHost) items.push({ label: `Remove from ${scope}`, icon: "trash", danger: true, run: () => removeFrom(sel, [scope!]) });
  items.push({ label: "Remove from…", icon: "trash", danger: true, run: () => removeFrom(sel) });
  if (n.path) {
    const dir = n.kind === "dir" || n.kind === "group";
    items.push(
      { sep: true, label: "" },
      { label: dir ? "Download as .tar" : "Download", icon: "up", run: () => download(n.path!) },
      { label: "Show in files", icon: "files", run: () => go("files", n.kind === "file" ? n.path!.slice(0, n.path!.lastIndexOf("/")) || "/" : n.path!) },
      { label: "Delete everywhere…", icon: "trash", danger: true, run: () => deletePath(n.path!, n.kind !== "file") },
    );
  }
  openMenu(x, y, n.name, sub, items);
}
