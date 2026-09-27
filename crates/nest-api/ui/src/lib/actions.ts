// Actions on a selection (a path or hf:org/name), shared by the treemap's
// context menu, model details and the files list.

import { post, api, type TreeNode } from "./api";
import { app, startJob, targets, toast, go, poll } from "./state.svelte";
import { confirm, pick, openMenu, type MenuItem } from "./ui.svelte";
import { human, selLabel } from "./format";

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
  if (!(await confirm(`Remove ${selLabel(selector)} from ${from.join(", ")}?`, "The last copy of a file is never removed.", "Remove", true)))
    return;
  try {
    const v = await post<{ hosts: { host: string; removed: number; refused: [number, string][] }[] }>("/v1/evict", {
      selector,
      hosts: from,
    });
    const msg = v.hosts
      .map((r) => `${r.host}: removed ${r.removed}${r.refused.length ? `, kept ${r.refused.length} last copies` : ""}`)
      .join("; ");
    toast(msg);
    poll();
  } catch (e) {
    toast((e as Error).message, true);
  }
}

export async function offload(selector: string) {
  if (!app.stores.length) return toast("No archive stores yet", true);
  const s = await pick(
    `Offload ${selLabel(selector)} into…`,
    app.stores.map((s) => {
      const g = s.gateways.find(([, h]) => h.healthy)?.[1];
      return { name: s.name, kind: "archive", note: g ? `${human(g.free_bytes)} free` : "unreachable" };
    }),
    false,
    "Copies into the archive, then removes the live copies. Reads stream through a gateway; copy back any time.",
  );
  if (!s?.length) return;
  await startJob("/v1/offload", { selector, store: s[0] }, `Offloading ${selLabel(selector)} to ${s[0]}`);
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
    { label: "Copy to…", icon: "copy", run: () => copyTo(sel) },
    { label: "Copy to every host", icon: "sparkle", run: () => copyTo(sel, ["@all"]) },
    { label: "Keep on… (rule)", icon: "pin", run: () => keepOn(sel) },
    { label: "Offload to archive…", icon: "archive", run: () => offload(sel), disabled: !app.stores.length },
    { sep: true, label: "" },
  );
  if (isHost) items.push({ label: `Remove from ${scope}`, icon: "trash", danger: true, run: () => removeFrom(sel, [scope!]) });
  items.push({ label: "Remove from…", icon: "trash", danger: true, run: () => removeFrom(sel) });
  if (n.path) items.push({ sep: true, label: "" }, { label: "Show in files", icon: "files", run: () => go("files", n.path!) });
  openMenu(x, y, n.name, sub, items);
}
