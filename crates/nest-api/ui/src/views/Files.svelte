<script lang="ts">
  import { get, post } from "../lib/api";
  import { app, route, go, toast, singleFlight } from "../lib/state.svelte";
  import { human } from "../lib/format";
  import { nodeMenu } from "../lib/actions";
  import { download, remove, mkdir, upload } from "../lib/files.svelte";
  import Icon from "../components/Icon.svelte";

  interface Entry { name: string; kind: string; size: number; sealed: boolean; writing_on?: string; hosts: string[]; target?: string }
  const path = $derived(route.arg || "/");
  let entries = $state<Entry[]>([]);
  let err = $state("");
  let drop = $state(false);
  let picker: HTMLInputElement;
  let folderPicker: HTMLInputElement;
  async function load() {
    try {
      entries = (await get<{ entries: Entry[] }>("/v1/ls?path=" + encodeURIComponent(path))).entries;
      err = "";
    } catch (e) {
      err = (e as Error).message;
    }
  }
  const refresh = singleFlight(load);
  $effect(() => {
    void path;
    void app.changed;
    refresh();
  });
  const full = (n: string) => (path === "/" ? "" : path) + "/" + n;
  const parts = $derived(path.split("/").filter(Boolean));
  const isStore = (h: string) => app.stores.some((s) => s.name === h);
  const sorted = $derived(entries.slice().sort((a, b) => (a.kind === "directory" ? 0 : 1) - (b.kind === "directory" ? 0 : 1) || a.name.localeCompare(b.name)));

  async function seal(e: Entry) {
    try {
      await post("/v1/seal", { path: full(e.name), sealed: !e.sealed });
      load();
    } catch (x) {
      toast((x as Error).message, true);
    }
  }
  const menu = (ev: MouseEvent, e: Entry) => {
    ev.preventDefault();
    nodeMenu({ name: e.name, kind: e.kind === "directory" ? "dir" : "file", selector: full(e.name), path: full(e.name), bytes: e.size, files: 1, hosts: e.hosts }, ev.clientX, ev.clientY, null);
  };
  async function newFolder() {
    const name = prompt("New folder name");
    if (name && (await mkdir(full(name)))) load();
  }
  function pickFiles(input: HTMLInputElement, folder: boolean) {
    const files = [...(input.files ?? [])];
    if (files.length) upload(files, path, folder ? (f) => (f as File & { webkitRelativePath: string }).webkitRelativePath || f.name : undefined, false, load);
    input.value = "";
  }
  // Drag and drop: files and whole folders.
  async function ondrop(e: DragEvent) {
    e.preventDefault();
    drop = false;
    const items = [...(e.dataTransfer?.items ?? [])];
    const out: { f: File; rel: string }[] = [];
    const walk = async (entry: any, prefix: string): Promise<void> => {
      if (entry.isFile) {
        const f: File = await new Promise((res, rej) => entry.file(res, rej));
        out.push({ f, rel: prefix + f.name });
      } else if (entry.isDirectory) {
        const reader = entry.createReader();
        let batch: any[];
        do {
          batch = await new Promise((res, rej) => reader.readEntries(res, rej));
          for (const c of batch) await walk(c, prefix + entry.name + "/");
        } while (batch.length);
      }
    };
    const roots = items.map((i) => i.webkitGetAsEntry?.()).filter(Boolean);
    if (roots.length) for (const r of roots) await walk(r, "");
    else for (const f of [...(e.dataTransfer?.files ?? [])]) out.push({ f, rel: f.name });
    if (!out.length) return;
    const rel = new Map(out.map((o) => [o.f, o.rel]));
    upload(out.map((o) => o.f), path, (f) => rel.get(f) ?? f.name, false, load);
  }
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="stack" ondragover={(e) => { e.preventDefault(); drop = true; }} ondragleave={(e) => { if (e.currentTarget === e.target) drop = false; }} {ondrop}>
  <div class="panel pad bar-top row">
    <div class="crumbs row">
      <button class="crumb" onclick={() => go("files", "/")}>/</button>
      {#each parts as p, i}<span class="faint">/</span><button class="crumb" onclick={() => go("files", "/" + parts.slice(0, i + 1).join("/"))}>{p}</button>{/each}
    </div>
    <span class="spacer"></span>
    <button class="btn sm" onclick={() => picker.click()}><Icon name="up" size={14} /> Upload files</button>
    <button class="btn sm" onclick={() => folderPicker.click()}><Icon name="files" size={14} /> Upload folder</button>
    <button class="btn sm ghost" onclick={newFolder}>New folder</button>
    {#if path !== "/"}<button class="btn sm ghost" onclick={() => download(path)} title="Download this folder as .tar"><Icon name="copy" size={14} /> .tar</button>{/if}
    <input type="file" multiple bind:this={picker} onchange={(e) => pickFiles(e.currentTarget, false)} hidden />
    <input type="file" multiple bind:this={folderPicker} onchange={(e) => pickFiles(e.currentTarget, true)} hidden webkitdirectory />
  </div>
  {#if err}<div class="panel pad" style="color:var(--bad)">{err}</div>{/if}
  <div class="panel scroll-x list" class:drop>
    {#if drop}<div class="dropnote">Drop to upload into {path}</div>{/if}
    <table class="t">
      <thead><tr><th>Name</th><th class="num">Size</th><th class="hide-s">Copies</th><th></th></tr></thead>
      <tbody>
        {#each sorted as e (e.name)}
          <tr oncontextmenu={(ev) => menu(ev, e)}>
            <td class="name">
              {#if e.kind === "directory"}<button class="lnk" onclick={() => go("files", full(e.name))}><Icon name="files" size={15} /> {e.name}/</button>
              {:else if e.kind === "symlink"}<span class="muted">{e.name} → {e.target}</span>
              {:else}{e.name} {#if e.sealed}<span class="badge">sealed</span>{/if}{/if}
            </td>
            <td class="num mono">{e.kind === "regular" ? human(e.size) : ""}</td>
            <td class="hide-s">{#if e.writing_on}<span class="badge warn">writing on {e.writing_on}</span>
              {:else}{#each e.hosts as h}<span class="badge" class:arch={isStore(h)}>{h}</span> {/each}{/if}</td>
            <td class="acts">
              {#if e.kind !== "symlink"}
                <button class="btn sm ghost" onclick={() => download(full(e.name))} title={e.kind === "directory" ? "Download as .tar" : "Download"}>⤓</button>
                <button class="btn sm ghost" onclick={async () => { if (await remove(full(e.name), e.kind === "directory")) load(); }} title="Delete"><Icon name="trash" size={14} /></button>
                <button class="btn sm ghost" onclick={(ev) => menu(ev, e)} title="More"><Icon name="more" size={15} /></button>
              {:else}
                <button class="btn sm ghost" onclick={async () => { if (await remove(full(e.name), false)) load(); }} title="Delete link"><Icon name="trash" size={14} /></button>
              {/if}
              {#if e.kind === "regular"}<button class="btn sm ghost hide-s" onclick={() => seal(e)}>{e.sealed ? "Unseal" : "Seal"}</button>{/if}
            </td>
          </tr>
        {:else}<tr><td colspan="4" class="empty">Empty. Drop files here to upload.</td></tr>{/each}
      </tbody>
    </table>
  </div>
</div>

<style>
  .bar-top { gap: 8px; }
  .crumbs { gap: 4px; min-width: 0; }
  .crumb { background: none; border: 0; color: var(--spark); cursor: pointer; padding: 2px 4px; }
  .lnk { background: none; border: 0; color: var(--text); cursor: pointer; display: inline-flex; gap: 6px; align-items: center; padding: 0; }
  .lnk:hover { color: var(--spark); }
  .name { word-break: break-all; }
  .acts { white-space: nowrap; text-align: right; }
  .arch { color: var(--violet); border-color: rgba(155, 123, 255, 0.4); }
  .list { position: relative; transition: border-color 0.15s, box-shadow 0.15s; }
  .list.drop { border-color: var(--spark); box-shadow: 0 0 30px rgba(56, 232, 255, 0.3); }
  .dropnote { position: absolute; inset: 0; display: grid; place-items: center; font-size: 18px; color: var(--spark); background: rgba(3, 12, 28, 0.75); z-index: 2; pointer-events: none; }
  @media (max-width: 899px) { .hide-s { display: none; } }
</style>
