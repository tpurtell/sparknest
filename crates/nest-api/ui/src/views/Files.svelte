<script lang="ts">
  import { get, post } from "../lib/api";
  import { app, route, go, toast } from "../lib/state.svelte";
  import { human } from "../lib/format";
  import { nodeMenu } from "../lib/actions";
  import Icon from "../components/Icon.svelte";

  interface Entry { name: string; kind: string; size: number; sealed: boolean; writing_on?: string; hosts: string[]; target?: string }
  const path = $derived(route.arg || "/");
  let entries = $state<Entry[]>([]);
  let err = $state("");
  async function load() {
    try {
      entries = (await get<{ entries: Entry[] }>("/v1/ls?path=" + encodeURIComponent(path))).entries;
      err = "";
    } catch (e) {
      err = (e as Error).message;
    }
  }
  $effect(() => {
    void path;
    void app.tick;
    load();
  });
  const full = (n: string) => (path === "/" ? "" : path) + "/" + n;
  const parts = $derived(path.split("/").filter(Boolean));
  const isStore = (h: string) => app.stores.some((s) => s.name === h);
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
</script>

<div class="stack">
  <div class="panel pad crumbs row">
    <button class="crumb" onclick={() => go("files", "/")}>/</button>
    {#each parts as p, i}<span class="faint">/</span><button class="crumb" onclick={() => go("files", "/" + parts.slice(0, i + 1).join("/"))}>{p}</button>{/each}
  </div>
  {#if err}<div class="panel pad" style="color:var(--bad)">{err}</div>{/if}
  <div class="panel scroll-x">
    <table class="t">
      <thead><tr><th>Name</th><th class="num">Size</th><th>Copies</th><th></th></tr></thead>
      <tbody>
        {#each entries as e (e.name)}
          <tr oncontextmenu={(ev) => menu(ev, e)}>
            <td class="name">
              {#if e.kind === "directory"}<button class="lnk" onclick={() => go("files", full(e.name))}><Icon name="files" size={15} /> {e.name}/</button>
              {:else if e.kind === "symlink"}<span class="muted">{e.name} → {e.target}</span>
              {:else}{e.name} {#if e.sealed}<span class="badge">sealed</span>{/if}{/if}
            </td>
            <td class="num mono">{e.kind === "regular" ? human(e.size) : ""}</td>
            <td>{#if e.writing_on}<span class="badge warn">writing on {e.writing_on}</span>
              {:else}{#each e.hosts as h}<span class="badge" class:arch={isStore(h)}>{h}</span> {/each}{/if}</td>
            <td class="acts">
              {#if e.kind !== "symlink"}<button class="btn sm ghost" onclick={(ev) => menu(ev, e)}><Icon name="more" size={15} /></button>{/if}
              {#if e.kind === "regular"}<button class="btn sm ghost" onclick={() => seal(e)}>{e.sealed ? "Unseal" : "Seal"}</button>{/if}
            </td>
          </tr>
        {:else}<tr><td colspan="4" class="empty">Empty</td></tr>{/each}
      </tbody>
    </table>
  </div>
</div>

<style>
  .crumbs { gap: 4px; }
  .crumb { background: none; border: 0; color: var(--spark); cursor: pointer; padding: 2px 4px; }
  .lnk { background: none; border: 0; color: var(--text); cursor: pointer; display: inline-flex; gap: 6px; align-items: center; padding: 0; }
  .lnk:hover { color: var(--spark); }
  .name { word-break: break-all; }
  .acts { white-space: nowrap; text-align: right; }
  .arch { color: var(--violet); border-color: rgba(155, 123, 255, 0.4); }
</style>
