<script lang="ts">
  import { get, api, post, type Rule } from "../lib/api";
  import { app, toast, targets } from "../lib/state.svelte";
  import { confirm } from "../lib/ui.svelte";
  import { selLabel } from "../lib/format";

  let rules = $state<Rule[]>([]);
  const load = async () => (rules = (await get<{ rules: Rule[] }>("/v1/rules")).rules);
  $effect(() => {
    load();
  });
  let name = $state("");
  let sel = $state("");
  let auto = $state(true);
  let hosts = $state<string[]>([]);
  const toggle = (h: string) => (hosts = hosts.includes(h) ? hosts.filter((x) => x !== h) : [...hosts, h]);
  async function save() {
    try {
      await api("PUT", "/v1/rules/" + encodeURIComponent(name), { selector: sel, hosts, auto });
      toast("Rule saved");
      name = sel = "";
      hosts = [];
      load();
    } catch (e) {
      toast((e as Error).message, true);
    }
  }
  async function remove(r: Rule) {
    if (!(await confirm(`Delete rule ${r.name}?`, "Existing copies stay where they are.", "Delete", true))) return;
    await api("DELETE", "/v1/rules/" + encodeURIComponent(r.name));
    load();
  }
  async function apply(r: Rule) {
    try {
      await post("/v1/reconcile", { name: r.name });
      toast("Applying " + r.name);
    } catch (e) {
      toast((e as Error).message, true);
    }
  }
</script>

<div class="stack">
  <div class="row"><h2>Rules</h2><span class="small muted">where things must be; plans never remove what a rule requires</span></div>
  <div class="grid list">
    {#each rules as r (r.name)}
      <div class="rule panel">
        <div class="row"><b>{r.name}</b>{#if r.auto}<span class="badge spark">automatic</span>{/if}<span class="spacer"></span>
          <button class="btn sm" onclick={() => apply(r)}>Apply now</button>
          <button class="btn sm danger" onclick={() => remove(r)}>Delete</button></div>
        <div class="mono small">{selLabel(r.selector)}</div>
        <div class="chips">{#each r.hosts as h}<span class="badge">{h}</span>{/each}</div>
      </div>
    {:else}<div class="panel empty">No rules yet.</div>{/each}
  </div>
  <div class="panel pad stack">
    <h3>New rule</h3>
    <div class="row">
      <input placeholder="name" bind:value={name} style="width:160px" />
      <input placeholder="hf:org/name, hf-dataset:org/name or /path" bind:value={sel} style="flex:1;min-width:220px" />
      <label class="check small"><input type="checkbox" bind:checked={auto} /> automatic (follows new files)</label>
    </div>
    <div class="chips">
      <button class="chip" class:on={hosts.includes("@all")} onclick={() => toggle("@all")}>@all</button>
      {#each app.groups as g}<button class="chip" class:on={hosts.includes("@" + g.name)} onclick={() => toggle("@" + g.name)}>@{g.name}</button>{/each}
      {#each targets() as t}<button class="chip" class:on={hosts.includes(t.name)} onclick={() => toggle(t.name)}>{t.kind === "store" ? "⧉ " : ""}{t.name}</button>{/each}
    </div>
    <div><button class="btn primary" disabled={!name || !sel || !hosts.length} onclick={save}>Save rule</button></div>
  </div>
</div>

<style>
  .list { grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
  .rule { padding: 12px 14px; display: flex; flex-direction: column; gap: 8px; }
</style>
