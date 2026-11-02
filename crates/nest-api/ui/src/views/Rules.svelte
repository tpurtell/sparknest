<script lang="ts">
  import { get, api, post, type Rule } from "../lib/api";
  import { app, toast, targets } from "../lib/state.svelte";
  import { confirm, showPaths } from "../lib/ui.svelte";
  import { selSummary } from "../lib/format";

  function details(r: Rule) {
    const s = r.summary;
    const head = `${r.selector}\n→ ${r.hosts.join(", ")}${s?.models?.length ? `\nmodels: ${s.models.join(", ")}` : ""}${s?.error ? `\n${s.error}` : ""}`;
    showPaths(r.name, head, s?.paths ?? []);
  }

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
      {@const sum = selSummary(r.selector, r.summary)}
      <div class="rule panel">
        <div class="row"><b class="title" title={r.selector}>{sum.title}</b>{#if r.auto}<span class="badge spark">automatic</span>{/if}</div>
        <div class="small muted">{r.name}{sum.detail ? ` · ${sum.detail}` : ""}</div>
        <div class="row">
          <div class="chips">{#each r.hosts as h}<span class="badge">{h}</span>{/each}</div>
          <span class="spacer"></span>
          <button class="btn sm ghost" onclick={() => details(r)}>Details</button>
          <button class="btn sm" onclick={() => apply(r)}>Apply now</button>
          <button class="btn sm danger" onclick={() => remove(r)}>Delete</button>
        </div>
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
  .rule { padding: 12px 14px; display: flex; flex-direction: column; gap: 8px; min-width: 0; }
  .title { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
</style>
