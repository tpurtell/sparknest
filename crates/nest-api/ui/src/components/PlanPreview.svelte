<script lang="ts">
  // A plan as a tree to edit before applying: host → model (or directory)
  // → files, each with a checkbox. Unchecking skips those copies (the plan
  // is edited on the server, so `nest plan show` sees the same); a removal
  // whose keeper copy is skipped simply does not happen.
  import { get, post, type Plan, type Copy, type Step } from "../lib/api";
  import { human } from "../lib/format";
  import { toast } from "../lib/state.svelte";
  import { confirm } from "../lib/ui.svelte";
  import Icon from "./Icon.svelte";

  let { plan = $bindable(), onapplied }: { plan: Plan; onapplied?: () => void } = $props();

  type Kind = Step["kind"];
  interface Row {
    c: Copy;
    kind: Kind;
    store?: string;
  }
  interface Model {
    name: string;
    rows: Row[];
    bytes: number;
  }
  interface Host {
    host: string;
    models: Model[];
    rows: Row[];
  }

  const tree = $derived.by(() => {
    const hosts: Host[] = [];
    for (const s of plan.steps) {
      let h = hosts.find((x) => x.host === s.host);
      if (!h) hosts.push((h = { host: s.host, models: [], rows: [] }));
      for (const c of s.copies) {
        const g = c.group || "(other)";
        let m = h.models.find((x) => x.name === g);
        if (!m) h.models.push((m = { name: g, rows: [], bytes: 0 }));
        const r = { c, kind: s.kind, store: s.kind === "offload" ? s.store : undefined };
        m.rows.push(r);
        m.bytes += c.size;
        h.rows.push(r);
      }
    }
    for (const h of hosts) {
      h.models.sort((a, b) => b.bytes - a.bytes);
      for (const m of h.models) m.rows.sort((a, b) => b.c.size - a.c.size);
    }
    return hosts;
  });

  const picked = (rows: Row[]) => rows.filter((r) => !r.c.skip);
  const bytes = (rows: Row[], k?: Kind) => picked(rows).filter((r) => !k || r.kind === k).reduce((a, r) => a + r.c.size, 0);
  const mark = (rows: Row[]) => {
    const n = picked(rows).length;
    return n === 0 ? "none" : n === rows.length ? "all" : "some";
  };
  const all = $derived(plan.steps.flatMap((s) => s.copies.map((c) => ({ c, kind: s.kind }) as Row)));
  const chosen = $derived(picked(all));
  const skipped = $derived(all.length - chosen.length);

  // Checkbox with a third, "some" state.
  function tri(node: HTMLInputElement, st: string) {
    const set = (s: string) => {
      node.checked = s === "all";
      node.indeterminate = s === "some";
    };
    set(st);
    return { update: set };
  }

  const label = (r: Row) => r.c.name || r.c.path;
  const sign = (k: Kind) => (k === "replicate" ? "+" : k === "offload" ? "⧉" : "−");

  // Edits land locally at once and on the server behind them; the server's
  // plan replaces ours when it answers.
  let pending = 0;
  async function select(host: string, matches: string[], on: boolean) {
    const match = (c: Copy) =>
      !matches.length || matches.some((m) => (m.startsWith("#") ? String(c.file) === m.slice(1) : (c.group || "(other)") === m));
    for (const s of plan.steps) if (s.host === host) for (const c of s.copies) if (match(c)) c.skip = !on;
    pending++;
    try {
      const r = await post<{ plan: Plan }>(`/v1/plans/${plan.id}/select`, { host, matches, on });
      if (--pending === 0) plan = r.plan;
    } catch (e) {
      pending--;
      toast((e as Error).message, true);
      plan = await get<Plan>(`/v1/plans/${plan.id}`).catch(() => plan);
    }
  }

  async function apply() {
    const b = bytes(all);
    if (!(await confirm(`Apply this plan?`, `${chosen.length} file moves, ${human(b)}${skipped ? ` (${skipped} skipped)` : ""}. Each step is rechecked against current rules first; the last copy of a file is never removed.`, "Apply")))
      return;
    try {
      await post(`/v1/plans/${plan.id}/apply`);
      toast("Plan started");
      onapplied?.();
    } catch (e) {
      toast((e as Error).message, true);
    }
  }
</script>

<div class="pv stack">
  {#if plan.archives.length}
    <div class="arch">
      {#each plan.archives as a}
        <div class="a panel">
          <b>⧉ {a.store}</b>
          {#if a.reachable}
            <span class="mono small">{human(a.free_now)} → <span class:spark={a.adds}>{human(a.projected_free)}</span> free</span>
            <span class="tiny muted">{a.adds ? `stores ${human(a.adds)}` : "takes nothing"} · of {human(a.total)}</span>
          {:else}<span class="small" style="color:var(--bad)">unreachable</span>{/if}
        </div>
      {/each}
    </div>
  {/if}

  {#each plan.blocked as b}<div class="note bad"><Icon name="info" size={15} /> {b}</div>{/each}
  {#each plan.notes as b}<div class="note"><Icon name="info" size={15} /> {b}</div>{/each}

  {#if plan.steps.length}
    <div class="tree">
      {#each tree as h (h.host)}
        <details class="host panel" open={tree.length <= 2}>
          <summary>
            <input type="checkbox" use:tri={mark(h.rows)} onclick={(e) => e.stopPropagation()}
              onchange={(e) => select(h.host, [], e.currentTarget.checked)} aria-label="include {h.host}" />
            <b>{h.host}</b>
            <span class="spacer"></span>
            {#if bytes(h.rows, "replicate")}<span class="amt add mono small">+{human(bytes(h.rows, "replicate"))}</span>{/if}
            {#if bytes(h.rows, "evict")}<span class="amt drop mono small">−{human(bytes(h.rows, "evict"))}</span>{/if}
            {#if bytes(h.rows, "offload")}<span class="amt off mono small">⧉ {human(bytes(h.rows, "offload"))}</span>{/if}
            <span class="tiny muted">{picked(h.rows).length}/{h.rows.length} files</span>
          </summary>
          <div class="models">
            {#each h.models as m (m.name)}
              <details class="model">
                <summary>
                  <input type="checkbox" use:tri={mark(m.rows)} onclick={(e) => e.stopPropagation()}
                    onchange={(e) => select(h.host, m.name === "(other)" ? m.rows.map((r) => "#" + r.c.file) : [m.name], e.currentTarget.checked)} aria-label="include {m.name} on {h.host}" />
                  <span class="mname">{m.name}</span>
                  <span class="spacer"></span>
                  <span class="mono small">{human(bytes(m.rows))}</span>
                  <span class="tiny muted">{picked(m.rows).length}/{m.rows.length}</span>
                </summary>
                <div class="files">
                  {#each m.rows.slice(0, 400) as r (r.kind + r.c.file)}
                    <label class="f" class:off={r.c.skip}>
                      <input type="checkbox" checked={!r.c.skip} onchange={(e) => select(h.host, ["#" + r.c.file], e.currentTarget.checked)} />
                      <span class="k {r.kind}">{sign(r.kind)}</span>
                      <span class="p" title={r.c.path}>{label(r)}</span>
                      <span class="mono tiny">{human(r.c.size)}</span>
                      <span class="why tiny muted">{r.store ? `into ${r.store} · ` : ""}{r.c.why}</span>
                    </label>
                  {/each}
                  {#if m.rows.length > 400}<div class="tiny muted">… and {m.rows.length - 400} more (toggle the model to include or skip them)</div>{/if}
                </div>
              </details>
            {/each}
          </div>
        </details>
      {/each}
    </div>
    <div class="row">
      <button class="btn primary" onclick={apply} disabled={!chosen.length}><Icon name="play" size={15} /> Apply plan</button>
      <span class="small muted">{chosen.length} files · {human(bytes(all))}{skipped ? ` · ${skipped} skipped` : ""}</span>
    </div>
  {:else}
    <div class="muted small">Nothing to do{plan.blocked.length ? " that reaches the goal" : ""}.</div>
  {/if}
</div>

<style>
  .arch { display: flex; gap: 8px; flex-wrap: wrap; }
  .a { padding: 8px 12px; display: flex; flex-direction: column; gap: 2px; }
  .spark { color: var(--spark); }
  .note { display: flex; gap: 8px; align-items: flex-start; font-size: 13px; color: var(--muted); }
  .note.bad { color: #ffb3c5; }
  .tree { display: flex; flex-direction: column; gap: 6px; }
  summary { list-style: none; display: flex; gap: 10px; align-items: center; cursor: pointer; }
  summary::-webkit-details-marker { display: none; }
  summary::before { content: "▸"; color: var(--faint); font-size: 11px; width: 10px; transition: transform 0.15s; }
  details[open] > summary::before { transform: rotate(90deg); }
  .host > summary { padding: 10px 12px; }
  .models { padding: 0 10px 8px 22px; display: flex; flex-direction: column; gap: 2px; }
  .model > summary { padding: 6px 8px; border-radius: 8px; }
  .model > summary:hover { background: rgba(56, 232, 255, 0.05); }
  .mname { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
  .amt { padding: 1px 7px; border-radius: 999px; }
  .amt.add { color: var(--ok); background: rgba(61, 255, 197, 0.1); }
  .amt.drop { color: var(--warn); background: rgba(255, 193, 94, 0.1); }
  .amt.off { color: var(--violet); background: rgba(155, 123, 255, 0.12); }
  .files { padding: 2px 8px 8px 26px; display: flex; flex-direction: column; max-height: 320px; overflow: auto; }
  .f { display: grid; grid-template-columns: auto 18px minmax(0, 1fr) 76px; gap: 0 8px; align-items: center; padding: 3px 0; border-top: 1px solid rgba(90, 170, 255, 0.07); cursor: pointer; }
  .f.off .p, .f.off .k { opacity: 0.4; text-decoration: line-through; }
  .k { font-weight: 700; text-align: center; }
  .k.evict { color: var(--warn); }
  .k.offload { color: var(--violet); }
  .k.replicate { color: var(--ok); }
  .p { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }
  .why { grid-column: 3 / -1; }
</style>
