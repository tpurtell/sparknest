<script lang="ts">
  import { post, type Plan } from "../lib/api";
  import { human, selLabel } from "../lib/format";
  import { poll, toast } from "../lib/state.svelte";
  import { confirm } from "../lib/ui.svelte";
  import Icon from "./Icon.svelte";

  let { plan, onapplied }: { plan: Plan; onapplied?: () => void } = $props();
  const moved = $derived(plan.steps.reduce((a, s) => a + s.bytes, 0));
  const label = (s: Plan["steps"][number]) =>
    s.kind === "evict" ? (s.requires !== undefined ? "Remove moved copies" : "Remove redundant copies") : s.kind === "offload" ? "Offload sole copies" : "Copy";

  async function apply() {
    const n = plan.steps.reduce((a, s) => a + s.copies.length, 0);
    if (!(await confirm(`Apply this plan?`, `${n} file moves, ${human(moved)}. Each step is rechecked against current rules first; the last copy of a file is never removed.`, "Apply")))
      return;
    try {
      await post(`/v1/plans/${plan.id}/apply`);
      toast("Plan started");
      poll();
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
    <div class="steps">
      {#each plan.steps as s, i}
        <details class="step panel" open={i === 0 && plan.steps.length < 4}>
          <summary>
            <span class="k {s.kind}">{s.kind === "replicate" ? "+" : s.kind === "offload" ? "⧉" : "−"}</span>
            <span>{label(s)} {s.kind === "replicate" ? "to" : "from"} <b>{s.host}</b>{#if s.kind === "offload"} into <b>{s.store}</b>{/if}</span>
            <span class="spacer"></span>
            <span class="mono small">{s.copies.length} files · {human(s.bytes)}</span>
          </summary>
          <div class="copies">
            {#each s.copies.slice(0, 200) as c}
              <div class="c"><span class="p">{selLabel(c.path)}</span><span class="mono tiny">{human(c.size)}</span><span class="why tiny muted">{c.why}</span></div>
            {/each}
            {#if s.copies.length > 200}<div class="tiny muted">… and {s.copies.length - 200} more</div>{/if}
          </div>
        </details>
      {/each}
    </div>
    <div class="row">
      <button class="btn primary" onclick={apply}><Icon name="play" size={15} /> Apply plan</button>
      <span class="small muted">{human(moved)} across {plan.steps.length} step{plan.steps.length > 1 ? "s" : ""}</span>
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
  .steps { display: flex; flex-direction: column; gap: 6px; }
  .step summary { list-style: none; display: flex; gap: 10px; align-items: center; padding: 10px 12px; cursor: pointer; }
  .step summary::-webkit-details-marker { display: none; }
  .k { width: 22px; height: 22px; border-radius: 7px; display: grid; place-items: center; font-weight: 700; flex: none; }
  .k.evict { color: var(--warn); background: rgba(255, 193, 94, 0.12); }
  .k.offload { color: var(--violet); background: rgba(155, 123, 255, 0.15); }
  .k.replicate { color: var(--ok); background: rgba(61, 255, 197, 0.12); }
  .copies { padding: 0 12px 10px; display: flex; flex-direction: column; gap: 4px; max-height: 340px; overflow: auto; }
  .c { display: grid; grid-template-columns: minmax(0, 1fr) 80px; gap: 0 10px; padding: 4px 0; border-top: 1px solid rgba(90, 170, 255, 0.07); }
  .p { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 13px; }
  .why { grid-column: 1 / -1; }
</style>
