<script lang="ts">
  import { post, type Plan } from "../lib/api";
  import { app, route } from "../lib/state.svelte";
  import { human, pct } from "../lib/format";
  import PlanPreview from "../components/PlanPreview.svelte";
  import Icon from "../components/Icon.svelte";

  type Goal = "free" | "tidy" | "speedup";
  let goal = $state<Goal>(route.params.get("host") ? "free" : "free");
  let archives = $state<string[]>([]);
  let plan = $state<Plan | null>(null);
  let busy = $state(false);
  let err = $state("");

  // ---- make room: desired free bytes per host, set by dragging.
  let want = $state<Record<string, number>>({});
  const hosts = $derived(app.status?.nodes.filter((n) => n.info) ?? []);
  $effect(() => {
    // Arriving from "plan to make room here": start that host 20% freer.
    const h = route.params.get("host");
    const n = hosts.find((x) => x.name === h);
    if (n?.info && want[n.name] === undefined) want[n.name] = Math.min(n.info.total_bytes, n.info.free_bytes + n.info.total_bytes * 0.2);
  });

  // ---- tidy / speed up
  let days = $state(7);
  let scope = $state<string[]>([]);
  let minNet = $state(1 << 30);

  let timer: ReturnType<typeof setTimeout> | undefined;
  function schedule() {
    clearTimeout(timer);
    timer = setTimeout(make, 350);
  }
  async function make() {
    let body: any;
    if (goal === "free") {
      const free = Object.entries(want).map(([h, b]) => [h, Math.round(b)]);
      if (!free.length) {
        plan = null;
        return;
      }
      body = { goal: "free", free, archives };
    } else if (goal === "tidy") body = { goal: "tidy", days, hosts: scope, archives };
    else body = { goal: "speedup", days, hosts: scope, min_remote_bytes: minNet };
    busy = true;
    try {
      plan = await post<Plan>("/v1/plans", body);
      err = "";
    } catch (e) {
      err = (e as Error).message;
    }
    busy = false;
  }
  $effect(() => {
    void goal;
    void days;
    void minNet;
    void scope.length;
    void archives.length;
    schedule();
  });

  // Dragging a host's handle: position = where used space would end.
  let drag: { host: string; el: HTMLElement } | null = null;
  function down(e: PointerEvent, host: string) {
    const el = (e.currentTarget as HTMLElement).closest(".track") as HTMLElement;
    drag = { host, el };
    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    move(e);
  }
  function move(e: PointerEvent) {
    if (!drag) return;
    const n = hosts.find((x) => x.name === drag!.host)?.info;
    if (!n) return;
    const r = drag.el.getBoundingClientRect();
    const f = Math.max(0, Math.min(1, (e.clientX - r.left) / r.width));
    // Never ask for less free space than there is now.
    want[drag.host] = Math.max(n.free_bytes, n.total_bytes * (1 - f));
    schedule();
  }
  function up() {
    drag = null;
  }
  function reset(h: string) {
    delete want[h];
    schedule();
  }
  const toggle = (list: string[], v: string) => (list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);
  const projected = (h: string) => plan?.hosts.find((x) => x.host === h);
</script>

<svelte:window onpointermove={move} onpointerup={up} />

<div class="stack">
  <div class="goals">
    <button class="goal panel" class:on={goal === "free"} onclick={() => (goal = "free")}>
      <Icon name="space" size={22} /><div><b>Make room</b><div class="small muted">Drag a host's handle to the space you want free</div></div>
    </button>
    <button class="goal panel" class:on={goal === "tidy"} onclick={() => (goal = "tidy")}>
      <Icon name="broom" size={22} /><div><b>Tidy up</b><div class="small muted">Remove copies nobody opened lately</div></div>
    </button>
    <button class="goal panel" class:on={goal === "speedup"} onclick={() => (goal = "speedup")}>
      <Icon name="rocket" size={22} /><div><b>Speed up</b><div class="small muted">Copy what hosts keep pulling over the network</div></div>
    </button>
  </div>

  <div class="panel pad stack">
    {#if goal === "free"}
      <div class="row"><h3>Desired free space</h3><span class="small muted">Removes redundant copies least recently used first; sole copies go to the archives you pick.</span></div>
      <div class="bars">
        {#each hosts as h (h.name)}
          {@const i = h.info!}
          {@const other = i.total_bytes - i.free_bytes - i.object_bytes}
          {@const w = want[h.name]}
          {@const p = projected(h.name)}
          {@const target = w ?? i.free_bytes}
          <div class="hrow">
            <div class="hn"><b>{h.name}</b><div class="tiny muted mono">{human(i.free_bytes)} free</div></div>
            <div class="track" class:set={w !== undefined}>
              <i class="other" style="width:{pct(other, i.total_bytes)}%"></i>
              <i class="ours" style="left:{pct(other, i.total_bytes)}%;width:{pct(i.object_bytes, i.total_bytes)}%"></i>
              {#if p && w !== undefined}
                <i class="gain" style="left:{pct(i.total_bytes - p.projected_free, i.total_bytes)}%;width:{pct(p.projected_free - i.free_bytes, i.total_bytes)}%"></i>
                {#if p.projected_free < p.target}<i class="short" style="left:{pct(i.total_bytes - p.target, i.total_bytes)}%;width:{pct(p.target - p.projected_free, i.total_bytes)}%"></i>{/if}
              {/if}
              <button class="handle" style="left:{pct(i.total_bytes - target, i.total_bytes)}%" aria-label="free space target for {h.name}"
                onpointerdown={(e) => down(e, h.name)}
                onkeydown={(e) => { if (e.key === "ArrowLeft" || e.key === "ArrowRight") { want[h.name] = Math.max(i.free_bytes, Math.min(i.total_bytes, target + (e.key === "ArrowLeft" ? 1 : -1) * i.total_bytes * 0.01)); schedule(); } }}></button>
            </div>
            <div class="hv mono small">
              {#if w !== undefined}
                want <b>{human(w)}</b>
                {#if p}<div class="tiny" class:ok={p.projected_free >= p.target} class:bad={p.projected_free < p.target}>{p.projected_free >= p.target ? "✓ reachable" : `short ${human(p.target - p.projected_free)}`}</div>{/if}
                <button class="btn sm ghost" onclick={() => reset(h.name)}>reset</button>
              {:else}<span class="faint">drag ◂</span>{/if}
            </div>
          </div>
        {/each}
      </div>
    {:else}
      <div class="row">
        <span class="caps">{goal === "tidy" ? "Not opened within" : "Read over the network within"}</span>
        <div class="chips">
          {#each [[1, "a day"], [7, "a week"], [30, "a month"]] as [d, l]}
            <button class="chip" class:on={days === d} onclick={() => (days = d as number)}>{l}</button>
          {/each}
        </div>
        {#if goal === "speedup"}
          <span class="caps">at least</span>
          <div class="chips">
            {#each [[256 << 20, "256 MiB"], [1 << 30, "1 GiB"], [4 << 30, "4 GiB"]] as [b, l]}
              <button class="chip" class:on={minNet === b} onclick={() => (minNet = b as number)}>{l}</button>
            {/each}
          </div>
        {/if}
      </div>
      <div class="row">
        <span class="caps">Hosts</span>
        <div class="chips">
          <button class="chip" class:on={!scope.length} onclick={() => (scope = [])}>all</button>
          {#each hosts as h}<button class="chip" class:on={scope.includes(h.name)} onclick={() => (scope = toggle(scope, h.name))}>{h.name}</button>{/each}
        </div>
      </div>
    {/if}
    {#if goal !== "speedup" && app.stores.length}
      <div class="row">
        <span class="caps">Sole copies may go to</span>
        <div class="chips">
          {#each app.stores as s}
            {@const g = s.gateways.find(([, x]) => x.healthy)?.[1]}
            <button class="chip" class:on={archives.includes(s.name)} onclick={() => (archives = toggle(archives, s.name))}>⧉ {s.name}{g ? ` · ${human(g.free_bytes)} free` : " · unreachable"}</button>
          {/each}
        </div>
        {#if !archives.length}<span class="tiny muted">none: only redundant copies are removed</span>{/if}
      </div>
    {/if}
  </div>

  {#if err}<div class="panel pad" style="color:var(--bad)">{err}</div>{/if}
  {#if plan}
    <div class="panel pad" class:dim={busy}>
      <div class="row" style="margin-bottom:10px"><h3>Preview</h3><span class="small muted">nothing moves until you apply</span></div>
      <PlanPreview {plan} onapplied={() => { want = {}; plan = null; }} />
    </div>
  {:else if goal === "free"}
    <div class="panel empty">Drag a handle to the left to ask for more free space on that host.</div>
  {/if}
</div>

<style>
  .goals { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
  .goal { display: flex; gap: 12px; align-items: center; text-align: left; padding: 14px 16px; cursor: pointer; color: var(--muted); transition: all 0.15s; }
  .goal b { color: var(--text); }
  .goal:hover { border-color: var(--line-hi); }
  .goal.on { color: var(--spark); border-color: rgba(56, 232, 255, 0.55); box-shadow: 0 0 30px rgba(56, 232, 255, 0.18); }
  .bars { display: flex; flex-direction: column; gap: 12px; }
  .hrow { display: grid; grid-template-columns: 110px 1fr 150px; gap: 14px; align-items: center; }
  .hn b { display: block; overflow: hidden; text-overflow: ellipsis; }
  .track { position: relative; height: 22px; border-radius: 8px; background: rgba(90, 140, 220, 0.1); border: 1px solid var(--line); touch-action: none; }
  .track i { position: absolute; top: 0; bottom: 0; }
  .track .other { left: 0; background: #2c3a52; border-radius: 7px 0 0 7px; }
  .track .ours { background: linear-gradient(90deg, var(--spark-2), var(--spark)); box-shadow: 0 0 12px rgba(56, 232, 255, 0.5); }
  .track .gain { background: repeating-linear-gradient(45deg, rgba(61, 255, 197, 0.55) 0 6px, rgba(61, 255, 197, 0.25) 6px 12px); box-shadow: 0 0 14px rgba(61, 255, 197, 0.5); }
  .track .short { background: repeating-linear-gradient(45deg, rgba(255, 79, 123, 0.5) 0 6px, transparent 6px 12px); }
  .handle { position: absolute; top: -6px; bottom: -6px; width: 14px; margin-left: -7px; border-radius: 6px; border: 1px solid var(--spark); background: #031018; cursor: ew-resize; box-shadow: 0 0 14px var(--spark); padding: 0; }
  .handle::after { content: ""; position: absolute; left: 5px; right: 5px; top: 6px; bottom: 6px; border-left: 1px solid var(--spark); border-right: 1px solid var(--spark); }
  .track.set .handle { background: var(--spark); }
  .hv { display: flex; flex-direction: column; gap: 2px; align-items: flex-start; }
  .ok { color: var(--ok); }
  .bad { color: var(--bad); }
  .dim { opacity: 0.6; transition: opacity 0.2s; }
  @media (max-width: 899px) {
    .goals { grid-template-columns: 1fr; }
    .hrow { grid-template-columns: 1fr; gap: 6px; }
    .hv { flex-direction: row; gap: 10px; align-items: center; }
  }
</style>
