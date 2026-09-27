<script lang="ts">
  import { app } from "../lib/state.svelte";
  import { human, rate } from "../lib/format";
  import type { SourceReport } from "../lib/api";

  const nodes = $derived(app.status?.nodes ?? []);
  const nameOf = (id: number) => nodes.find((n) => n.node === id)?.name ?? `node${id}`;
  const srcName = (s: SourceReport["source"], self: string) => (s === "Local" ? self : nameOf(s.Peer));
  // Reader × source matrix of current rates.
  const matrix = $derived.by(() => {
    const m = new Map<string, SourceReport>();
    for (const n of nodes) for (const s of n.info?.io ?? []) m.set(n.name + "→" + srcName(s.source, n.name), s);
    return m;
  });
  const peak = $derived(Math.max(1, ...[...matrix.values()].map((s) => s.bytes_per_s)));
  const fromDisk = $derived([...nodes].reduce((a, n) => a + (n.info?.io ?? []).filter((s) => s.source === "Local").reduce((b, s) => b + s.bytes_per_s, 0), 0));
  const fromNet = $derived([...nodes].reduce((a, n) => a + (n.info?.io ?? []).filter((s) => s.source !== "Local").reduce((b, s) => b + s.bytes_per_s, 0), 0));
  const ms = (us: number) => (us / 1000).toFixed(us < 10_000 ? 2 : 1) + " ms";
</script>

<div class="stack">
  <div class="panel pad">
    <div class="row"><h2>I/O</h2><span class="small muted">how each host reads files that have several copies: its own disk first while it is idle, then whichever copy answers fastest for what is in flight (ADR-030). Numbers are each host's own measurements over the last seconds.</span></div>
  </div>
  <div class="tiles">
    <div class="tile panel"><div class="caps">From local disks</div><div class="big">{rate(fromDisk)}</div><div class="small muted">spread reads, last 10 s</div></div>
    <div class="tile panel" class:hot={fromNet > 1e8}><div class="caps">From other hosts</div><div class="big">{rate(fromNet)}</div><div class="small muted">over the fabric</div></div>
  </div>

  <div class="panel pad">
    <h3 class="sec">Who reads from whom</h3>
    <div class="scroll-x">
      <table class="mx">
        <thead><tr><th class="corner"><span>reader ↓ · source →</span></th>{#each nodes as s}<th class="col"><span>{s.name}</span></th>{/each}</tr></thead>
        <tbody>
          {#each nodes as r}
            <tr>
              <th class="rowh">{r.name}</th>
              {#each nodes as s}
                {@const x = matrix.get(r.name + "→" + s.name)}
                <td>
                  {#if x}
                    <div class="cellx" class:self={r.name === s.name} style="--k:{Math.min(1, x.bytes_per_s / peak)}"
                      title="{r.name} reads {r.name === s.name ? 'its disk' : 'from ' + s.name}: {rate(x.bytes_per_s)} · {ms(x.latency_us)}/chunk{x.measured ? '' : ' (not measured yet)'} · {x.in_flight} in flight · {human(x.bytes_total)} total{x.errors ? ` · ${x.errors} errors` : ''}">
                      {#if x.bytes_per_s > 0}<span class="mono">{human(x.bytes_per_s, 0)}</span>{:else}<span class="faint">·</span>{/if}
                    </div>
                  {:else}<div class="cellx none"></div>{/if}
                </td>
              {/each}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </div>

  <div class="grid hosts">
    {#each nodes as n (n.name)}
      {@const io = n.info?.io ?? []}
      <div class="host panel">
        <div class="row"><b>{n.name}</b><span class="spacer"></span><span class="tiny muted">{io.length ? `${io.length} source${io.length > 1 ? "s" : ""}` : "no spread reads yet"}</span></div>
        {#each io as s}
          <div class="src">
            <span class="sn">{s.source === "Local" ? "disk" : srcName(s.source, n.name)}</span>
            <span class="lat mono" class:guess={!s.measured} title="recent latency per chunk">{ms(s.latency_us)}</span>
            <span class="fl">{#each Array(Math.min(s.in_flight, 12)) as _}<i></i>{/each}{#if s.in_flight > 12}<b class="tiny">+{s.in_flight - 12}</b>{/if}</span>
            <span class="rt mono" class:live={s.bytes_per_s > 0}>{rate(s.bytes_per_s)}</span>
          </div>
        {/each}
      </div>
    {/each}
  </div>
</div>

<style>
  .tiles { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  .tile { padding: 14px 16px; }
  .tile.hot { border-color: rgba(56, 232, 255, 0.5); box-shadow: 0 0 30px rgba(56, 232, 255, 0.2); }
  .big { font-size: clamp(20px, 2.6vw, 30px); font-weight: 650; font-family: var(--mono); background: linear-gradient(90deg, #f1fbff, #8fe9ff); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .sec { margin-bottom: 10px; color: var(--muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.12em; font-size: 11px; }
  .mx { border-collapse: separate; border-spacing: 4px; }
  .mx th { font-weight: 500; font-size: 11px; color: var(--muted); }
  .col { height: 60px; position: relative; min-width: 56px; }
  .col span { position: absolute; left: 50%; bottom: 4px; transform-origin: left bottom; transform: rotate(-45deg); white-space: nowrap; }
  .corner span { font-size: 10px; color: var(--faint); }
  .rowh { text-align: right; padding-right: 6px; }
  .cellx { width: 64px; height: 34px; border-radius: 8px; display: grid; place-items: center; font-size: 11px; border: 1px solid rgba(90, 170, 255, 0.15);
    background: rgba(56, 232, 255, calc(0.06 + 0.6 * var(--k))); box-shadow: 0 0 calc(18px * var(--k)) rgba(56, 232, 255, calc(0.6 * var(--k))); }
  .cellx.self { border-color: rgba(155, 123, 255, 0.5); background: rgba(155, 123, 255, calc(0.08 + 0.6 * var(--k))); }
  .cellx.none { background: none; border-style: dashed; opacity: 0.25; }
  .hosts { grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); }
  .host { padding: 12px 14px; display: flex; flex-direction: column; gap: 6px; }
  .src { display: grid; grid-template-columns: 70px 76px 1fr 90px; gap: 8px; align-items: center; font-size: 12px; }
  .sn { overflow: hidden; text-overflow: ellipsis; }
  .lat.guess { color: var(--faint); }
  .fl { display: flex; gap: 2px; align-items: center; }
  .fl i { width: 5px; height: 12px; border-radius: 2px; background: var(--spark); box-shadow: 0 0 6px var(--spark); }
  .rt { text-align: right; color: var(--faint); }
  .rt.live { color: var(--spark); }
  @media (max-width: 899px) { .tiles { grid-template-columns: 1fr 1fr; } }
</style>
