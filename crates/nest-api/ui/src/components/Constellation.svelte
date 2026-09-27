<script lang="ts">
  import { onMount } from "svelte";
  import { startArcs, type ArcScene } from "../gl/arcs";
  import { app, go, reducedMotion } from "../lib/state.svelte";
  import { human, rate } from "../lib/format";
  import type { NodeStatus } from "../lib/api";

  let wrap = $state<HTMLDivElement>();
  let canvas: HTMLCanvasElement;
  let W = $state(800);
  let H = $state(460);
  let hover = $state<string | null>(null);

  const hosts = $derived<NodeStatus[]>(app.status?.nodes ?? []);

  // Concentric rings: up to 12 on one ring, then 8, 14, 20, ... from the
  // inside out, so 2 hosts and 36 hosts both read well.
  const layout = $derived.by(() => {
    const n = hosts.length;
    const cx = W / 2;
    const cy = H / 2;
    const caps: number[] = [];
    if (n <= 12) caps.push(n);
    else {
      let left = n;
      let cap = 8;
      while (left > 0) {
        caps.push(Math.min(cap, left));
        left -= cap;
        cap += 6;
      }
    }
    const rings = caps.length;
    const out: { x: number; y: number; r: number }[] = [];
    const maxR = Math.min(W * 0.44, H * 0.42);
    let i = 0;
    caps.forEach((c, k) => {
      const f = rings === 1 ? 1 : 0.45 + (0.55 * k) / (rings - 1);
      let rx = n <= 2 ? W * 0.22 : maxR * f * (W > H * 1.3 ? 1.35 : 1);
      let ry = n <= 2 ? 0 : maxR * f;
      const circ = 2 * Math.PI * Math.max(ry, 40);
      const orb = Math.max(15, Math.min(58, circ / c / 2.9, (Math.min(W, H) / 5) * (n <= 4 ? 1 : 0.8)));
      // Keep whole orbs (and their status dot) inside the frame.
      rx = Math.min(rx, W / 2 - orb - 12);
      ry = Math.min(ry, H / 2 - orb - 12);
      for (let j = 0; j < c; j++) {
        const a = -Math.PI / 2 + (2 * Math.PI * j) / c + k * 0.35;
        out.push({ x: cx + (n <= 2 ? (j ? rx : -rx) : rx * Math.cos(a)), y: cy + (n <= 2 ? 0 : ry * Math.sin(a)), r: orb });
        i++;
      }
    });
    return out;
  });

  // Arcs: every host to its ring neighbour (ambient), plus traffic from
  // hosts that serve to hosts that read, split by share of what is served.
  const scene = (): ArcScene => {
    const pts = layout.map((p) => ({ x: p.x, y: p.y }));
    const arcs: ArcScene["arcs"] = [];
    const n = hosts.length;
    const seen = new Set<string>();
    const blue: [number, number, number] = [0.22, 0.75, 1.0];
    const add = (a: number, b: number, k: number, color = blue) => {
      const key = a < b ? `${a}-${b}` : `${b}-${a}`;
      if (seen.has(key) && k === 0) return;
      seen.add(key);
      arcs.push({ a, b, intensity: k, color });
    };
    const rates = hosts.map((h) => app.rates[h.name] ?? { read: 0, served: 0, local: 0, waste: null });
    const served = rates.reduce((a, r) => a + r.served, 0);
    rates.forEach((r, dst) => {
      if (r.read < 1e5 || served < 1) return;
      rates.forEach((s, src) => {
        if (src === dst || s.served < 1e5) return;
        const bps = (r.read * s.served) / served;
        const k = Math.min(1, Math.log10(1 + bps / 1e6) / 3.5);
        if (k > 0.02) add(src, dst, k, [0.35, 0.85, 1.0]);
      });
    });
    if (n > 1)
      for (let i = 0; i < n; i++) {
        const j = (i + 1) % n;
        if (n === 2 && i === 1) break;
        add(i, j, 0);
      }
    return { points: pts, arcs };
  };

  // A host reading its own disk crackles around the edge of its bubble,
  // harder the faster it reads (1 MB/s faint, ~10 GB/s full).
  const still = reducedMotion();
  let zap = $state(0);
  const spark = (bps: number) => Math.min(1, Math.log10(1 + bps / 1e6) / 4);
  const rnd = (a: number, b: number, c: number) => {
    const x = Math.sin(a * 127.1 + b * 311.7 + c * 74.7) * 43758.5453;
    return x - Math.floor(x);
  };
  /** A jittery closed ring just outside radius r. */
  const crackle = (r: number, seed: number, k: number, t: number) => {
    const n = 64;
    let d = "";
    for (let j = 0; j < n; j++) {
      const a = (2 * Math.PI * j) / n;
      const rr = r * (1.03 + (rnd(seed, j, t) - 0.5) * 0.16 * k);
      d += `${j ? "L" : "M"}${(rr * Math.cos(a)).toFixed(1)} ${(rr * Math.sin(a)).toFixed(1)}`;
    }
    return d + "Z";
  };
  /** A few short forks leaping off the edge. */
  const forks = (r: number, seed: number, k: number, t: number) => {
    let d = "";
    const count = 1 + Math.round(3 * k);
    for (let b = 0; b < count; b++) {
      let a = 2 * Math.PI * rnd(seed, b, t + 0.3);
      let rr = r * 1.03;
      d += `M${(rr * Math.cos(a)).toFixed(1)} ${(rr * Math.sin(a)).toFixed(1)}`;
      for (let s = 0; s < 4; s++) {
        rr += r * (0.05 + 0.06 * k) * rnd(seed + s, b, t);
        a += (rnd(seed, b + s, t + 0.7) - 0.5) * 0.35;
        d += `L${(rr * Math.cos(a)).toFixed(1)} ${(rr * Math.sin(a)).toFixed(1)}`;
      }
    }
    return d;
  };

  onMount(() => {
    const flicker = setInterval(() => {
      if (!still && hosts.some((h) => (app.rates[h.name]?.local ?? 0) > 1e6)) zap++;
    }, 90);
    const ro = new ResizeObserver(() => {
      if (!wrap) return;
      W = wrap.clientWidth;
      H = wrap.clientHeight;
    });
    ro.observe(wrap!);
    const stop = startArcs(canvas, scene, reducedMotion());
    return () => {
      clearInterval(flicker);
      ro.disconnect();
      stop();
    };
  });

  const C = (r: number) => 2 * Math.PI * r;
</script>

<div class="wrap" bind:this={wrap}>
  <canvas bind:this={canvas} aria-hidden="true"></canvas>
  <svg width={W} height={H} role="img" aria-label="Hosts">
    <defs>
      <radialGradient id="halo">
        <stop offset="0%" stop-color="#38e8ff" stop-opacity="0.35" />
        <stop offset="60%" stop-color="#2b6cff" stop-opacity="0.08" />
        <stop offset="100%" stop-color="#2b6cff" stop-opacity="0" />
      </radialGradient>
      <radialGradient id="core">
        <stop offset="0%" stop-color="#0d2446" />
        <stop offset="100%" stop-color="#040b1b" />
      </radialGradient>
    </defs>
    {#each hosts as h, i (h.name)}
      {@const p = layout[i]}
      {@const info = h.info}
      {#if p}
        {@const r = p.r}
        {@const ringR = r * 0.86}
        {@const total = info?.total_bytes || 1}
        {@const held = info ? info.object_bytes / total : 0}
        {@const used = info ? (info.total_bytes - info.free_bytes) / total : 0}
        {@const rt = app.rates[h.name]}
        {@const active = rt && rt.read + rt.served + rt.local > 1e5}
        {@const k = spark(rt?.local ?? 0)}
        {@const leader = app.status?.leader === h.node}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <g class="orb" class:down={!info?.serving} transform="translate({p.x},{p.y})" role="button" tabindex="0"
          aria-label="{h.name}: open its space map"
          onclick={() => go("space", "", { scope: h.name })}
          onkeydown={(e) => e.key === "Enter" && go("space", "", { scope: h.name })}
          onmouseenter={() => (hover = h.name)} onmouseleave={() => (hover = null)}>
          <circle r={r * (active ? 1.9 : 1.55)} fill="url(#halo)" class:pulse={active} />
          <circle r={r} fill="url(#core)" stroke="rgba(110,200,255,0.25)" />
          {#if k > 0.02}
            <g class="zap" style="opacity:{0.4 + 0.6 * k}">
              <path d={crackle(r, i + 1, k, zap)} class="zap-glow" stroke-width={2 + 5 * k} />
              <path d={crackle(r, i + 7.5, k, zap)} class="zap-core" />
              <path d={forks(r, i + 3, k, zap)} class="zap-core" />
            </g>
          {/if}
          <g class="reactor" style="animation-duration:{18 + (i % 5) * 4}s">
            <circle r={r * 1.14} fill="none" stroke="rgba(56,232,255,0.35)" stroke-width="1" stroke-dasharray="2 {Math.max(4, r * 0.18)}" />
            <circle r={r * 1.24} fill="none" stroke="rgba(79,141,255,0.18)" stroke-width="1" stroke-dasharray="{r * 0.9} {r * 0.5}" />
          </g>
          <!-- capacity ring: other used (slate), sparknest (spark) -->
          <circle r={ringR} fill="none" stroke="rgba(80,120,180,0.18)" stroke-width={Math.max(3, r * 0.12)} />
          <circle r={ringR} fill="none" stroke="#3b4d6b" stroke-width={Math.max(3, r * 0.12)}
            stroke-dasharray="{C(ringR) * used} {C(ringR)}" transform="rotate(-90)" />
          <circle r={ringR} fill="none" stroke="#38e8ff" stroke-width={Math.max(3, r * 0.12)} class="held"
            stroke-dasharray="{C(ringR) * held} {C(ringR)}" transform="rotate(-90)" />
          {#if leader}
            <g class="orbit"><circle cx={r + 6} cy="0" r="3" fill="#ffd36e" /></g>
          {/if}
          <text y={r > 30 ? -4 : 4} class="name" font-size={Math.max(10, Math.min(15, r * 0.34))}>{h.name}</text>
          {#if r > 30 && info}
            <text y={r * 0.34} class="sub" font-size={Math.max(9, Math.min(12, r * 0.24))}>{human(info.object_bytes)}</text>
          {/if}
          <circle cx={r * 0.72} cy={-r * 0.72} r="4" class="st" class:ok={info?.serving} />
        </g>
      {/if}
    {/each}
  </svg>
  {#if hover}
    {@const h = hosts.find((x) => x.name === hover)}
    {@const i = hosts.findIndex((x) => x.name === hover)}
    {#if h && layout[i]}
      {@const p = layout[i]}
      <div class="tip panel" style="left:{Math.min(W - 250, Math.max(8, p.x + p.r + 10))}px;top:{Math.max(8, p.y - 60)}px">
        <b>{h.name}</b> {#if app.status?.leader === h.node}<span class="badge warn">leader</span>{/if}
        {#if h.info}
          <div class="small">sparknest holds <b class="spark">{human(h.info.object_bytes)}</b> · {h.info.objects} files</div>
          <div class="small muted">{human(h.info.free_bytes)} free of {human(h.info.total_bytes)}</div>
          {#if app.rates[h.name]}
            <div class="small muted">↓ {rate(app.rates[h.name].read)} · ↑ {rate(app.rates[h.name].served)} · disk {rate(app.rates[h.name].local)}</div>
          {/if}
          <div class="tiny faint">{h.info.rails.length} rails · click for its space map</div>
        {:else}
          <div class="small" style="color:var(--bad)">{h.error ?? "unreachable"}</div>
        {/if}
      </div>
    {/if}
  {/if}
</div>

<style>
  .wrap {
    position: relative; width: 100%; height: clamp(320px, 58vh, 640px);
    /* holographic floor: faint dots, brighter toward the middle */
    background:
      radial-gradient(ellipse at 50% 50%, rgba(56, 232, 255, 0.07), transparent 65%),
      radial-gradient(circle, rgba(110, 200, 255, 0.16) 1px, transparent 1.6px) 0 0 / 22px 22px;
    -webkit-mask: radial-gradient(ellipse at 50% 50%, #000 55%, transparent 95%);
    mask: radial-gradient(ellipse at 50% 50%, #000 55%, transparent 95%);
  }
  canvas, svg { position: absolute; inset: 0; width: 100%; height: 100%; }
  svg { overflow: visible; }
  .orb { cursor: pointer; outline: none; transition: transform 0.2s; }
  .orb:hover .held, .orb:focus-visible .held { filter: drop-shadow(0 0 6px #38e8ff); }
  .orb.down { opacity: 0.55; }
  .zap { pointer-events: none; }
  .zap-glow { fill: none; stroke: rgba(56, 232, 255, 0.4); filter: blur(2px); }
  .zap-core { fill: none; stroke: #e4fbff; stroke-width: 1.1; stroke-linejoin: round; filter: drop-shadow(0 0 4px #38e8ff); }
  .held { filter: drop-shadow(0 0 3px rgba(56, 232, 255, 0.8)); transition: stroke-dasharray 0.8s; }
  .name { fill: #eaf6ff; text-anchor: middle; font-weight: 600; pointer-events: none; }
  .sub { fill: #7fe9ff; text-anchor: middle; font-family: var(--mono); pointer-events: none; }
  .st { fill: var(--bad); }
  .st.ok { fill: var(--ok); filter: drop-shadow(0 0 4px var(--ok)); }
  .pulse { animation: pulse 1.6s ease-in-out infinite; }
  .orbit { animation: spin 6s linear infinite; }
  .reactor { animation: spin 20s linear infinite; transform-origin: 0 0; }
  .orb:hover .reactor circle { stroke: rgba(56, 232, 255, 0.7); }
  @keyframes spin { to { transform: rotate(360deg); } }
  .tip { position: absolute; z-index: 5; padding: 10px 12px; width: 240px; pointer-events: none; background: rgba(6, 13, 30, 0.92); }
  .spark { color: var(--spark); }
</style>
