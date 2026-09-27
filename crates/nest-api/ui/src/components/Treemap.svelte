<script lang="ts">
  import { onMount, untrack } from "svelte";
  import type { TreeNode } from "../lib/api";
  import { squarify, type Rect } from "../lib/squarify";
  import { RectRenderer, type GlRect } from "../gl/rects";
  import { rgb } from "../gl/util";
  import { human } from "../lib/format";
  import { reducedMotion } from "../lib/state.svelte";
  import Icon from "./Icon.svelte";

  let {
    root,
    height = "min(70vh, 720px)",
    busy = () => false,
    oncontext,
    onload,
    crumbs = true,
  }: {
    root: TreeNode;
    height?: string;
    /** Nodes with a copy in flight shimmer. */
    busy?: (n: TreeNode) => boolean;
    /** Right-click / long-press / click on a leaf. */
    oncontext?: (n: TreeNode, x: number, y: number, trail: TreeNode[]) => void;
    /** Load children below the depth fetched so far. */
    onload?: (n: TreeNode) => Promise<TreeNode | null>;
    crumbs?: boolean;
  } = $props();

  // Electric palette for top-level blocks; children vary in lightness.
  const PALETTE = ["#38e8ff", "#4f8dff", "#9b7bff", "#2ff5c8", "#5ab8ff", "#c07bff", "#3dd6ff", "#7aa2ff", "#56f0e0", "#b69cff"].map(rgb);

  let box = $state<HTMLDivElement>();
  let canvas: HTMLCanvasElement;
  let labels: HTMLCanvasElement;
  let gl: RectRenderer | null = null;
  let failed = $state("");
  let trail = $state<TreeNode[]>([]);
  let hover = $state<{ n: TreeNode; x: number; y: number; path: string[] } | null>(null);
  let W = $state(0);
  let H = $state(0);

  $effect(() => {
    // New data: keep the zoom if the same path still exists.
    const names = untrack(() => trail.slice(1).map((n) => n.name));
    const t: TreeNode[] = [root];
    for (const nm of names) {
      const c = t[t.length - 1].children?.find((x) => x.name === nm);
      if (!c) break;
      t.push(c);
    }
    trail = t;
  });
  const current = $derived(trail[trail.length - 1] ?? root);

  interface Item {
    key: string;
    n: TreeNode;
    r: Rect;
    level: number;
    color: [number, number, number];
    header: boolean;
    parentKey: string;
  }
  let items: Item[] = [];
  let prev = new Map<string, Rect>();
  let anim = { start: 0, from: new Map<string, Rect>() };

  function shade(c: [number, number, number], i: number, n: number): [number, number, number] {
    const f = 0.78 + 0.35 * (n > 1 ? i / (n - 1) : 0.5);
    return [Math.min(1, c[0] * f), Math.min(1, c[1] * f), Math.min(1, c[2] * f)];
  }

  function layout() {
    const out: Item[] = [];
    const place = (n: TreeNode, r: Rect, level: number, color: [number, number, number], key: string) => {
      const kids = (n.children ?? []).filter((c) => c.bytes > 0);
      if (!kids.length) return;
      const laid = squarify(
        kids.map((c) => ({ value: c.bytes, item: c })),
        r,
      );
      laid.forEach(({ item: c, rect }, i) => {
        const col = level === 0 ? (c.kind === "free" ? rgb("#1d4a6b") : PALETTE[i % PALETTE.length]) : shade(color, i, laid.length);
        const k = key + "/" + c.name;
        const big = rect.w > 70 && rect.h > 44;
        const hasKids = (c.children?.length ?? 0) > 0;
        const header = level < 2 && big && hasKids;
        out.push({ key: k, n: c, r: rect, level, color: col, header, parentKey: key });
        if (level < 2 && hasKids && rect.w > 36 && rect.h > 30) {
          const pad = 2;
          const top = header ? 18 : pad;
          place(c, { x: rect.x + pad, y: rect.y + top, w: rect.w - 2 * pad, h: rect.h - top - pad }, level + 1, col, k);
        }
      });
    };
    place(current, { x: 0, y: 0, w: W, h: H }, 0, PALETTE[0], "");
    // Animate from where each block was (or from its parent).
    const from = new Map<string, Rect>();
    for (const it of out) {
      const p = prev.get(it.key) ?? prev.get(it.parentKey);
      if (p) from.set(it.key, p);
    }
    anim = { start: performance.now(), from };
    items = out;
    prev = new Map(out.map((it) => [it.key, it.r]));
    kick();
  }

  let raf = 0;
  let looping = false;
  function kick() {
    if (!looping) {
      looping = true;
      raf = requestAnimationFrame(frame);
    }
  }

  const ease = (t: number) => 1 - Math.pow(1 - t, 3);
  function current_rects(now: number): { it: Item; r: Rect }[] {
    const t = reducedMotion() ? 1 : Math.min(1, (now - anim.start) / 480);
    const e = ease(t);
    return items.map((it) => {
      const f = anim.from.get(it.key);
      if (!f || t >= 1) return { it, r: it.r };
      return {
        it,
        r: { x: f.x + (it.r.x - f.x) * e, y: f.y + (it.r.y - f.y) * e, w: f.w + (it.r.w - f.w) * e, h: f.h + (it.r.h - f.h) * e },
      };
    });
  }

  function frame(now: number) {
    looping = false;
    if (!gl) return;
    const rs = current_rects(now);
    const glr: GlRect[] = rs.map(({ it, r }) => ({
      ...r,
      color: it.color,
      level: it.level,
      flags:
        (hover && hover.n === it.n ? 1 : 0) |
        (it.n.kind === "free" ? 2 : 0) |
        (it.n.kind === "more" ? 4 : 0) |
        (busy(it.n) ? 8 : 0),
    }));
    gl.draw(glr, now / 1000);
    drawLabels(rs);
    const animating = now - anim.start < 500;
    const live = !reducedMotion() && (hover || items.some((it) => busy(it.n)));
    if (animating || live) kick();
  }

  function drawLabels(rs: { it: Item; r: Rect }[]) {
    const ctx = labels.getContext("2d");
    if (!ctx) return;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    if (labels.width !== Math.round(W * dpr) || labels.height !== Math.round(H * dpr)) {
      labels.width = Math.round(W * dpr);
      labels.height = Math.round(H * dpr);
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    ctx.textBaseline = "top";
    const fit = (s: string, w: number) => {
      if (ctx.measureText(s).width <= w) return s;
      let lo = 0;
      let hi = s.length;
      while (lo < hi) {
        const m = (lo + hi + 1) >> 1;
        if (ctx.measureText(s.slice(0, m) + "…").width <= w) lo = m;
        else hi = m - 1;
      }
      return lo > 1 ? s.slice(0, lo) + "…" : "";
    };
    for (const { it, r } of rs) {
      const leaf = !it.header && !(it.level < 2 && (it.n.children?.length ?? 0) > 0 && r.w > 36 && r.h > 30);
      if (it.header) {
        ctx.font = "600 12px Inter, system-ui, sans-serif";
        ctx.fillStyle = "rgba(235,248,255,0.95)";
        const size = human(it.n.bytes);
        ctx.font = "11px ui-monospace, monospace";
        const sw = ctx.measureText(size).width;
        ctx.fillStyle = "rgba(150,230,255,0.85)";
        if (r.w > sw + 60) ctx.fillText(size, r.x + r.w - sw - 6, r.y + 4);
        ctx.font = "600 12px Inter, system-ui, sans-serif";
        ctx.fillStyle = "rgba(235,248,255,0.95)";
        ctx.fillText(fit(it.n.name, r.w - (r.w > sw + 60 ? sw + 16 : 10)), r.x + 6, r.y + 3);
      } else if (leaf && r.w > 44 && r.h > 22) {
        const fs = Math.max(10, Math.min(15, Math.sqrt(r.w * r.h) / 9));
        ctx.font = `600 ${fs}px Inter, system-ui, sans-serif`;
        ctx.fillStyle = "rgba(240,250,255,0.95)";
        ctx.shadowColor = "rgba(0,0,0,0.7)";
        ctx.shadowBlur = 4;
        ctx.fillText(fit(it.n.name, r.w - 10), r.x + 6, r.y + 5);
        if (r.h > fs + 22) {
          ctx.font = `${Math.max(10, fs - 2)}px ui-monospace, monospace`;
          ctx.fillStyle = "rgba(150,230,255,0.9)";
          ctx.fillText(fit(human(it.n.bytes), r.w - 10), r.x + 6, r.y + fs + 9);
        }
        ctx.shadowBlur = 0;
      }
    }
  }

  function hit(x: number, y: number): Item | null {
    for (let i = items.length - 1; i >= 0; i--) {
      const r = items[i].r;
      if (x >= r.x && x < r.x + r.w && y >= r.y && y < r.y + r.h) return items[i];
    }
    return null;
  }

  function trailTo(it: Item): TreeNode[] {
    // Nodes from the current zoom down to `it`.
    const names = it.key.split("/").slice(1);
    const out: TreeNode[] = [];
    let n: TreeNode = current;
    for (const nm of names) {
      const c = n.children?.find((x) => x.name === nm);
      if (!c) break;
      out.push(c);
      n = c;
    }
    return out;
  }

  const pos = (e: MouseEvent) => {
    const b = canvas.getBoundingClientRect();
    return { x: e.clientX - b.left, y: e.clientY - b.top };
  };

  function onmove(e: PointerEvent) {
    if (e.pointerType === "touch") return;
    const { x, y } = pos(e);
    const it = hit(x, y);
    const n = it?.n ?? null;
    if (!n) {
      hover = null;
    } else {
      hover = { n, x, y, path: [...trail.slice(1), ...trailTo(it!)].map((t) => t.name) };
    }
    kick();
  }

  async function zoomInto(path: TreeNode[]) {
    let target = path[path.length - 1];
    if (!target) return;
    if (target.truncated && !(target.children?.length) && onload) {
      const loaded = await onload(target);
      if (loaded?.children) target.children = loaded.children;
    }
    if (!(target.children?.length)) return;
    trail = [...trail, ...path];
  }

  function onclick(e: MouseEvent) {
    const { x, y } = pos(e);
    const it = hit(x, y);
    if (!it || it.n.kind === "more" || it.n.kind === "free") return;
    const path = trailTo(it);
    // Zoom into the outermost block with children under the pointer.
    const firstWithKids = path.findIndex((n) => (n.children?.length ?? 0) > 0 || n.truncated);
    if (firstWithKids >= 0) zoomInto(path.slice(0, firstWithKids + 1));
    else oncontext?.(it.n, e.clientX, e.clientY, [...trail, ...path]);
  }

  function oncontextmenu(e: MouseEvent) {
    e.preventDefault();
    const { x, y } = pos(e);
    const it = hit(x, y);
    if (!it) return;
    oncontext?.(it.n, e.clientX, e.clientY, [...trail, ...trailTo(it)]);
  }

  let press: ReturnType<typeof setTimeout> | undefined;
  function ondown(e: PointerEvent) {
    if (e.pointerType !== "touch") return;
    clearTimeout(press);
    const ev = { clientX: e.clientX, clientY: e.clientY };
    press = setTimeout(() => {
      press = undefined;
      const { x, y } = pos(ev as MouseEvent);
      const it = hit(x, y);
      if (it) oncontext?.(it.n, ev.clientX, ev.clientY, [...trail, ...trailTo(it)]);
    }, 520);
  }
  const cancelPress = () => clearTimeout(press);

  onMount(() => {
    try {
      gl = new RectRenderer(canvas);
    } catch (e) {
      failed = (e as Error).message;
    }
    const ro = new ResizeObserver(() => {
      if (!box) return;
      W = box.clientWidth;
      H = box.clientHeight;
    });
    ro.observe(box!);
    return () => {
      ro.disconnect();
      cancelAnimationFrame(raf);
    };
  });

  $effect(() => {
    void current;
    void root;
    void H;
    if (W) layout();
  });
</script>

<div class="tm">
  {#if crumbs}
    <div class="crumbs row">
      {#if trail.length > 1}
        <button class="btn sm ghost" onclick={() => (trail = trail.slice(0, -1))} aria-label="Zoom out"><Icon name="up" size={14} /></button>
      {/if}
      {#each trail as t, i}
        {#if i}<span class="faint">›</span>{/if}
        <button class="crumb" class:on={i === trail.length - 1} onclick={() => (trail = trail.slice(0, i + 1))}>{t.name}</button>
      {/each}
      <span class="spacer"></span>
      <span class="mono small muted">{human(current.bytes)} · {current.files.toLocaleString()} files</span>
    </div>
  {/if}
  <div class="box" bind:this={box} style="height:{height}">
    <canvas class="gl" bind:this={canvas}
      onpointermove={onmove} onpointerleave={() => { hover = null; kick(); }}
      onpointerdown={ondown} onpointerup={cancelPress} onpointercancel={cancelPress}
      {onclick} {oncontextmenu}></canvas>
    <canvas class="lbl" bind:this={labels} aria-hidden="true"></canvas>
    {#if failed}<div class="empty">Treemap needs WebGL2: {failed}</div>{/if}
    {#if hover}
      <div class="tip panel" style="left:{Math.min(W - 280, hover.x + 14)}px;top:{Math.min(H - 110, hover.y + 14)}px">
        <div class="tiny faint path">{hover.path.slice(0, -1).join(" › ")}</div>
        <b>{hover.n.name}</b>
        <div class="mono small"><span class="spark">{human(hover.n.bytes)}</span>{#if hover.n.files > 1} · {hover.n.files.toLocaleString()} files{/if}</div>
        {#if hover.n.hosts?.length}<div class="tiny muted">on {hover.n.hosts.join(", ")}</div>{/if}
        {#if hover.n.children?.length || hover.n.truncated}<div class="tiny faint">click to zoom · right-click for actions</div>
        {:else if hover.n.kind !== "free" && hover.n.kind !== "more"}<div class="tiny faint">click for actions</div>{/if}
      </div>
    {/if}
  </div>
</div>

<style>
  .tm { display: flex; flex-direction: column; gap: 8px; }
  .crumbs { gap: 6px; min-height: 30px; }
  .crumb { background: none; border: 0; color: var(--muted); cursor: pointer; padding: 3px 4px; border-radius: 6px; max-width: 240px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .crumb:hover { color: var(--text); }
  .crumb.on { color: var(--spark); font-weight: 600; }
  .box { position: relative; width: 100%; border-radius: 12px; overflow: hidden; background: rgba(2, 8, 20, 0.6); border: 1px solid var(--line); touch-action: manipulation; }
  canvas { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }
  .gl { cursor: pointer; }
  .lbl { pointer-events: none; }
  .tip { position: absolute; z-index: 3; padding: 9px 12px; max-width: 270px; pointer-events: none; background: rgba(5, 12, 28, 0.93); }
  .path { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .spark { color: var(--spark); }
  .empty { position: absolute; inset: 0; display: grid; place-items: center; }
</style>
