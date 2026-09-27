// Electric arcs between points (hosts on the overview), drawn additively:
// a soft glow along a gentle curve, a jagged bolt that re-forms ~12 times a
// second, and sparks travelling from source to destination. Intensity 0..1
// scales everything; idle links still crackle faintly now and then.

import { compile, fit, loop } from "./util";

export interface ArcPoint {
  x: number; // CSS pixels within the canvas
  y: number;
}
export interface Arc {
  a: number;
  b: number;
  /** 0..1 */
  intensity: number;
  color: [number, number, number];
}
export interface ArcScene {
  points: ArcPoint[];
  arcs: Arc[];
}

const VS = `#version 300 es
in vec2 pos; in float side; in float alpha; in vec3 color;
uniform vec2 res;
out float vSide; out float vAlpha; out vec3 vColor;
void main() {
  vSide = side; vAlpha = alpha; vColor = color;
  vec2 c = pos / res * 2.0 - 1.0;
  gl_Position = vec4(c.x, -c.y, 0.0, 1.0);
}`;

const FS = `#version 300 es
precision mediump float;
in float vSide; in float vAlpha; in vec3 vColor;
out vec4 o;
void main() {
  float a = vAlpha * exp(-vSide * vSide * 3.5);
  o = vec4(vColor * a, a);
}`;

// Deterministic per (arc, time bucket) jitter.
function rand(seed: number) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return ((s >>> 0) % 10000) / 10000;
  };
}

type V = number[];

function strip(out: V, pts: [number, number][], width: number, alpha: number, c: [number, number, number]) {
  // Triangles for a polyline of `width` px, side coordinate -1..1.
  for (let i = 0; i < pts.length - 1; i++) {
    const [x0, y0] = pts[i];
    const [x1, y1] = pts[i + 1];
    const dx = x1 - x0;
    const dy = y1 - y0;
    const l = Math.hypot(dx, dy) || 1;
    const nx = (-dy / l) * width;
    const ny = (dx / l) * width;
    const q = [
      [x0 + nx, y0 + ny, 1],
      [x0 - nx, y0 - ny, -1],
      [x1 + nx, y1 + ny, 1],
      [x1 - nx, y1 - ny, -1],
    ];
    for (const k of [0, 1, 2, 2, 1, 3]) out.push(q[k][0], q[k][1], q[k][2], alpha, c[0], c[1], c[2]);
  }
}

function curve(ax: number, ay: number, bx: number, by: number, bend: number, n: number): [number, number][] {
  const mx = (ax + bx) / 2;
  const my = (ay + by) / 2;
  const dx = bx - ax;
  const dy = by - ay;
  const cx = mx - dy * bend;
  const cy = my + dx * bend;
  const pts: [number, number][] = [];
  for (let i = 0; i <= n; i++) {
    const t = i / n;
    const u = 1 - t;
    pts.push([u * u * ax + 2 * u * t * cx + t * t * bx, u * u * ay + 2 * u * t * cy + t * t * by]);
  }
  return pts;
}

function bolt(ax: number, ay: number, bx: number, by: number, r: () => number, rough: number): [number, number][] {
  let pts: [number, number][] = [
    [ax, ay],
    [bx, by],
  ];
  let amp = Math.hypot(bx - ax, by - ay) * rough;
  for (let level = 0; level < 5; level++) {
    const next: [number, number][] = [pts[0]];
    for (let i = 0; i < pts.length - 1; i++) {
      const [x0, y0] = pts[i];
      const [x1, y1] = pts[i + 1];
      const dx = x1 - x0;
      const dy = y1 - y0;
      const l = Math.hypot(dx, dy) || 1;
      const off = (r() - 0.5) * amp;
      next.push([(x0 + x1) / 2 + (-dy / l) * off, (y0 + y1) / 2 + (dx / l) * off], pts[i + 1]);
    }
    pts = next;
    amp *= 0.55;
  }
  return pts;
}

export function startArcs(canvas: HTMLCanvasElement, scene: () => ArcScene, still: boolean) {
  const gl = canvas.getContext("webgl2", { premultipliedAlpha: true, antialias: true });
  if (!gl) return () => {};
  const prog = compile(gl, VS, FS);
  const buf = gl.createBuffer();
  const vao = gl.createVertexArray();
  gl.bindVertexArray(vao);
  gl.bindBuffer(gl.ARRAY_BUFFER, buf);
  const stride = 7 * 4;
  const attr = (name: string, size: number, off: number) => {
    const l = gl.getAttribLocation(prog, name);
    gl.enableVertexAttribArray(l);
    gl.vertexAttribPointer(l, size, gl.FLOAT, false, stride, off * 4);
  };
  attr("pos", 2, 0);
  attr("side", 1, 2);
  attr("alpha", 1, 3);
  attr("color", 3, 4);
  const ures = gl.getUniformLocation(prog, "res");
  // Occasional ambient crackle on idle links.
  const flare = new Map<string, number>();

  const draw = (t: number) => {
    fit(canvas);
    const dpr = canvas.width / Math.max(1, canvas.clientWidth);
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    const s = scene();
    const v: V = [];
    const bucket = Math.floor(t * 12);
    s.arcs.forEach((arc, i) => {
      const A = s.points[arc.a];
      const B = s.points[arc.b];
      if (!A || !B) return;
      const ax = A.x * dpr, ay = A.y * dpr, bx = B.x * dpr, by = B.y * dpr;
      const key = `${arc.a}-${arc.b}`;
      let k = arc.intensity;
      if (k < 0.05 && !still) {
        // Idle: a brief flare every so often.
        const until = flare.get(key) ?? 0;
        if (t < until) k = 0.25 * ((until - t) / 0.35);
        else if (Math.random() < 0.0025) flare.set(key, t + 0.35);
      }
      const c = arc.color;
      const bend = 0.12 * (i % 2 ? 1 : -1);
      // Glow.
      strip(v, curve(ax, ay, bx, by, bend, 24), (6 + 10 * k) * dpr, 0.05 + 0.25 * k, c);
      if (k > 0.02) {
        const r = rand(bucket * 7919 + i * 104729 + 1);
        const pts = bolt(ax, ay, bx, by, r, 0.12);
        strip(v, pts, (1.2 + 2.2 * k) * dpr, Math.min(1, 0.35 + 0.9 * k), [
          Math.min(1, c[0] + 0.4),
          Math.min(1, c[1] + 0.3),
          1,
        ]);
        // Sparks travelling a → b.
        const n = Math.round(2 + 10 * k);
        const path = curve(ax, ay, bx, by, bend, 48);
        for (let j = 0; j < n; j++) {
          const f = (t * (0.25 + 0.6 * k) + j / n) % 1;
          const idx = Math.min(path.length - 2, Math.floor(f * (path.length - 1)));
          const [x0, y0] = path[idx];
          const [x1, y1] = path[idx + 1];
          strip(
            v,
            [
              [x0, y0],
              [x0 + (x1 - x0) * 2.5, y0 + (y1 - y0) * 2.5],
            ],
            3.2 * dpr,
            0.9,
            [0.8, 0.97, 1],
          );
        }
      }
    });
    if (!v.length) return;
    gl.useProgram(prog);
    gl.uniform2f(ures, canvas.width, canvas.height);
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.ONE, gl.ONE);
    gl.bindVertexArray(vao);
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(v), gl.DYNAMIC_DRAW);
    gl.drawArrays(gl.TRIANGLES, 0, v.length / 7);
  };
  if (still) {
    const id = setInterval(() => draw(5), 1000);
    draw(5);
    return () => clearInterval(id);
  }
  return loop(40, draw);
}
