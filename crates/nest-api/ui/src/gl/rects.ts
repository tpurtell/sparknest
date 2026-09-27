// Instanced glowing rectangles for the treemap. Each instance: rect (CSS
// px), color, level, flags. Flags: 1 hover, 2 free space, 4 "more", 8 busy
// (a copy in flight), 16 dimmed.

import { compile, fit } from "./util";

export interface GlRect {
  x: number;
  y: number;
  w: number;
  h: number;
  color: [number, number, number];
  level: number;
  flags: number;
}

const VS = `#version 300 es
in vec2 corner;
in vec4 rect; in vec3 color; in vec2 extra;
uniform vec2 res; uniform float dpr;
out vec2 vPx; out vec2 vSize; out vec3 vColor; out vec2 vExtra; out vec2 vUv;
void main() {
  vec2 p = rect.xy + corner * rect.zw;
  vPx = corner * rect.zw * dpr; vSize = rect.zw * dpr; vUv = corner;
  vColor = color; vExtra = extra;
  vec2 c = p * dpr / res * 2.0 - 1.0;
  gl_Position = vec4(c.x, -c.y, 0.0, 1.0);
}`;

const FS = `#version 300 es
precision highp float;
in vec2 vPx; in vec2 vSize; in vec3 vColor; in vec2 vExtra; in vec2 vUv;
uniform float t; uniform float dpr;
out vec4 o;
bool has(float f, float bit) { return mod(floor(f / bit), 2.0) > 0.5; }
void main() {
  float flags = vExtra.y;
  vec2 d = min(vPx, vSize - vPx);
  float e = min(d.x, d.y) / dpr;
  vec3 c = vColor;
  float lvl = vExtra.x;
  // Body: darker toward the bottom, deeper levels a touch brighter.
  vec3 body = c * (0.22 + 0.18 * (1.0 - vUv.y) + 0.06 * lvl);
  // Edge glow.
  float edge = exp(-e / 1.1);
  vec3 col = body + c * edge * 0.95 + vec3(0.6, 0.9, 1.0) * exp(-e / 0.5) * 0.25;
  if (has(flags, 2.0)) {
    // Free space: dark with faint diagonal hatching.
    float s = mod(gl_FragCoord.x + gl_FragCoord.y, 14.0 * dpr);
    col = vec3(0.02, 0.05, 0.09) + vec3(0.1, 0.3, 0.45) * step(s, 1.2 * dpr) * 0.5 + c * edge * 0.5;
  }
  if (has(flags, 4.0)) col = mix(col, vec3(0.12, 0.16, 0.24), 0.6);
  if (has(flags, 8.0)) {
    // Copy in flight: a band sweeping across.
    float band = fract(vUv.x * 0.6 - t * 0.6 + vUv.y * 0.2);
    col += c * smoothstep(0.9, 1.0, band) * 0.6;
  }
  if (has(flags, 1.0)) {
    float sweep = fract(t * 0.5 + (vUv.x + vUv.y) * 0.25);
    col = col * 1.55 + vec3(0.4, 0.8, 1.0) * smoothstep(0.96, 1.0, sweep) * 0.35;
  }
  if (has(flags, 16.0)) col *= 0.35;
  o = vec4(col, 1.0);
}`;

export class RectRenderer {
  gl: WebGL2RenderingContext;
  prog: WebGLProgram;
  vao: WebGLVertexArrayObject;
  inst: WebGLBuffer;
  n = 0;
  constructor(public canvas: HTMLCanvasElement) {
    const gl = canvas.getContext("webgl2", { antialias: false, alpha: true, premultipliedAlpha: true });
    if (!gl) throw new Error("WebGL2 is not available");
    this.gl = gl;
    this.prog = compile(gl, VS, FS);
    this.vao = gl.createVertexArray()!;
    gl.bindVertexArray(this.vao);
    const quad = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, quad);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([0, 0, 1, 0, 0, 1, 1, 1]), gl.STATIC_DRAW);
    const lc = gl.getAttribLocation(this.prog, "corner");
    gl.enableVertexAttribArray(lc);
    gl.vertexAttribPointer(lc, 2, gl.FLOAT, false, 0, 0);
    this.inst = gl.createBuffer()!;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.inst);
    const stride = 9 * 4;
    const a = (name: string, size: number, off: number) => {
      const l = gl.getAttribLocation(this.prog, name);
      gl.enableVertexAttribArray(l);
      gl.vertexAttribPointer(l, size, gl.FLOAT, false, stride, off * 4);
      gl.vertexAttribDivisor(l, 1);
    };
    a("rect", 4, 0);
    a("color", 3, 4);
    a("extra", 2, 7);
  }

  draw(rects: GlRect[], t: number) {
    const { gl } = this;
    fit(this.canvas);
    const dpr = this.canvas.width / Math.max(1, this.canvas.clientWidth);
    gl.viewport(0, 0, this.canvas.width, this.canvas.height);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    const data = new Float32Array(rects.length * 9);
    rects.forEach((r, i) => {
      data.set([r.x, r.y, r.w, r.h, r.color[0], r.color[1], r.color[2], r.level, r.flags], i * 9);
    });
    gl.useProgram(this.prog);
    gl.uniform2f(gl.getUniformLocation(this.prog, "res"), this.canvas.width, this.canvas.height);
    gl.uniform1f(gl.getUniformLocation(this.prog, "dpr"), dpr);
    gl.uniform1f(gl.getUniformLocation(this.prog, "t"), t);
    gl.bindVertexArray(this.vao);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.inst);
    gl.bufferData(gl.ARRAY_BUFFER, data, gl.DYNAMIC_DRAW);
    gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, rects.length);
  }
}
