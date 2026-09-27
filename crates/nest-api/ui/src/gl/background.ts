// The page's backdrop: a slow blue plasma with faint electric filaments and
// drifting sparks. Rendered at half resolution, 30 fps, paused when hidden;
// one still frame when the user prefers reduced motion.

import { compile, fit, loop } from "./util";

const VS = `#version 300 es
in vec2 p; out vec2 uv;
void main() { uv = p * 0.5 + 0.5; gl_Position = vec4(p, 0.0, 1.0); }`;

const FS = `#version 300 es
precision highp float;
in vec2 uv; out vec4 o;
uniform float t; uniform vec2 res; uniform float energy;

float hash(vec2 p) { return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453); }
float noise(vec2 p) {
  vec2 i = floor(p), f = fract(p);
  vec2 u = f * f * (3.0 - 2.0 * f);
  return mix(mix(hash(i), hash(i + vec2(1, 0)), u.x), mix(hash(i + vec2(0, 1)), hash(i + vec2(1, 1)), u.x), u.y);
}
float fbm(vec2 p) {
  float v = 0.0, a = 0.5;
  for (int i = 0; i < 5; i++) { v += a * noise(p); p = p * 2.03 + vec2(1.7, 9.2); a *= 0.5; }
  return v;
}

void main() {
  vec2 q = (uv - 0.5) * vec2(res.x / res.y, 1.0);
  float tt = t * 0.035;
  // Nebula: domain-warped fbm in deep blues.
  vec2 w = vec2(fbm(q * 1.6 + tt), fbm(q * 1.6 - tt + 4.0));
  float n = fbm(q * 2.2 + w * 1.4 + vec2(tt * 0.7, -tt * 0.4));
  vec3 col = mix(vec3(0.006, 0.016, 0.045), vec3(0.03, 0.12, 0.28), smoothstep(0.2, 0.9, n));
  col += vec3(0.09, 0.03, 0.22) * smoothstep(0.5, 0.95, fbm(q * 1.1 - w + tt * 0.5));
  // A slow aurora band sweeping across.
  float band = exp(-pow((q.y + 0.35 * sin(q.x * 1.3 + t * 0.05) - 0.1) * 3.2, 2.0));
  col += vec3(0.02, 0.18, 0.3) * band * (0.35 + 0.65 * fbm(q * 4.0 + vec2(t * 0.04, 0.0)));
  // Filaments: thin ridges of a second warped field.
  float r = fbm(q * 3.0 + w * 2.0 - vec2(0.0, t * 0.02));
  float ridge = 1.0 - abs(r * 2.0 - 1.0);
  float fil = pow(ridge, 22.0) * (0.5 + 0.9 * energy);
  col += vec3(0.25, 0.8, 1.0) * fil * 0.9;
  // Sparks: sparse cells with a drifting bright point.
  vec2 g = q * 22.0 + vec2(0.0, t * 0.25);
  vec2 id = floor(g); vec2 f = fract(g) - 0.5;
  float h = hash(id);
  if (h > 0.965) {
    vec2 c = vec2(sin(t * 0.7 + h * 40.0), cos(t * 0.5 + h * 17.0)) * 0.3;
    float d = length(f - c);
    float tw = 0.5 + 0.5 * sin(t * (2.0 + h * 5.0) + h * 30.0);
    col += vec3(0.5, 0.9, 1.0) * smoothstep(0.08, 0.0, d) * tw * 0.8;
  }
  // Vignette.
  col *= 1.0 - 0.45 * dot(q * 0.9, q * 0.9);
  o = vec4(col, 1.0);
}`;

export function startBackground(canvas: HTMLCanvasElement, energy: () => number, still: boolean) {
  const gl = canvas.getContext("webgl2", { antialias: false, alpha: false });
  if (!gl) {
    canvas.style.background = "radial-gradient(ellipse at 50% 30%, #0b1e3f, #020617 70%)";
    return () => {};
  }
  const prog = compile(gl, VS, FS);
  const buf = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, buf);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
  const loc = gl.getAttribLocation(prog, "p");
  const ut = gl.getUniformLocation(prog, "t");
  const ures = gl.getUniformLocation(prog, "res");
  const uen = gl.getUniformLocation(prog, "energy");
  let e = 0;
  const draw = (t: number) => {
    // A soft backdrop: about a third of a megapixel is enough at any size.
    const css = Math.max(1, canvas.clientWidth * canvas.clientHeight);
    fit(canvas, Math.min(0.5, Math.sqrt(350_000 / css) / Math.min(window.devicePixelRatio || 1, 2)));
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.useProgram(prog);
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.enableVertexAttribArray(loc);
    gl.vertexAttribPointer(loc, 2, gl.FLOAT, false, 0, 0);
    e += (energy() - e) * 0.05;
    gl.uniform1f(ut, t);
    gl.uniform2f(ures, canvas.width, canvas.height);
    gl.uniform1f(uen, e);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
  };
  if (still) {
    draw(12.0);
    const onResize = () => draw(12.0);
    addEventListener("resize", onResize);
    return () => removeEventListener("resize", onResize);
  }
  return loop(30, draw);
}
