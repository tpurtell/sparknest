// Small WebGL2 helpers.

export function compile(gl: WebGL2RenderingContext, vs: string, fs: string): WebGLProgram {
  const sh = (type: number, src: string) => {
    const s = gl.createShader(type)!;
    gl.shaderSource(s, src);
    gl.compileShader(s);
    if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
      throw new Error("shader: " + gl.getShaderInfoLog(s));
    }
    return s;
  };
  const p = gl.createProgram()!;
  gl.attachShader(p, sh(gl.VERTEX_SHADER, vs));
  gl.attachShader(p, sh(gl.FRAGMENT_SHADER, fs));
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) throw new Error("link: " + gl.getProgramInfoLog(p));
  return p;
}

/** Keep a canvas's backing store at its CSS size times `scale` (≤ DPR). */
export function fit(canvas: HTMLCanvasElement, scale = 1): boolean {
  const dpr = Math.min(window.devicePixelRatio || 1, 2) * scale;
  const w = Math.max(1, Math.round(canvas.clientWidth * dpr));
  const h = Math.max(1, Math.round(canvas.clientHeight * dpr));
  if (canvas.width !== w || canvas.height !== h) {
    canvas.width = w;
    canvas.height = h;
    return true;
  }
  return false;
}

/** Hex "#38e8ff" → [r, g, b] in 0..1. */
export function rgb(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16);
  return [((n >> 16) & 255) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255];
}

/** A frame loop that pauses while hidden and caps the frame rate. */
export function loop(fps: number, frame: (t: number) => void): () => void {
  let raf = 0;
  let last = 0;
  const step = (t: number) => {
    raf = requestAnimationFrame(step);
    if (document.visibilityState !== "visible") return;
    if (t - last < 1000 / fps - 2) return;
    last = t;
    frame(t / 1000);
  };
  raf = requestAnimationFrame(step);
  return () => cancelAnimationFrame(raf);
}
