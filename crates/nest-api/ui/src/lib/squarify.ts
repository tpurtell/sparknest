// Squarified treemap layout (Bruls, Huizing, van Wijk): rows along the
// shorter side, each row kept while it improves the worst aspect ratio.

export interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

export function squarify<T>(items: { value: number; item: T }[], r: Rect): { item: T; rect: Rect }[] {
  const out: { item: T; rect: Rect }[] = [];
  const list = items.filter((i) => i.value > 0).sort((a, b) => b.value - a.value);
  const total = list.reduce((a, i) => a + i.value, 0);
  if (!total || r.w <= 0 || r.h <= 0) return out;
  const scale = (r.w * r.h) / total;
  let rect = { ...r };
  let i = 0;
  while (i < list.length) {
    const side = Math.min(rect.w, rect.h);
    const row: typeof list = [];
    let sum = 0;
    let worst = Infinity;
    while (i < list.length) {
      const v = list[i].value * scale;
      const s2 = sum + v;
      const rowMax = Math.max(v, row.length ? row[0].value * scale : v);
      const rowMin = v;
      const w = Math.max((side * side * rowMax) / (s2 * s2), (s2 * s2) / (side * side * rowMin));
      if (row.length && w > worst) break;
      row.push(list[i]);
      sum = s2;
      worst = w;
      i++;
    }
    // Lay the row along the shorter side.
    const thick = sum / side;
    let off = 0;
    for (const it of row) {
      const len = (it.value * scale) / thick;
      out.push({
        item: it.item,
        rect:
          rect.w >= rect.h
            ? { x: rect.x, y: rect.y + off, w: thick, h: len }
            : { x: rect.x + off, y: rect.y, w: len, h: thick },
      });
      off += len;
    }
    rect =
      rect.w >= rect.h
        ? { x: rect.x + thick, y: rect.y, w: rect.w - thick, h: rect.h }
        : { x: rect.x, y: rect.y + thick, w: rect.w, h: rect.h - thick };
  }
  return out;
}
