const UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

export function human(b: number | undefined | null, digits = 1): string {
  if (b == null || !isFinite(b)) return "";
  let v = Math.abs(b);
  let i = 0;
  while (v >= 1024 && i < UNITS.length - 1) {
    v /= 1024;
    i++;
  }
  const s = i === 0 ? String(Math.round(v)) : v.toFixed(v >= 100 ? 0 : digits);
  return (b < 0 ? "-" : "") + s + " " + UNITS[i];
}

export const rate = (bps: number) => (bps < 1 ? "idle" : human(bps) + "/s");

export function pct(part: number, whole: number): number {
  return whole > 0 ? (100 * part) / whole : 0;
}

/** "3 min ago", "2 days ago"; "never" for 0. */
export function ago(ms: number, now = Date.now()): string {
  if (!ms) return "never";
  const s = Math.max(0, (now - ms) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400 * 2) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} days ago`;
}

export function duration(ms: number): string {
  const s = ms / 1000;
  if (s < 90) return s.toFixed(1) + " s";
  if (s < 5400) return (s / 60).toFixed(1) + " min";
  return (s / 3600).toFixed(1) + " h";
}

/** "600", "1.5T", "800G" → bytes; a bare number is GiB. */
export function parseSize(s: string): number {
  const m = /^\s*([\d.]+)\s*([kmgt]?)(i?b)?\s*$/i.exec(s);
  if (!m) throw new Error(`bad size "${s}" (e.g. 600, 600G, 1.5T)`);
  const unit = (m[2] || "g").toLowerCase();
  return Math.floor(parseFloat(m[1]) * 1024 ** " kmgt".indexOf(unit));
}

/** Short label for a selector: "hf:org/name" → "org/name". */
export const selLabel = (s: string) => s.replace(/^hf(-dataset)?:/, "");

/** "org/name" → ["org/", "name"]; legacy repos have no org. */
export function splitRepo(repo: string): [string, string] {
  const i = repo.indexOf("/");
  return i < 0 ? ["", repo] : [repo.slice(0, i + 1), repo.slice(i + 1)];
}
