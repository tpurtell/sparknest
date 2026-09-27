// The daemon's HTTP API, with the bearer token from `nest ui`'s link.

let token = "";
try {
  const m = location.hash.match(/token=([0-9a-f]+)/);
  if (m) {
    token = m[1];
    sessionStorage.setItem("nest-token", token);
    history.replaceState(null, "", location.pathname + location.search);
  } else token = sessionStorage.getItem("nest-token") || "";
} catch {
  /* storage blocked: the login form still works for this page view */
}

export const hasToken = () => token !== "";
export function setToken(t: string) {
  token = t.trim();
  try {
    sessionStorage.setItem("nest-token", token);
  } catch {
    /* ignore */
  }
}

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
  }
}

export async function api<T = any>(method: string, path: string, body?: unknown): Promise<T> {
  const r = await fetch(path, {
    method,
    headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const v = await r.json().catch(() => ({ error: r.statusText }));
  if (!r.ok) throw new ApiError(v.error || r.statusText, r.status);
  return v as T;
}

export const get = <T = any>(p: string) => api<T>("GET", p);
export const post = <T = any>(p: string, b: unknown = {}) => api<T>("POST", p, b);
export const del = <T = any>(p: string) => api<T>("DELETE", p);

// ---------------------------------------------------------------- types

export interface NodeInfo {
  node: number;
  name: string;
  mountpoint?: string;
  total_bytes: number;
  free_bytes: number;
  objects: number;
  object_bytes: number;
  rails: string[];
  serving: boolean;
  leader?: number;
  applied: number;
  version: string;
  fabric_read_bytes: number;
  fabric_served_bytes: number;
}
export interface NodeStatus {
  node: number;
  name: string;
  info?: NodeInfo;
  error?: string;
}
export interface Status {
  leader?: number;
  entries: number;
  nodes: NodeStatus[];
}
export interface StoreHealth {
  healthy: boolean;
  total_bytes: number;
  free_bytes: number;
  objects: number;
  object_bytes: number;
  error?: string;
}
export interface Store {
  id: number;
  name: string;
  path: string;
  gateways: [string, StoreHealth][];
}
export interface JobProgress {
  total_files: number;
  done_files: number;
  total_bytes: number;
  done_bytes: number;
  failed: [number, string][];
  finished: boolean;
  cancelled?: boolean;
}
export interface Job {
  id: number;
  what: string;
  hosts: Record<string, JobProgress>;
  pending: number;
  finished: boolean;
  error?: string;
  started_ms: number;
  finished_ms?: number;
  cancelled?: boolean;
}
export interface Readiness {
  host: string;
  files: number;
  bytes: number;
  missing_files: number;
  missing_bytes: number;
  ready: boolean;
}
export interface HostUsage {
  opens: number;
  last_open_ms: number;
  local_bytes: number;
  net_bytes: number;
}
export interface Repo {
  repo: string;
  kind: "model" | "dataset";
  selector: string;
  files: number;
  bytes: number;
  writing: number;
  revisions: number;
  hosts: Readiness[];
  usage: Record<string, HostUsage>;
  last_open_ms: number;
  error?: string;
}
export interface TreeNode {
  name: string;
  path?: string;
  kind: "dir" | "file" | "repo" | "revision" | "group" | "more" | "free";
  selector?: string;
  bytes: number;
  files: number;
  hosts?: string[];
  file?: number;
  children?: TreeNode[];
  truncated?: boolean;
}
export interface SpaceStore {
  name: string;
  kind: "host" | "archive";
  free: number;
  total: number;
  held: number;
}
export interface Copy {
  file: number;
  generation: number;
  size: number;
  path: string;
  why: string;
}
export type Step =
  | { kind: "evict"; host: string; bytes: number; copies: Copy[] }
  | { kind: "offload"; host: string; store: string; bytes: number; copies: Copy[] }
  | { kind: "replicate"; host: string; bytes: number; copies: Copy[] };
export interface Plan {
  id: number;
  goal: any;
  hosts: { host: string; free_now: number; target: number; projected_free: number; total: number }[];
  archives: {
    store: string;
    reachable: boolean;
    free_now: number;
    total: number;
    adds: number;
    projected_free: number;
  }[];
  steps: Step[];
  blocked: string[];
  notes: string[];
  feasible: boolean;
}
export interface Rule {
  name: string;
  selector: string;
  hosts: string[];
  auto: boolean;
  revision: number;
}
export interface LogLine {
  host: string;
  ts_ms: number;
  level: string;
  target: string;
  message: string;
}
