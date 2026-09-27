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
/** A link that downloads `path` (a file, or a directory as .tar). */
export const downloadUrl = (path: string) =>
  `/v1/download?path=${encodeURIComponent(path)}&token=${encodeURIComponent(token)}`;
export const authHeader = () => "Bearer " + token;
/** The page's WebSocket (a WebSocket cannot send a header). */
export const wsUrl = () =>
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/v1/ws?token=${encodeURIComponent(token)}`;
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

/** Requests in flight and the age of the oldest, for the "API slow" pill. */
export const net = { inflight: new Map<number, number>(), seq: 0 };

type Transport = (method: string, path: string, body: unknown, timeoutMs: number) => Promise<{ status: number; body: any }>;
let transport: Transport | null = null;
/** Every API call travels over the page's WebSocket (state.svelte.ts). */
export const setTransport = (t: Transport) => (transport = t);

export async function api<T = any>(method: string, path: string, body?: unknown, timeoutMs = 20_000): Promise<T> {
  if (!transport) throw new ApiError("not connected", 0);
  const id = ++net.seq;
  net.inflight.set(id, performance.now());
  try {
    const r = await transport(method, path, body, timeoutMs);
    if (r.status >= 400) throw new ApiError(r.body?.error || `HTTP ${r.status}`, r.status);
    return r.body as T;
  } finally {
    net.inflight.delete(id);
  }
}

export const get = <T = any>(p: string) => api<T>("GET", p);
/** Plans and actions may take longer than a status poll. */
export const post = <T = any>(p: string, b: unknown = {}) => api<T>("POST", p, b, 60_000);
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
  io?: SourceReport[];
}
export interface SourceReport {
  source: "Local" | { Peer: number };
  latency_us: number;
  measured: boolean;
  in_flight: number;
  bytes_per_s: number;
  bytes_total: number;
  reads_total: number;
  errors: number;
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
  notes?: string[];
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
  | { kind: "evict"; host: string; bytes: number; copies: Copy[]; requires?: number }
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
  seq?: number;
  ts_ms: number;
  level: string;
  target: string;
  message: string;
}
