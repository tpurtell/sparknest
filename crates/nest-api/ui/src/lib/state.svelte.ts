// Cluster state shared by every view, refreshed every 2 s while the page is
// visible, plus routing and toasts.

import { post, hasToken, tokenRefused, forgetToken, ApiError, net, wsUrl, setTransport, type Status, type Store, type DiskInfo, type Job, type LogLine } from "./api";

/** One run of `fn` at a time: a slow server gets one request of each kind,
 * never a growing pile. A call made meanwhile is not lost: it runs once
 * more when the current run ends (so the latest change is always shown). */
export function singleFlight<A extends unknown[]>(fn: (...a: A) => Promise<unknown>): (...a: A) => Promise<void> {
  let busy = false;
  let again: A | null = null;
  const run = async (...a: A): Promise<void> => {
    if (busy) {
      again = a;
      return;
    }
    busy = true;
    try {
      await fn(...a);
    } catch {
      /* the caller reports its own errors */
    } finally {
      busy = false;
    }
    if (again) {
      const next = again;
      again = null;
      await run(...next);
    }
  };
  return run;
}

export interface Rates {
  read: number;
  served: number;
  /** From its own disk, tracked by sparknest. */
  local: number;
}

export const app = $state({
  authed: hasToken(),
  status: null as Status | null,
  stores: [] as Store[],
  /** Hosts with extra disks: their stores, by host name (ADR-073). */
  disks: {} as Record<string, DiskInfo[]>,
  jobs: [] as Job[],
  groups: [] as { name: string; members: string[] }[],
  /** Fabric bytes/s per host, from counter differences between polls. */
  rates: {} as Record<string, Rates>,
  /** Each running job's current rate (bytes/s), smoothed over a few seconds. */
  jobRates: {} as Record<number, number>,
  error: "",
  tick: 0,
  /** Seconds the oldest request has been waiting, when that is long. */
  slow: 0,
  /** Bumped when the namespace or placement changed: views refetch. */
  changed: 0,
  /** The page's socket is open. */
  live: false,
});

const prev: Record<string, { t: number; r: number; s: number; l: number }> = {};
const prevJob: Record<number, { t: number; b: number; r: number }> = {};

/** Bytes a job has moved so far: whole files, plus what has landed of those in flight. */
export const jobDone = (j: { hosts: Record<string, { done_bytes: number; partial_bytes?: number }> }) =>
  Object.values(j.hosts).reduce((a, p) => a + p.done_bytes + (p.partial_bytes ?? 0), 0);
export const jobTotal = (j: { hosts: Record<string, { total_bytes: number }> }) =>
  Object.values(j.hosts).reduce((a, p) => a + p.total_bytes, 0);

let runningIds = new Set<number>();
let uiSeen = "";

function apply(status: Status, stores: Store[], jobs: Job[], groups: { name: string; members: string[] }[]) {
  {
    // A job that just finished changed what views show: refetch them.
    const running = new Set(jobs.filter((j) => !j.finished).map((j) => j.id));
    if ([...runningIds].some((id) => !running.has(id))) app.changed++;
    runningIds = running;
    const now = performance.now();
    const rates: Record<string, Rates> = {};
    for (const n of status.nodes) {
      const i = n.info;
      if (!i) continue;
      const p = prev[n.name];
      if (p && now > p.t) {
        const dt = (now - p.t) / 1000;
        rates[n.name] = {
          read: Math.max(0, (i.fabric_read_bytes - p.r) / dt),
          served: Math.max(0, (i.fabric_served_bytes - p.s) / dt),
          local: Math.max(0, ((i.local_read_bytes ?? 0) - p.l) / dt),
        };
      }
      prev[n.name] = { t: now, r: i.fabric_read_bytes, s: i.fabric_served_bytes, l: i.local_read_bytes ?? 0 };
    }
    app.status = status;
    app.stores = stores;
    app.jobs = jobs;
    const jr: Record<number, number> = {};
    for (const j of jobs) {
      if (j.finished) continue;
      const b = jobDone(j);
      const p = prevJob[j.id];
      if (p && now > p.t) {
        // Exponential smoothing over ~3 s; a retried file can step back.
        const inst = Math.max(0, (b - p.b) / ((now - p.t) / 1000));
        const a = Math.min(1, (now - p.t) / 3000);
        jr[j.id] = p.r + a * (inst - p.r);
      }
      prevJob[j.id] = { t: now, b, r: jr[j.id] ?? 0 };
    }
    for (const id of Object.keys(prevJob)) if (!jobs.some((j) => j.id === Number(id) && !j.finished)) delete prevJob[Number(id)];
    app.jobRates = jr;
    app.groups = groups;
    app.rates = rates;
    app.error = "";
    app.tick++;
  }
}

// ---------------------------------------------------------------- live

let socket: WebSocket | null = null;
let lastEvent = 0;
let retry = 1000;
let everOpen = false;
let failures = 0;
let logSub: Record<string, unknown> | null = null;
let onLines: ((l: LogLine[]) => void) | null = null;

// Calls over the socket: sent now if it is open, else when it opens.
let callId = 0;
const pending = new Map<number, { resolve: (v: { status: number; body: any }) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
const unsent: { id: number; frame: string }[] = [];
setTransport((method, path, body, timeoutMs) =>
  new Promise((resolve, reject) => {
    const id = ++callId;
    const timer = setTimeout(() => {
      pending.delete(id);
      const i = unsent.findIndex((u) => u.id === id);
      if (i >= 0) unsent.splice(i, 1);
      reject(new ApiError(`${path.split("?")[0]} did not answer within ${timeoutMs / 1000} s`, 0));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
    const frame = JSON.stringify({ type: "call", id, method, path, body });
    if (socket?.readyState === WebSocket.OPEN) socket.send(frame);
    else unsent.push({ id, frame });
  }),
);

/** One WebSocket per page carries every call and every live update. */
function connect() {
  socket?.close();
  const ws = new WebSocket(wsUrl());
  socket = ws;
  ws.onopen = () => {
    everOpen = true;
    failures = 0;
    retry = 1000;
    app.live = true;
    lastEvent = performance.now();
    for (const u of unsent.splice(0)) ws.send(u.frame);
    if (logSub) ws.send(JSON.stringify({ type: "logs", ...logSub }));
  };
  ws.onmessage = (e) => {
    lastEvent = performance.now();
    let m: any;
    try {
      m = JSON.parse(e.data);
    } catch {
      return;
    }
    if (m.type === "reply") {
      const p = pending.get(m.id);
      if (!p) return;
      pending.delete(m.id);
      clearTimeout(p.timer);
      p.resolve({ status: m.status, body: m.body });
    } else if (m.type === "state") {
      // The node now serves another UI (it was upgraded): load that one.
      if (m.data.ui) {
        if (uiSeen && uiSeen !== m.data.ui) location.reload();
        uiSeen = m.data.ui;
      }
      app.error = "";
      apply(m.data.status, m.data.stores, m.data.jobs, m.data.groups);
      app.disks = m.data.disks ?? {};
    } else if (m.type === "changed") app.changed++;
    else if (m.type === "lines") onLines?.(m.lines);
  };
  ws.onclose = () => {
    if (socket !== ws) return; // replaced on purpose
    app.live = false;
    // Calls sent on this socket will not be answered.
    for (const [id, p] of pending) {
      if (unsent.some((u) => u.id === id)) continue;
      clearTimeout(p.timer);
      p.reject(new ApiError("connection to the node was lost", 0));
      pending.delete(id);
    }
    app.error = "reconnecting…";
    const again = () => {
      setTimeout(() => socket === ws && connect(), retry);
      retry = Math.min(retry * 2, 15_000);
    };
    // Refused before ever opening: ask over HTTP whether the token is the
    // reason (a phone that killed and reloaded the tab, a proxy dropping
    // the socket, or a node restarting is not a reason to log out).
    if (!everOpen && ++failures >= 2) {
      tokenRefused().then((refused) => {
        if (socket !== ws) return;
        if (refused) {
          forgetToken();
          app.authed = false;
        }
        else again();
      });
      return;
    }
    again();
  };
}

/** Follow logs over the page's socket until the returned function runs. */
export function followLogs(q: Record<string, unknown>, handler: (l: LogLine[]) => void): () => void {
  logSub = q;
  onLines = handler;
  if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: "logs", ...q }));
  return () => {
    if (onLines !== handler) return;
    logSub = null;
    onLines = null;
    if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: "logs_off" }));
  };
}

let timer: ReturnType<typeof setInterval> | undefined;
export function startLive() {
  clearInterval(timer);
  everOpen = false;
  failures = 0;
  connect();
  timer = setInterval(() => {
    const oldest = Math.min(Infinity, ...net.inflight.values());
    app.slow = isFinite(oldest) && performance.now() - oldest > 4000 ? Math.round((performance.now() - oldest) / 1000) : 0;
    // The node sends state at least every few seconds and pings every 20:
    // a silent socket is a dead one.
    if (app.live && performance.now() - lastEvent > 45_000) connect();
  }, 2000);
}

// ---------------------------------------------------------------- derived

export const nodes = () => app.status?.nodes ?? [];
export const hostNames = () => nodes().map((n) => n.name);
export const leaderName = () =>
  app.status?.nodes.find((n) => n.node === app.status?.leader)?.name ?? "";
export const runningJobs = () => app.jobs.filter((j) => !j.finished);

/** Every place a copy can go: hosts, then archive stores. */
export function targets(): { name: string; kind: "host" | "store" }[] {
  return [
    ...nodes().map((n) => ({ name: n.name, kind: "host" as const })),
    ...app.stores.map((s) => ({ name: s.name, kind: "store" as const })),
  ];
}

/** A host's extra disks (not its primary store); empty for a one-disk host. */
export const extraDisks = (host: string) => (app.disks[host] ?? []).filter((d) => !d.primary);

// ---------------------------------------------------------------- toasts

export const toasts = $state<{ id: number; msg: string; err: boolean }[]>([]);
let toastId = 0;
export function toast(msg: string, err = false) {
  const id = ++toastId;
  toasts.push({ id, msg, err });
  setTimeout(
    () => {
      const i = toasts.findIndex((t) => t.id === id);
      if (i >= 0) toasts.splice(i, 1);
    },
    err ? 7000 : 3500,
  );
}

/** Start a job without leaving the page; refresh so it shows at once. */
export async function startJob(path: string, body: unknown, label: string) {
  try {
    await post(path, body);
    toast(label);
  } catch (e) {
    toast((e as Error).message, true);
  }
}

export async function cancelJob(id: number) {
  try {
    await post(`/v1/jobs/${id}/cancel`);
    toast("Cancelling");
  } catch (e) {
    toast((e as Error).message, true);
  }
}

// ---------------------------------------------------------------- routing

export type View = "overview" | "models" | "space" | "plans" | "io" | "loads" | "jobs" | "rules" | "files" | "logs";
export const VIEWS: View[] = ["overview", "models", "space", "plans", "io", "loads", "jobs", "rules", "files", "logs"];

function parse() {
  const h = location.hash.replace(/^#\/?/, "");
  const [path, q] = h.split("?");
  const [view, ...rest] = path.split("/");
  return {
    view: (VIEWS.includes(view as View) ? view : "overview") as View,
    arg: decodeURIComponent(rest.join("/")),
    params: new URLSearchParams(q ?? ""),
  };
}

export const route = $state(parse());
addEventListener("hashchange", () => Object.assign(route, parse()));

export function go(view: View, arg = "", params: Record<string, string> = {}) {
  const q = new URLSearchParams(params).toString();
  location.hash = `/${view}${arg ? "/" + encodeURIComponent(arg) : ""}${q ? "?" + q : ""}`;
}

// ---------------------------------------------------------------- motion

export const reducedMotion = () =>
  typeof matchMedia !== "undefined" && matchMedia("(prefers-reduced-motion: reduce)").matches;
