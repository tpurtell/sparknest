// Cluster state shared by every view, refreshed every 2 s while the page is
// visible, plus routing and toasts.

import { get, post, hasToken, ApiError, net, eventsUrl, type Status, type Store, type Job } from "./api";

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
}

export const app = $state({
  authed: hasToken(),
  status: null as Status | null,
  stores: [] as Store[],
  jobs: [] as Job[],
  groups: [] as { name: string; members: string[] }[],
  /** Fabric bytes/s per host, from counter differences between polls. */
  rates: {} as Record<string, Rates>,
  error: "",
  tick: 0,
  /** Seconds the oldest request has been waiting, when that is long. */
  slow: 0,
  /** Bumped when the namespace or placement changed: views refetch. */
  changed: 0,
  /** The live stream is connected (else polling). */
  live: false,
});

const prev: Record<string, { t: number; r: number; s: number }> = {};

export const poll = singleFlight(pollOnce);

async function pollOnce() {
  if (!app.authed) return;
  try {
    const [status, stores, jobs, groups] = await Promise.all([
      get<Status>("/v1/status"),
      get<{ stores: Store[] }>("/v1/stores").then((v) => v.stores),
      get<{ jobs: Job[] }>("/v1/jobs").then((v) => v.jobs),
      get<{ groups: { name: string; members: string[] }[] }>("/v1/groups").then((v) => v.groups),
    ]);
    apply(status, stores, jobs, groups);
    // Without the stream, refetch views on every poll.
    if (!app.live) app.changed++;
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) app.authed = false;
    app.error = (e as Error).message;
  }
}

function apply(status: Status, stores: Store[], jobs: Job[], groups: { name: string; members: string[] }[]) {
  {
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
        };
      }
      prev[n.name] = { t: now, r: i.fabric_read_bytes, s: i.fabric_served_bytes };
    }
    app.status = status;
    app.stores = stores;
    app.jobs = jobs;
    app.groups = groups;
    app.rates = rates;
    app.error = "";
    app.tick++;
  }
}

// ---------------------------------------------------------------- live

let source: EventSource | null = null;
let lastEvent = 0;

/** Subscribe to the node's event stream; polling covers any gap. */
function connect() {
  source?.close();
  source = new EventSource(eventsUrl());
  source.addEventListener("state", (e) => {
    lastEvent = performance.now();
    app.live = true;
    try {
      const v = JSON.parse((e as MessageEvent).data);
      apply(v.status, v.stores, v.jobs, v.groups);
    } catch {
      /* a malformed frame: the next one replaces it */
    }
  });
  source.addEventListener("changed", () => {
    lastEvent = performance.now();
    app.changed++;
  });
  source.onerror = () => {
    // EventSource reconnects by itself; until it does, polling fills in.
    app.live = false;
  };
}

let timer: ReturnType<typeof setInterval> | undefined;
export function startPolling() {
  clearInterval(timer);
  poll();
  connect();
  timer = setInterval(() => {
    const oldest = Math.min(Infinity, ...net.inflight.values());
    app.slow = isFinite(oldest) && performance.now() - oldest > 4000 ? Math.round((performance.now() - oldest) / 1000) : 0;
    // State arrives when it changes; a silent stream for long is a dead one.
    if (app.live && performance.now() - lastEvent > 45_000) {
      app.live = false;
      connect();
    }
    if (!app.live && document.visibilityState === "visible") poll();
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
    await poll();
  } catch (e) {
    toast((e as Error).message, true);
  }
}

export async function cancelJob(id: number) {
  try {
    await post(`/v1/jobs/${id}/cancel`);
    toast("Cancelling");
    await poll();
  } catch (e) {
    toast((e as Error).message, true);
  }
}

// ---------------------------------------------------------------- routing

export type View = "overview" | "models" | "space" | "plans" | "jobs" | "rules" | "files" | "logs";
export const VIEWS: View[] = ["overview", "models", "space", "plans", "jobs", "rules", "files", "logs"];

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
