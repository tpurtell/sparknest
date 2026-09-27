// Cluster state shared by every view, refreshed every 2 s while the page is
// visible, plus routing and toasts.

import { get, post, hasToken, ApiError, type Status, type Store, type Job } from "./api";

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
});

const prev: Record<string, { t: number; r: number; s: number }> = {};

export async function poll() {
  if (!app.authed) return;
  try {
    const [status, stores, jobs, groups] = await Promise.all([
      get<Status>("/v1/status"),
      get<{ stores: Store[] }>("/v1/stores").then((v) => v.stores),
      get<{ jobs: Job[] }>("/v1/jobs").then((v) => v.jobs),
      get<{ groups: { name: string; members: string[] }[] }>("/v1/groups").then((v) => v.groups),
    ]);
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
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) app.authed = false;
    app.error = (e as Error).message;
  }
}

let timer: ReturnType<typeof setInterval> | undefined;
export function startPolling() {
  clearInterval(timer);
  poll();
  timer = setInterval(() => {
    if (document.visibilityState === "visible") poll();
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
