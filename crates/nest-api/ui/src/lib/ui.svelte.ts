// App-wide overlays: context menu, dialogs, drawer.

export interface MenuItem {
  label: string;
  icon?: string;
  danger?: boolean;
  disabled?: boolean;
  hint?: string;
  run?: () => void;
  /** A separator line when set. */
  sep?: boolean;
}

export const menu = $state({ open: false, x: 0, y: 0, title: "", sub: "", items: [] as MenuItem[] });

export function openMenu(x: number, y: number, title: string, sub: string, items: MenuItem[]) {
  Object.assign(menu, { open: true, x, y, title, sub, items });
}
export const closeMenu = () => (menu.open = false);

type Dialog =
  | { kind: "info"; title: string; body: string }
  | { kind: "confirm"; title: string; body: string; ok: string; danger: boolean }
  | { kind: "pick"; title: string; body: string; options: { name: string; kind: string; note?: string }[]; multi: boolean };

export const dialog = $state<{ open: boolean; d: Dialog | null; resolve: ((v: any) => void) | null }>({
  open: false,
  d: null,
  resolve: null,
});

function show<T>(d: Dialog): Promise<T | null> {
  return new Promise((resolve) => {
    dialog.d = d;
    dialog.resolve = resolve;
    dialog.open = true;
  });
}

export function finish(v: unknown) {
  dialog.open = false;
  dialog.resolve?.(v);
  dialog.resolve = null;
}

export const confirm = (title: string, body = "", ok = "OK", danger = false) =>
  show<boolean>({ kind: "confirm", title, body, ok, danger }).then((v) => v === true);

/** Tell the user something; resolves when dismissed. */
export const inform = (title: string, body = "") => show<boolean>({ kind: "info", title, body }).then(() => undefined);

/** The Place dialog (multi host / single host / archive) for a selection. */
export const placing = $state({ open: false, selector: "", label: "" });
export function openPlace(selector: string, label: string) {
  Object.assign(placing, { open: true, selector, label });
}

export const pick = (
  title: string,
  options: { name: string; kind: string; note?: string }[],
  multi = true,
  body = "",
) => show<string[]>({ kind: "pick", title, body, options, multi });
