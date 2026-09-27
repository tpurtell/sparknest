// Render an overlay at the end of <body>: a fixed-position element inside an
// ancestor with a transform or backdrop-filter (the views' entry animation,
// the glass panels) is positioned against that ancestor, not the screen.
export function portal(node: HTMLElement) {
  document.body.appendChild(node);
  return {
    destroy() {
      node.remove();
    },
  };
}
