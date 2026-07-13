// Mono-line icon set — no emoji, so the HUD renders identically on every
// OS/projector. Stroke-based, inherits currentColor, one visual grammar.
// (Pattern adapted from the Marshal reference app.)

const paths = {
  // pipeline stages
  stream: '<path d="M3 7h12M3 12h16M3 17h10"/><circle cx="19" cy="7" r="1.4" fill="currentColor" stroke="none"/><circle cx="17" cy="17" r="1.4" fill="currentColor" stroke="none"/>',
  window: '<rect x="4" y="5" width="16" height="14" rx="2"/><path d="M4 10h16M9 10v9M15 10v9"/>',
  radar: '<circle cx="12" cy="12" r="8"/><circle cx="12" cy="12" r="4.2"/><path d="M12 12l5.5-5.5"/><circle cx="12" cy="12" r="1.2" fill="currentColor" stroke="none"/>',
  vector: '<circle cx="12" cy="12" r="8"/><path d="M12 4v3M12 17v3M4 12h3M17 12h3"/><circle cx="12" cy="12" r="1.6" fill="currentColor" stroke="none"/>',
  agent: '<rect x="5" y="7" width="14" height="11" rx="3"/><path d="M12 7V4M8.5 4h7"/><circle cx="9.5" cy="12" r="1.1" fill="currentColor" stroke="none"/><circle cx="14.5" cy="12" r="1.1" fill="currentColor" stroke="none"/><path d="M9.5 15.2h5"/>',
  boat: '<path d="M4 15h16l-2.5 4h-11L4 15z"/><path d="M12 15V5M12 5l5 6.5H12"/>',
  // feed vocabulary
  traffic: '<path d="M4 17l4-6 4 3 5-8 3 4"/><path d="M4 20h16"/>',
  surge: '<path d="M12 4L2.8 19.5h18.4L12 4z"/><path d="M12 10v4.5M12 17.2v.3"/>',
  kb: '<path d="M7 3.5h7l4 4V20.5H7v-17z"/><path d="M14 3.5v4h4M10 12h5M10 15.5h5"/>',
  insert: '<circle cx="12" cy="12" r="8.5"/><path d="M8 12.2l2.7 2.7L16 9.4"/>',
  reason: '<rect x="5" y="5" width="14" height="14" rx="3"/><path d="M9 12h6M12 9v6"/>',
  launch: '<path d="M8 5.5v13l11-6.5-11-6.5z"/>',
};

/** Inline SVG for a named icon. `size` in px; stroke inherits currentColor. */
export function icon(name, size = 16, cls = '') {
  const body = paths[name] ?? paths.reason;
  return `<svg class="ic ${cls}" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${body}</svg>`;
}
