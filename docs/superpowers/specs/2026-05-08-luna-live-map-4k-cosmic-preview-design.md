# Luna Live Map 4K Cosmic Preview Design

## Goal

Create a separate, approval-only preview of Luna Live Map that is dramatically more cinematic and 3D while preserving the current Luna dashboard until the operator explicitly approves installation.

## Current Context

The current Luna Live Map is part of the current dashboard in `D:/SurgeApp/luna_dashboard`. The named launcher, `D:/SurgeApp/LaunchLunaDashboard_Edge.pyw`, forces Edge and delegates to `LaunchLunaDashboard.pyw`; no old Luna terminal files are needed for this work.

The live map currently uses `D:/SurgeApp/luna_dashboard/index.html`, `D:/SurgeApp/luna_dashboard/style.css`, `D:/SurgeApp/luna_dashboard/app.js`, and locally vendored visual libraries in `D:/SurgeApp/luna_dashboard/vendor`. The existing static safety test for the map is `D:/SurgeApp/tests/test_live_map_animation_static.py`.

## Proposed Architecture

Build a preview-only page and module beside the current dashboard instead of replacing the production map first. The preview page will reuse the dashboard's local browser/server path and local `three.min.js`, but it will not alter worker logic, terminal launchers, scheduled tasks, memory files, or backend runtime behavior.

The preview will be treated as a candidate visual layer. Approval installs it into the current Luna Live Map. Denial removes only the preview additions and leaves the current dashboard structure and function as it was before the preview.

## Visual Direction

The preview should feel out of this world: a 3D Luna command universe rather than a flat constellation.

- Deep 3D starfield with parallax depth and high-density 4K-friendly rendering.
- Luna Core as a glowing energy sphere with layered corona rings, rotating aura shells, and expanding pulse waves.
- Agent nodes as luminous 3D orbit bodies around the core.
- Energy trails for handoffs between Luna Core, current work, tier progression, verifier, guardian, memory, and blockers.
- Shooting stars, cosmic dust, nebula veils, and subtle camera drift.
- Tier/progress state as large orbital arcs and ring structures around Luna Core.
- Visual intensity should increase when Luna is active, while idle mode remains alive and cinematic.

## Safety And Revert Model

Preview work must be additive:

- Create new preview files rather than overwrite the current map first.
- Do not delete existing files.
- Do not modify `worker.py`.
- Do not modify old terminal launchers or legacy terminal files.
- Do not change `LaunchLunaDashboard_Edge.pyw` unless a later approved install step specifically needs a launcher link, which is not expected.
- Keep all new preview code isolated enough that denial can be handled by deleting only the new preview files and reverting any preview link in `index.html`, if one is added.

## Files Expected

Likely new files:

- `D:/SurgeApp/luna_dashboard/live-map-4k-preview.html`
- `D:/SurgeApp/luna_dashboard/live-map-4k-preview.js`
- `D:/SurgeApp/luna_dashboard/live-map-4k-preview.css`

Likely modified files:

- `D:/SurgeApp/luna_dashboard/index.html` only if adding a preview link or button.
- `D:/SurgeApp/tests/test_live_map_animation_static.py` or a new test file to pin preview isolation and no legacy terminal coupling.

## Runtime Behavior

The preview should run from the existing local dashboard server at `http://127.0.0.1:8765/`. It should not require network CDNs or package installs. It should use existing local vendor assets already present under `D:/SurgeApp/luna_dashboard/vendor`.

The preview should respect Luna's existing motion/performance intent:

- Pause or reduce animation when the tab is hidden.
- Keep rendering bounded for normal dashboard use.
- Preserve a reduced-motion path.
- Avoid duplicate animation loops after reloads.

## Verification

Before asking for operator approval, verify:

- Existing dashboard still opens.
- Current Luna Live Map still exists.
- Preview page opens from the local server.
- Browser screenshot confirms a nonblank 3D scene.
- Static tests confirm no old terminal coupling and no `worker.py` surface coupling.
- If possible, console logs show no fatal browser errors.

## Approval Gate

After implementation, show the preview to the operator in the browser. If approved, install into the current Luna Live Map. If denied, remove preview additions and restore any preview link changes.
