# Task Spec: M3-BUILD (Observability WebUI Architecture & Dashboard)

## Metadata
- **Task ID:** `M3-BUILD`
- **Type:** `build`
- **Risk Tier:** `medium`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Source Modules: `golem/webui/`, `golem/sensory/`, `app.py`.
- Async web server (aiohttp/Starlette) with SSE or WebSocket streaming.
- Stitch Design integration with clean, dark-mode semantic HTML/CSS fallback:
  - Eye Agent Log Pane
  - Nose Agent Log Pane
  - Mouth Agent Log Pane
  - Hand Agents Grid (0 to 9 dynamic mounting/unmounting terminal DIVs).

## Hand B (Adversarial Auditor) Invariants
1. WebUI server initializes cleanly with non-blocking lifecycle.
2. Dashboard renders in browsers without missing assets, fonts, or JS errors.
3. Stitch MCP utilized if available, with 100% graceful fallback to semantic CSS/JS if unavailable.
4. Unit tests verify SSE payload formatting and client subscriber handling.

## Blast Radius & Boundaries
- Confined to `golem/webui/` and optional CLI flag `--webUI` / env var `GOLEM_ENABLE_WEBUI`.
