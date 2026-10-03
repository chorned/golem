# Task Spec: M5-BUILD (Flagship Documentation & Operating Manuals)

## Metadata
- **Task ID:** `M5-BUILD`
- **Type:** `build`
- **Risk Tier:** `low`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- Documentation Files: `README.md`, `AGENTS.md`.
- Document architectural flex: why naive PTY scraping fails vs. Golem's decoupled Mouth/Hands/Sensory architecture.
- ASCII topology diagrams, quickstart configuration, and First Mate/Herdr lineage credits.
- `AGENTS.md` operational pair-programming rules and backstage invariants.

## Hand B (Adversarial Auditor) Invariants
1. README renders cleanly on GitHub preview without broken markdown or dead links.
2. Architectural flex clearly articulates PTY buffer corruption, token starvation, and async isolation.
3. Lineage invariant explicitly present.

## Blast Radius & Boundaries
- Markdown files only. Zero code impact.
