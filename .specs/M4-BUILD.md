# Task Spec: M4-BUILD (GitHub Actions CI & Open-Source Assets)

## Metadata
- **Task ID:** `M4-BUILD`
- **Type:** `build`
- **Risk Tier:** `low`
- **Status:** `[VERIFIED]`

## Hand A (Primary Actor) Scope
- CI Configuration: `.github/workflows/ci.yml`.
- Multi-version test matrix: Python 3.11, 3.12, 3.13.
- Assets: `LICENSE` (Apache-2.0 / MIT), `CONTRIBUTING.md`, `.gitignore`, `.github/ISSUE_TEMPLATE/`.

## Hand B (Adversarial Auditor) Invariants
1. CI YAML syntax conforms strictly to GitHub Actions schema.
2. Steps include dependency install, linting, mock verification, and `python -m unittest discover tests`.
3. Open-source licenses and contribution guidelines are complete and accurate.

## Blast Radius & Boundaries
- Non-functional repository configuration and docs. Zero impact on runtime daemon.
