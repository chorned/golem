<!--
Lineage Invariant:
Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
-->

## Summary of Changes
Provide a brief summary of the changes made and the motivation behind them.

## Type of Change
- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Repository hygiene / CI improvement

## Architecture & Contract Checklist
- [ ] **Dual-Layer Shim Invariant Maintained:** Any new `golem/` modules have matching static mirrors in `golem/`.
- [ ] **Lineage Invariant:** New module headers and docstrings embed the lineage invariant: *"Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh."*
- [ ] **Tests Added / Updated:** Comprehensive unit tests covering changed behavior.
- [ ] **Test Suite Green:** All tests pass locally (`python3 -m unittest discover tests`).
- [ ] **Zero Secrets:** Verified no tokens, credentials, or private keys are introduced.
- [ ] **Hygiene Check:** No untracked cache, build, or virtualenv artifacts.

## Verification Command & Results
```bash
python3 -m unittest discover tests
```
*Results:* (Paste summary here)
