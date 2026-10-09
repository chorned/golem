# Contributing to Golem

> **Lineage Invariant:**  
> *Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.*

Thank you for your interest in contributing to **Golem: The Autonomous Fleet Chief of Staff**!  
We welcome bug reports, feature proposals, documentation improvements, and pull requests.

---

## 1. Development Environment Setup

Golem requires **Python 3.11** or newer (fully tested on Python 3.11, 3.12, 3.13, and 3.14).

### Step-by-Step Onboarding

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/kunchenguid/golem.git
   cd golem
   ```

2. **Create and Activate a Virtual Environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install Dependencies in Editable Mode:**
   ```bash
   python -m pip install --upgrade pip
   pip install -e ".[dev]"
   ```

4. **Verify Installation:**
   ```bash
   golem --help
   ```

---

## 2. Code Layout & Architecture

Golem follows a tripartite sensory and execution architecture:

```
golem/
├── golem/                    # Canonical implementation package
│   ├── gateway/              # Ingress routing, session state, thread lifecycle
│   │   ├── ingress.py        # Front-of-House Mouth router & Slack event handling
│   │   ├── router.py         # SessionRouter & workspace provisioning
│   │   └── session.py        # SessionHandle & concurrency decoupling
│   ├── agent/                # Cognitive agent layer & Mouth LLM
│   │   ├── adapter.py        # Stream synchronization & IPC adapter
│   │   ├── mouth.py          # Mouth LLM orchestration & briefing synthesis
│   │   ├── prompts.py        # Calibrated system prompts
│   │   ├── events.py         # Strongly-typed AgentEvent protocol
│   │   └── drivers/          # Pluggable CLI drivers (Antigravity, Claude, OpenCode)
│   ├── execution/            # Backstage Hand worker execution & Herdr mesh
│   │   ├── harness.py        # Dual-pane workspace harness (Mouth vs Hands)
│   │   ├── runner.py         # RunnerExecutor with 3.0s non-blocking yield
│   │   ├── delegation.py     # WorkerDelegator (AGY default, opencode, hermes)
│   │   ├── buffer.py         # Terminal scrollback buffer management
│   │   └── mesh.py           # Herdr fleet PTY mesh abstraction
│   ├── sensory/              # Sensory monitoring plane
│   │   ├── bus.py            # ShoulderTapEventBus async alert routing
│   │   ├── eyes.py           # EyesTaskObserver (buffer watching, SHA-256 change hashing)
│   │   └── nose.py           # NoseSentinel (24/7 health watchdog & telemetry)
│   ├── transport/            # Wire egress normalization & file transfer
│   │   ├── chunker.py        # Slack 3,800-char paragraph chunker
│   │   ├── normalizer.py     # ANSI/TUI striping & Slack mrkdwn normalizer
│   │   └── file_transfer.py  # Inbound/outbound Slack file transfers
│   ├── webui/                # Real-time command center WebUI
│   │   ├── server.py         # Async aiohttp server & SSE event streamer
│   │   └── templates/        # Dark-mode dashboard HTML/CSS/JS assets
│   ├── config.py             # Dual-key environment configuration
│   └── cli.py                # Command-line interface entrypoint
│
├── tests/                    # Comprehensive unit, adversarial, and stress tests
├── tests/e2e/                # Multi-tier end-to-end acceptance tests
├── .github/                  # CI/CD workflows and issue templates
├── pyproject.toml            # PEP 621 packaging specification
└── setup.py                  # Package setup and build hooks
```

---

## 3. Architecture & Compatibility Invariant Maintenance

Golem preserves backward compatibility across flattened modules and subpackage facades.

### The Contract
1. **Module Flattening:**  
   Core modules reside directly under `golem/` (`golem.ingress`, `golem.mouth`, `golem.harness`, `golem.bus`, etc.).
2. **Object Identity (`assertIs`):**  
   `golem.ingress.IngressGateway is golem.gateway.ingress.IngressGateway` holds 100% true across all facade modules.
3. **Mock Patch Fidelity:**  
   Patching `golem.gateway.ingress.call_gemini_api` mutates the exact same object in memory as `golem.ingress.call_gemini_api`.
4. **Compatibility Facades:**  
   Subpackage modules re-export from flattened modules to preserve backward compatibility for legacy imports.

---

## 4. Testing Instructions

All contributions must pass the test suite with zero failures and zero regressions.

### Running Tests

- **Run the full test suite:**
  ```bash
  python3 -m unittest discover tests
  ```

- **Run specific unit test modules:**
  ```bash
  python3 -m unittest tests/test_ci_and_repo_hygiene.py
  python3 -m unittest tests/test_webui_observability.py
  python3 -m unittest tests/test_golem_namespace_and_shims.py
  ```

- **Run the End-to-End (E2E) feature verification suite:**
  ```bash
  python3 -m unittest tests/e2e/test_e2e_tier1_features.py
  python3 -m unittest tests/e2e/test_e2e_tier2_boundaries.py
  python3 -m unittest tests/e2e/test_e2e_tier3_combinations.py
  python3 -m unittest tests/e2e/test_e2e_tier4_scenarios.py
  ```

- **Run test coverage:**
  ```bash
  coverage run -m unittest discover tests
  coverage report -m
  ```

---

## 5. Repository Hygiene & Zero Secrets Policy

- **No Secrets:** Never commit API keys, bot tokens (`xoxb-*`, `xapp-*`), private keys, or passwords.
- **Environment Configuration:** Use `.env` locally (create from `.env.example`). The `.gitignore` file enforces that `.env` is never committed.
- **Untracked Artifacts:** Ensure temporary files, bytecode (`__pycache__`), build outputs (`build/`, `dist/`, `*.egg-info`), and virtualenvs (`.venv/`) are ignored by `.gitignore`.

---

## 6. Pull Request (PR) Guidelines

1. **Branch Naming:**
   - Features: `feat/feature-name`
   - Bug fixes: `fix/issue-description`
   - Documentation: `docs/improvement`
   - Refactoring / Hygiene: `chore/repo-hygiene`

2. **Commit Messages:**
   Use clear, imperative commit messages (e.g., `feat: implement pluggable AGY worker dispatch`, `fix: prevent SSE subscriber queue leak`).

3. **Pre-Submission Checklist:**
   - [ ] Created feature branch from `main`.
   - [ ] Added comprehensive unit tests covering new logic or bug fixes.
   - [ ] Maintained module compatibility across facades.
   - [ ] Ran `python3 -m unittest discover tests` and verified 100% test pass rate.
   - [ ] Verified zero sensitive credentials or tokens in tracked files.
   - [ ] Embedded the lineage invariant in new module docstrings where applicable.

4. **CI Matrix:**
   Every PR triggers GitHub Actions CI running tests across Python 3.11, 3.12, and 3.13. All matrix jobs must be green before merge.

---

## 7. License

By contributing to Golem, you agree that your contributions will be licensed under the project's [Apache License Version 2.0](LICENSE).
