# Fleet Topology & Node Profiles

## Fleet Machines
- **`hermes` Node (`localhost` / `fleet-control`):**
  - Chief of Staff & Primary Development Anchor (24/7 uptime).
  - Physical macOS controller machine running under `/Users/hermes` system user account.
  - **`hermes` CLI / Prototype Agent**: Standalone prototype agent executable invoked via `hermes` in terminal; predecessor to Golem leveraged backstage as an execution hand (Docker, storage, homelab ops).
  - **macOS Computer Use**: `macos-computer-use-kit` (AX Accessibility and Screen Recording permissions verified active). Enables GUI automation, AX tree inspection, desktop capture, and autonomous Chrome OAuth/token refresh flows.
- **`desktop-pc` (`100.80.59.45` / `compute-grid`):**
  - Dedicated Windows CUDA workstation and compute lane.
  - Handles NVIDIA GPU / CUDA workloads, PyTorch, model training, and heavy benchmarks.
- **`personal-laptop` (`100.96.38.5` / `dev-grid`):**
  - Carl's personal MacBook workstation (strict ad-hoc execution).
  - Sleeps frequently; never run unsolicited background jobs here. Explicit requests only ("on my laptop").
- **`oci-machine` (`100.113.173.33` / `cloud-ops`):**
  - Bounded Oracle Cloud VPS (Linux aarch64).
  - Strictly reserved for live production services, ingress, WireGuard, and DNS.

## Herdr Persistent Workspaces
- `w3`: `cloud-ops` (OCI machine operations)
- `w4`: `dev-grid` (personal laptop development lane)
- `w5`: `compute-grid` (desktop-pc CUDA workloads)
- `w6`: `fleet-control` (localhost Antigravity Chief of Staff)
- `w7`: `homelab-ops` (Hermes Butler, Docker, storage pools)
