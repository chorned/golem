# Fleet Topology & Node Profiles

## Fleet Machines
- **`hermes` (`localhost` / `fleet-control`):**
  - Chief of Staff & Primary Development Anchor.
  - 24/7 uptime, handles local development, repository management, unit test runs, and orchestration.
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
