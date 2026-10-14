import pytest
import asyncio
from pathlib import Path
from golem.memory.peripheral import compile_peripheral_vision, clear_peripheral_vision_cache
from golem.mouth import DISPATCH_TASK_TOOL_DECLARATION, CONSULT_MEMORY_TOOL_DECLARATION, MOUTH_TOOLS
from golem.ingress import _execute_backstage_task, IngressGateway


def test_dispatch_task_schema_functional_primitives():
    params = DISPATCH_TASK_TOOL_DECLARATION["parameters"]
    assert "command" in params["properties"]
    assert "cwd" in params["properties"]
    assert "target_node" in params["properties"]
    assert "command" in params["required"]
    assert params["properties"]["target_node"]["enum"] == ["hermes", "desktop-pc", "oci-machine", "personal-laptop"]


def test_consult_memory_schema_registered():
    assert CONSULT_MEMORY_TOOL_DECLARATION in MOUTH_TOOLS
    params = CONSULT_MEMORY_TOOL_DECLARATION["parameters"]
    assert "path" in params["properties"]
    assert "path" in params["required"]


def test_peripheral_vision_rendering(tmp_path: Path):
    flow_dir = tmp_path / "runbooks" / "flows"
    flow_dir.mkdir(parents=True)
    test_runbook = flow_dir / "test_flow.md"
    test_runbook.write_text("---\ntitle: Test Flow\nsummary: Handles test workflows\n---\n# Body")

    rendered = compile_peripheral_vision(wiki_root=tmp_path)
    assert "runbooks/flows/test_flow.md" in rendered
    assert "Test Flow" in rendered
    assert "Handles test workflows" in rendered


@pytest.mark.asyncio
async def test_ingress_rejects_fictional_precedent_path():
    result = await _execute_backstage_task(
        task_description="Restart container",
        precedent_status="VERIFIED_RUNBOOK",
        precedent_source="runbooks/flows/fictional_nonexistent_file.md",
        target_node="hermes"
    )
    assert result.get("status") == "CONTRACT_VIOLATION"
    assert "does not exist in ~/.golem/wiki/" in result.get("error", "")


@pytest.mark.asyncio
async def test_ingress_accepts_valid_novel_exploration():
    # Verification that valid novel explorations pass contract gating
    result = await _execute_backstage_task(
        task_description="Run unique diagnostic",
        precedent_status="NOVEL_EXPLORATION",
        precedent_source="First time testing this hardware interface on Linux",
        target_node="hermes",
        dry_run=True  # Ensure harness doesn't execute raw commands in tests
    )
    assert result.get("status") != "CONTRACT_VIOLATION"


@pytest.mark.asyncio
async def test_ingress_rejects_short_novel_rationale():
    result = await _execute_backstage_task(
        task_description="Run novel command",
        precedent_status="NOVEL_EXPLORATION",
        precedent_source="short",
        target_node="hermes",
        dry_run=True,
    )
    assert result.get("status") == "CONTRACT_VIOLATION"
    assert "requires a descriptive rationale" in result.get("error", "")


@pytest.mark.asyncio
async def test_consult_memory_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_file = tmp_path / "runbooks" / "flows" / "oauth_refresh.md"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("# OAuth Refresh Flow\nStep 1: Check token")

    gateway = IngressGateway()
    monkeypatch.setattr("golem.wiki.DEFAULT_WIKI_DIR", tmp_path)
    monkeypatch.setenv("GOLEM_WIKI_DIR", str(tmp_path))

    res = await gateway._execute_consult_memory("runbooks/flows/oauth_refresh.md")
    assert res.get("status") == "SUCCESS"
    assert "OAuth Refresh Flow" in res.get("content", "")

    # Test non-existent path
    res_not_found = await gateway._execute_consult_memory("runbooks/flows/ghost.md")
    assert res_not_found.get("status") == "NOT_FOUND"


@pytest.mark.asyncio
async def test_peripheral_vision_dynamic_update_gate(tmp_path: Path):
    flow_dir = tmp_path / "runbooks" / "flows"
    flow_dir.mkdir(parents=True, exist_ok=True)
    probe_file = flow_dir / "temp_test.md"
    probe_file.write_text("---\ntitle: Temp Probe\nsummary: Dynamic update probe\n---\n# Body")

    rendered = compile_peripheral_vision(wiki_root=tmp_path)
    assert "Temp Probe" in rendered
    assert "runbooks/flows/temp_test.md" in rendered

    # Remove temporary file cleanly
    probe_file.unlink()
    rendered_after = compile_peripheral_vision(wiki_root=tmp_path)
    assert "Temp Probe" not in rendered_after


@pytest.mark.asyncio
async def test_post_flight_settlement_reflection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from golem.memory.reflection import record_task_settlement_async
    monkeypatch.setattr("golem.wiki.DEFAULT_WIKI_DIR", tmp_path)
    monkeypatch.setenv("GOLEM_WIKI_DIR", str(tmp_path))

    from golem.wiki import GolemWiki
    wiki = GolemWiki(wiki_dir=str(tmp_path))

    await record_task_settlement_async(
        task_id="task_test_99",
        task_description="Configure wireguard interface on remote VPS",
        target_node="oci-machine",
        exit_code=0,
        output_tail="Wireguard interface wg0 active and healthy",
        precedent_source="First time setting up Wireguard tunnel on ARM VPS",
        precedent_status="NOVEL_EXPLORATION",
    )

    # Check logbook entry
    log_files = list(tmp_path.glob("logbook/*.md"))
    assert len(log_files) >= 1
    log_content = log_files[0].read_text()
    assert "task_test_99" in log_content
    assert "oci-machine" in log_content

    # Check proposed novel flow
    proposals = wiki.list_pending_updates()
    assert len(proposals) >= 1
