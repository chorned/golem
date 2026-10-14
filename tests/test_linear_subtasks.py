import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from golem.linear_bridge import LinearBridge, LinearBridgeError
from golem.mouth import (
    PLAN_TASK_WITH_SUBTASKS_DECLARATION,
    CHECK_LINEAR_TASK_STATUS_DECLARATION,
    APPROVE_LINEAR_TASK_DECLARATION,
    DISPATCH_TASK_TOOL_DECLARATION,
    MOUTH_TOOLS,
    BASE_SYSTEM_INSTRUCTION,
)
from golem.ingress import IngressGateway, _execute_backstage_task
from golem.runner import TaskHandle, RunnerExecutor


@pytest.fixture
def bridge():
    return LinearBridge(api_key="mock_key", project_slug="golem-brain-109337da0931")


@pytest.mark.asyncio
async def test_create_plan_with_subtasks(bridge):
    mock_states = {"in progress": "st-in-prog", "planned": "st-planned"}
    bridge._project_id_cache = "proj-1"
    bridge._team_id_cache = "team-1"
    bridge._state_cache = mock_states

    parent_resp = {"issueCreate": {"issue": {"id": "p-1", "identifier": "CHO-100", "url": "http://p", "title": "Podcast"}}}
    child_resp_1 = {"issueCreate": {"issue": {"id": "c-1", "identifier": "CHO-101", "url": "http://c1", "title": "Ep 1"}}}
    child_resp_2 = {"issueCreate": {"issue": {"id": "c-2", "identifier": "CHO-102", "url": "http://c2", "title": "Ep 2"}}}

    with patch.object(bridge, "execute_query", AsyncMock(side_effect=[parent_resp, child_resp_1, child_resp_2])):
        res = await bridge.create_plan_with_subtasks(
            parent_title="Podcast",
            parent_description="Scope",
            subtasks=["Ep 1 outline", "Ep 2 outline"],
        )
        assert res["parent"]["identifier"] == "CHO-100"
        assert len(res["subtasks"]) == 2
        assert res["subtasks"][0]["identifier"] == "CHO-101"


@pytest.mark.asyncio
async def test_settle_subtask_transitions_parent_when_all_done(bridge):
    mock_states = {"claimed fixed": "st-cf", "in progress": "st-prog"}
    bridge._project_id_cache = "proj-1"
    bridge._team_id_cache = "team-1"
    bridge._state_cache = mock_states

    tree_resp = {
        "issue": {
            "parent": {
                "id": "p-1",
                "identifier": "CHO-100",
                "title": "Podcast",
                "children": {
                    "nodes": [
                        {"id": "c-1", "identifier": "CHO-101", "state": {"name": "Claimed Fixed"}},
                        {"id": "c-2", "identifier": "CHO-102", "state": {"name": "Claimed Fixed"}},
                    ]
                },
            }
        }
    }

    with patch.object(bridge, "execute_query", AsyncMock(side_effect=[
        {"commentCreate": {"success": True}},  # Subtask comment
        {"issueUpdate": {"success": True}},    # Subtask update
        tree_resp,                             # Query tree
        {"commentCreate": {"success": True}},  # Parent comment
        {"issueUpdate": {"success": True}},    # Parent update
    ])):
        res = await bridge.settle_subtask_and_evaluate_parent("c-1", "Done")
        assert res["parent_settled"] is True
        assert res["parent_key"] == "CHO-100"


@pytest.mark.asyncio
async def test_settle_subtask_keeps_parent_in_progress_when_siblings_pending(bridge):
    mock_states = {"claimed fixed": "st-cf", "in progress": "st-prog"}
    bridge._project_id_cache = "proj-1"
    bridge._team_id_cache = "team-1"
    bridge._state_cache = mock_states

    tree_resp = {
        "issue": {
            "parent": {
                "id": "p-1",
                "identifier": "CHO-100",
                "title": "Podcast",
                "children": {
                    "nodes": [
                        {"id": "c-1", "identifier": "CHO-101", "state": {"name": "Claimed Fixed"}},
                        {"id": "c-2", "identifier": "CHO-102", "state": {"name": "In Progress"}},
                    ]
                },
            }
        }
    }

    with patch.object(bridge, "execute_query", AsyncMock(side_effect=[
        {"commentCreate": {"success": True}},  # Subtask comment
        {"issueUpdate": {"success": True}},    # Subtask update
        tree_resp,                             # Query tree
    ])):
        res = await bridge.settle_subtask_and_evaluate_parent("c-1", "Done")
        assert res["parent_settled"] is False
        assert res["parent_key"] == "CHO-100"


@pytest.mark.asyncio
async def test_approve_and_complete_tree(bridge):
    mock_states = {"completed": "st-done"}
    bridge._project_id_cache = "proj-1"
    bridge._team_id_cache = "team-1"
    bridge._state_cache = mock_states

    tree_resp = {
        "issue": {
            "id": "p-1",
            "children": {
                "nodes": [{"id": "c-1"}, {"id": "c-2"}],
            },
        }
    }

    with patch.object(bridge, "_resolve_issue_id", AsyncMock(return_value="p-1")), \
         patch.object(bridge, "execute_query", AsyncMock(side_effect=[
             tree_resp,
             {"commentCreate": {"success": True}}, {"issueUpdate": {"success": True}},  # Parent
             {"commentCreate": {"success": True}}, {"issueUpdate": {"success": True}},  # Child 1
             {"commentCreate": {"success": True}}, {"issueUpdate": {"success": True}},  # Child 2
         ])):
        success = await bridge.approve_and_complete_tree("CHO-100", "Carl approved")
        assert success is True


def test_headers_validation():
    b_no_key = LinearBridge(api_key="")
    b_no_key.api_key = ""
    with pytest.raises(LinearBridgeError, match="LINEAR_API_KEY is not configured"):
        b_no_key._headers()


def test_credential_resolution_fallback():
    with patch.dict("os.environ", {}, clear=True):
        b = LinearBridge()
        if not b.api_key:
            pytest.skip("No LINEAR_API_KEY available on disk for fallback resolution")
        assert b.api_key.startswith("lin_api_")
        assert b.project_slug == "golem-brain-109337da0931"
        assert b.default_team_key == "CHO"
        headers = b._headers()
        assert headers["Authorization"] == b.api_key


def test_tool_declarations_and_mouth_registration():
    # 1. PLAN_TASK_WITH_SUBTASKS_DECLARATION
    p_params = PLAN_TASK_WITH_SUBTASKS_DECLARATION["parameters"]
    assert "parent_title" in p_params["properties"]
    assert "parent_description" in p_params["properties"]
    assert "subtasks" in p_params["properties"]
    assert p_params["required"] == ["parent_title", "parent_description", "subtasks"]

    # 2. CHECK_LINEAR_TASK_STATUS_DECLARATION
    assert "query" in CHECK_LINEAR_TASK_STATUS_DECLARATION["parameters"]["properties"]

    # 3. APPROVE_LINEAR_TASK_DECLARATION
    a_params = APPROVE_LINEAR_TASK_DECLARATION["parameters"]
    assert "issue_key" in a_params["properties"]
    assert "signoff_comment" in a_params["properties"]
    assert a_params.get("required", []) in ([], ["issue_key"])

    # 4. DISPATCH_TASK_TOOL_DECLARATION contains command
    d_params = DISPATCH_TASK_TOOL_DECLARATION["parameters"]["properties"]
    assert "command" in d_params

    # 5. Registered in MOUTH_TOOLS
    assert CHECK_LINEAR_TASK_STATUS_DECLARATION in MOUTH_TOOLS
    assert APPROVE_LINEAR_TASK_DECLARATION in MOUTH_TOOLS

    # 6. Operational mandate present in system instructions
    assert "OPERATIONAL MANDATE" in BASE_SYSTEM_INSTRUCTION
    assert "BIAS TOWARD ACTION" in BASE_SYSTEM_INSTRUCTION


@pytest.mark.asyncio
async def test_ingress_plan_task_with_subtasks_tool_call():
    gateway = IngressGateway()
    client = MagicMock()
    client.post_chat_message = AsyncMock()
    client.add_reaction = AsyncMock()

    mock_plan = {
        "parent": {"id": "p-1", "identifier": "CHO-100", "url": "https://linear.app/issue/CHO-100", "title": "Build Podcast"},
        "subtasks": [
            {"id": "c-1", "identifier": "CHO-101", "title": "[1/2] Episode 1", "url": "https://linear.app/issue/CHO-101"},
            {"id": "c-2", "identifier": "CHO-102", "title": "[2/2] Episode 2", "url": "https://linear.app/issue/CHO-102"},
        ]
    }

    gemini_turn1 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "plan_task_with_subtasks",
                                "args": {
                                    "parent_title": "Build Podcast",
                                    "parent_description": "Podcast curriculum",
                                    "subtasks": ["Episode 1", "Episode 2"],
                                },
                            }
                        }
                    ]
                }
            }
        ]
    }

    gemini_turn2 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "*Plan Created:* Created parent issue `CHO-100` with 2 subtasks in Planned."
                        }
                    ]
                }
            }
        ]
    }

    with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
         patch("golem.linear_bridge.LinearBridge.create_plan_with_subtasks", new_callable=AsyncMock) as mock_create:
        mock_gemini.side_effect = [gemini_turn1, gemini_turn2]
        mock_create.return_value = mock_plan

        event = {"channel": "C123", "ts": "100.1", "thread_ts": "100.1", "text": "Plan podcast series"}
        await gateway.handle_message(event, client)

        mock_create.assert_called_once_with(
            "Build Podcast",
            "Thread: C123:100.1\nPodcast curriculum",
            ["Episode 1", "Episode 2"]
        )


@pytest.mark.asyncio
async def test_ingress_check_linear_task_status_tool_call():
    gateway = IngressGateway()
    client = MagicMock()
    client.post_chat_message = AsyncMock()
    client.add_reaction = AsyncMock()

    mock_tree = [
        {
            "identifier": "CHO-100",
            "title": "Podcast",
            "state": {"name": "In Progress"},
            "children": {
                "nodes": [
                    {"identifier": "CHO-101", "title": "Ep 1", "state": {"name": "Claimed Fixed"}},
                    {"identifier": "CHO-102", "title": "Ep 2", "state": {"name": "In Progress"}},
                ]
            }
        }
    ]

    gemini_turn1 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "check_linear_task_status",
                                "args": {},
                            }
                        }
                    ]
                }
            }
        ]
    }

    gemini_turn2 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "CHO-100: Ep 1 is Claimed Fixed, Ep 2 is In Progress."}
                    ]
                }
            }
        ]
    }

    with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
         patch("golem.linear_bridge.LinearBridge.get_project_task_tree", new_callable=AsyncMock) as mock_get:
        mock_gemini.side_effect = [gemini_turn1, gemini_turn2]
        mock_get.return_value = mock_tree

        event = {"channel": "C123", "ts": "100.2", "thread_ts": "100.2", "text": "How are the podcast episodes coming along?"}
        await gateway.handle_message(event, client)

        mock_get.assert_called_once()


@pytest.mark.asyncio
async def test_ingress_approve_linear_task_tool_call():
    gateway = IngressGateway()
    client = MagicMock()
    client.post_chat_message = AsyncMock()
    client.add_reaction = AsyncMock()

    gemini_turn1 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "approve_linear_task",
                                "args": {
                                    "issue_key": "CHO-100",
                                    "signoff_comment": "Approved by Carl.",
                                },
                            }
                        }
                    ]
                }
            }
        ]
    }

    gemini_turn2 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "✅ *Completed:* `CHO-100` and all subtasks transitioned to Completed."}
                    ]
                }
            }
        ]
    }

    with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
         patch("golem.linear_bridge.LinearBridge.approve_and_complete_tree", new_callable=AsyncMock) as mock_approve:
        mock_gemini.side_effect = [gemini_turn1, gemini_turn2]
        mock_approve.return_value = True

        event = {"channel": "C123", "ts": "100.3", "thread_ts": "100.3", "text": "looks good, mark it complete"}
        await gateway.handle_message(event, client)

        mock_approve.assert_called_once_with("CHO-100", "Approved by Carl.")


@pytest.mark.asyncio
async def test_backstage_dispatch_binds_subtask_and_settles():
    with patch("golem.linear_bridge.LinearBridge.update_issue_state", new_callable=AsyncMock) as mock_update, \
         patch("golem.linear_bridge.LinearBridge.settle_subtask_and_evaluate_parent", new_callable=AsyncMock) as mock_settle:
        mock_update.return_value = True
        mock_settle.return_value = {"parent_settled": False, "parent_key": "CHO-100"}

        result = await _execute_backstage_task(
            task_description="Render episode 1 audio",
            precedent_status="NOVEL_EXPLORATION",
            precedent_source="First time generating this podcast track",
            target_node="localhost",
            subtask_issue_id="c-1",
            dry_run=True,
        )

        assert result.get("status") == "SUCCESS"
        assert result.get("subtask_issue_id") == "c-1"


@pytest.mark.asyncio
async def test_runner_settle_command_triggers_subtask_settlement():
    executor = RunnerExecutor()
    th = TaskHandle(
        status="completed",
        pane="runner",
        opaque_coordinate="w1:p2",
        command="python3 generate_ep1.py",
        linear_issue_id="c-101",
        stdout="Episode 1 successfully generated.",
    )

    with patch("golem.linear_bridge.LinearBridge.settle_subtask_and_evaluate_parent", new_callable=AsyncMock) as mock_settle:
        mock_settle.return_value = {"parent_settled": True, "parent_key": "CHO-100"}

        await executor.settle_command(pane="runner", returncode=0, task_handle=th)
        # Give asyncio background task a brief tick to run
        await asyncio.sleep(0.05)

        mock_settle.assert_called_once()
        call_args = mock_settle.call_args[1]
        assert call_args["subtask_id"] == "c-101"
        assert "Episode 1 successfully generated." in call_args["settlement_receipt"]
        assert "**Exit Code:** `0`" in call_args["settlement_receipt"]


@pytest.mark.asyncio
async def test_ingress_chains_plan_and_dispatches_subtasks_sequentially():
    """Verify that after plan_task_with_subtasks, Golem chains to dispatch multiple Hand agents in sequence."""
    gateway = IngressGateway()
    client = MagicMock()
    client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "123.456"})
    client.chat_update = AsyncMock(return_value={"ok": True, "ts": "123.456"})
    client.reactions_add = AsyncMock(return_value={"ok": True})
    client.reactions_remove = AsyncMock(return_value={"ok": True})

    mock_plan = {
        "parent": {
            "id": "p-154",
            "identifier": "CHO-154",
            "url": "https://linear.app/issue/CHO-154",
            "title": "Validate audiobookshelf podcasts and episode durations",
        },
        "subtasks": [
            {
                "id": "c-155",
                "identifier": "CHO-155",
                "title": "Hand 1: Validate podcast series",
                "url": "https://linear.app/issue/CHO-155",
            },
            {
                "id": "c-156",
                "identifier": "CHO-156",
                "title": "Hand 2: Extract episode durations",
                "url": "https://linear.app/issue/CHO-156",
            },
        ],
    }

    # Turn 1: Model plans compound work with subtasks
    turn1_plan = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "plan_task_with_subtasks",
                                "args": {
                                    "parent_title": "Validate audiobookshelf podcasts and episode durations",
                                    "parent_description": "Validate podcasts and episode lengths",
                                    "subtasks": [
                                        "Hand 1: Validate podcast series",
                                        "Hand 2: Extract episode durations",
                                    ],
                                },
                            }
                        }
                    ]
                }
            }
        ]
    }

    # Turn 2: Model chains to dispatch Hand 1
    turn2_dispatch_hand1 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "dispatch_task",
                                "args": {
                                    "task_description": "Validate podcast series on Audiobookshelf",
                                    "target_node": "oci-machine",
                                    "subtask_issue_id": "CHO-155",
                                },
                            }
                        }
                    ]
                }
            }
        ]
    }

    # Turn 3: Model chains to dispatch Hand 2
    turn3_dispatch_hand2 = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "dispatch_task",
                                "args": {
                                    "task_description": "Extract episode durations on Audiobookshelf",
                                    "target_node": "oci-machine",
                                    "subtask_issue_id": "CHO-156",
                                },
                            }
                        }
                    ]
                }
            }
        ]
    }

    # Turn 4: Step 2 synthesis generates final briefing
    turn4_synthesis = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": "*Podcast Inventory Report:*\n• Hand 1 (`CHO-155`): Found 3 podcasts (`Huberman Lab`, `Hardcore History`, `Lex Fridman`).\n• Hand 2 (`CHO-156`): 42 episodes verified, average duration 2h 15m."
                        }
                    ]
                }
            }
        ]
    }

    dispatched_tasks = []

    async def mock_backstage(task_desc, target_node, target_cwd, **kwargs):
        sub_id = kwargs.get("subtask_issue_id")
        dispatched_tasks.append({"task_desc": task_desc, "target_node": target_node, "subtask_id": sub_id})
        return {
            "status": "SUCCESS",
            "duration_seconds": 1.2,
            "output_tail": f"Finished {task_desc} for {sub_id}",
            "subtask_issue_id": sub_id,
        }

    with patch("golem.gateway.ingress.call_gemini_api", new_callable=AsyncMock) as mock_gemini, \
         patch("golem.linear_bridge.LinearBridge.create_plan_with_subtasks", new_callable=AsyncMock) as mock_create, \
         patch.object(gateway, "_execute_backstage_task", side_effect=mock_backstage), \
         patch("golem.ingress.set_assistant_status", new_callable=AsyncMock):

        mock_gemini.side_effect = [turn1_plan, turn2_dispatch_hand1, turn3_dispatch_hand2, turn4_synthesis]
        mock_create.return_value = mock_plan

        event = {
            "channel": "C_SLACK",
            "ts": "100.999",
            "thread_ts": "100.999",
            "text": "Can you validate which podcasts are on our audiobookshelf? also the length of each episode we have\nPlease use two separate hand agents, one for each task, and make sure you leverage Linear to track progress",
        }
        await gateway.handle_message(event, client)

        # 1. Verify plan was created
        mock_create.assert_called_once()

        # 2. Verify BOTH hand agents were dispatched backstage
        assert len(dispatched_tasks) == 2
        assert dispatched_tasks[0]["subtask_id"] == "CHO-155"
        assert dispatched_tasks[0]["target_node"] == "oci-machine"
        assert dispatched_tasks[1]["subtask_id"] == "CHO-156"
        assert dispatched_tasks[1]["target_node"] == "oci-machine"

        # 3. Verify executive briefing was posted with both hand outcomes
        posted_calls = client.chat_postMessage.call_args_list
        assert len(posted_calls) >= 1
        combined_posted = " ".join(str(c) for c in posted_calls)
        assert "Podcast Inventory Report" in combined_posted
        assert "CHO-155" in combined_posted
        assert "CHO-156" in combined_posted


def test_resolve_key_from_plan_cache_variations():
    from golem.linear_bridge import resolve_key_from_plan_cache

    plan = {
        "parent": {"id": "p-1", "identifier": "CHO-200", "title": "Main Project"},
        "subtasks": [
            {"id": "c-1", "identifier": "CHO-201", "title": "[1/3] Database Schema"},
            {"id": "c-2", "identifier": "CHO-202", "title": "[2/3] API Endpoints"},
            {"id": "c-3", "identifier": "CHO-203", "title": "[3/3] Frontend Views"},
        ],
    }

    # 1. want_parent with empty key_or_index
    assert resolve_key_from_plan_cache(plan, want_parent=True) == "CHO-200"
    assert resolve_key_from_plan_cache(plan, key_or_index="", want_parent=True) == "CHO-200"
    assert resolve_key_from_plan_cache(plan, key_or_index=None, want_parent=True) == "CHO-200"

    # 2. Integer index resolution
    assert resolve_key_from_plan_cache(plan, key_or_index=1) == "CHO-201"
    assert resolve_key_from_plan_cache(plan, key_or_index="1") == "CHO-201"
    assert resolve_key_from_plan_cache(plan, key_or_index=2) == "CHO-202"
    assert resolve_key_from_plan_cache(plan, key_or_index="3") == "CHO-203"

    # 3. Bracketed index tag resolution
    assert resolve_key_from_plan_cache(plan, key_or_index="[1/3]") == "CHO-201"
    assert resolve_key_from_plan_cache(plan, key_or_index="[2/3]") == "CHO-202"

    # 4. Title prefix and substring matching
    assert resolve_key_from_plan_cache(plan, key_or_index="Database Schema") == "CHO-201"
    assert resolve_key_from_plan_cache(plan, key_or_index="API Endpoints") == "CHO-202"
    assert resolve_key_from_plan_cache(plan, key_or_index="Frontend") == "CHO-203"

    # 5. Direct issue key passthrough
    assert resolve_key_from_plan_cache(plan, key_or_index="CHO-999") == "CHO-999"
    assert resolve_key_from_plan_cache(None, key_or_index="CHO-123") == "CHO-123"

    # 6. Static method on LinearBridge
    assert LinearBridge.resolve_key_from_plan_cache(plan, key_or_index=2) == "CHO-202"


@pytest.mark.asyncio
async def test_linear_bridge_http_session_reuse():
    b = LinearBridge(api_key="lin_api_test")
    session1 = await b._get_http_session()
    session2 = await b._get_http_session()
    assert session1 is session2
    assert not session1.closed

    await b.close()
    assert session1.closed
    assert b._http_session is None
