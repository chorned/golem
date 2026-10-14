#!/usr/bin/env python3
"""
golem/linear_bridge.py
Linear GraphQL Integration for Multi-Stage Task Planning,
Subtask Hierarchies, and Human Sign-Off Verification Gate.

Golem is the open-source evolution of First Mate, powered by Herdr's distributed PTY mesh.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import aiohttp

logger = logging.getLogger("golem.linear")
LINEAR_GRAPHQL_ENDPOINT = "https://api.linear.app/graphql"

# Per-thread locks so concurrent dispatches in one Slack thread don't both create the thread's issue.
_THREAD_ISSUE_LOCKS: Dict[str, asyncio.Lock] = {}


class LinearBridgeError(Exception):
    """Raised on Linear API connection, GraphQL, or entity resolution errors."""
    pass


class LinearBridge:
    """
    Bridge to Linear for hierarchical project task management.
    Scaffolds parent deliverables and child subtasks under Golem_Brain,
    tracks multi-hand settlements, and enforces human signoff invariant.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        project_slug: str = "golem-brain-109337da0931",
        default_team_key: str = "CHO",
    ) -> None:
        resolved_key = api_key if api_key is not None else os.getenv("LINEAR_API_KEY", "")
        resolved_slug = project_slug or os.getenv("LINEAR_PROJECT_SLUG", "golem-brain-109337da0931")
        resolved_team = default_team_key or os.getenv("LINEAR_DEFAULT_TEAM_KEY", "CHO")

        # Fallback inspection of ~/.golem/config.env or .env if not provided in environment
        if not resolved_key and api_key is None:
            candidate_files = [
                Path(os.path.expanduser("~/.golem/config.env")),
                Path(__file__).resolve().parent.parent / ".env",
            ]
            for candidate in candidate_files:
                if candidate.exists():
                    try:
                        for line in candidate.read_text(encoding="utf-8").splitlines():
                            line = line.strip()
                            if line.startswith("LINEAR_API_KEY="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val:
                                    resolved_key = val
                            elif line.startswith("LINEAR_PROJECT_SLUG="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val and not project_slug:
                                    resolved_slug = val
                            elif line.startswith("LINEAR_DEFAULT_TEAM_KEY="):
                                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                                if val and not default_team_key:
                                    resolved_team = val
                    except Exception as exc:
                        logger.debug(f"Unable to read {candidate} for Linear credentials: {exc}")
                if resolved_key:
                    break

        self.api_key: str = resolved_key or ""
        self.project_slug: str = resolved_slug
        self.default_team_key: str = resolved_team
        self._project_id_cache: Optional[str] = None
        self._team_id_cache: Optional[str] = None
        self._state_cache: Dict[str, str] = {}
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._session_loop: Optional[asyncio.AbstractEventLoop] = None

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise LinearBridgeError("LINEAR_API_KEY is not configured in environment.")
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Loop-aware persistent HTTP session helper reusing self._http_session with total timeout 15s."""
        current_loop = asyncio.get_running_loop()
        if (
            self._http_session is not None
            and not self._http_session.closed
            and self._session_loop is current_loop
        ):
            return self._http_session

        if self._http_session is not None and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass

        timeout = aiohttp.ClientTimeout(total=15)
        self._http_session = aiohttp.ClientSession(timeout=timeout)
        self._session_loop = current_loop
        return self._http_session

    async def close(self) -> None:
        """Close persistent HTTP session if open."""
        if self._http_session is not None and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception:
                pass
            self._http_session = None
            self._session_loop = None

    async def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        session = await self._get_http_session()
        payload = {"query": query, "variables": variables or {}}
        async with session.post(LINEAR_GRAPHQL_ENDPOINT, headers=self._headers(), json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise LinearBridgeError(f"HTTP {resp.status} from Linear API: {body}")
            data = await resp.json()
            if "errors" in data and data["errors"]:
                raise LinearBridgeError(f"Linear GraphQL error: {data['errors']}")
            return data.get("data", {})

    async def resolve_project_and_states(self) -> Tuple[str, str, Dict[str, str]]:
        if self._project_id_cache and self._team_id_cache and self._state_cache:
            return self._project_id_cache, self._team_id_cache, self._state_cache

        query = """
        query GetProjectMetadata($slug: String!) {
            project(id: $slug) {
                id
                name
                teams {
                    nodes {
                        id
                        key
                        name
                        states {
                            nodes {
                                id
                                name
                                type
                            }
                        }
                    }
                }
            }
        }
        """
        data = await self.execute_query(query, {"slug": self.project_slug})
        proj = data.get("project")

        if not proj:
            list_query = "query ListProjects { projects(first: 50) { nodes { id name slugId } } }"
            proj_data = await self.execute_query(list_query)
            for p in proj_data.get("projects", {}).get("nodes", []):
                if "golem" in p.get("name", "").lower() or (p.get("slugId") and p["slugId"] in self.project_slug):
                    proj = p
                    break

        if not proj:
            raise LinearBridgeError(f"Could not locate Linear project '{self.project_slug}'.")

        project_id = proj["id"]
        teams = proj.get("teams", {}).get("nodes", [])

        if not teams:
            team_query = "query GetTeams { teams { nodes { id key states { nodes { id name type } } } } }"
            t_data = await self.execute_query(team_query)
            teams = t_data.get("teams", {}).get("nodes", [])

        selected_team = next((t for t in teams if t["key"].upper() == self.default_team_key.upper()), None)
        if not selected_team and teams:
            selected_team = teams[0]

        if not selected_team:
            raise LinearBridgeError(f"Could not locate Linear team '{self.default_team_key}'.")

        team_id = selected_team["id"]
        state_map: Dict[str, str] = {}
        for s in selected_team.get("states", {}).get("nodes", []):
            name = s.get("name", "").strip().lower()
            stype = s.get("type", "").strip().lower()
            if name:
                state_map[name] = s["id"]
            if stype:
                state_map[stype] = s["id"]

        self._project_id_cache = project_id
        self._team_id_cache = team_id
        self._state_cache = state_map
        return project_id, team_id, state_map

    async def create_plan_with_subtasks(
        self,
        parent_title: str,
        parent_description: str,
        subtasks: List[str],
    ) -> Dict[str, Any]:
        """
        Creates a parent issue in 'In Progress' and child subtasks in 'Planned'.
        """
        project_id, team_id, states = await self.resolve_project_and_states()
        in_progress_state_id = states.get("in progress") or states.get("started")
        planned_state_id = states.get("planned") or states.get("unstarted")

        # 1. Create Parent Issue
        parent_mutation = """
        mutation CreateParent($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier url title }
            }
        }
        """
        parent_vars = {
            "input": {
                "teamId": team_id,
                "projectId": project_id,
                "title": parent_title[:255],
                "description": parent_description,
                "stateId": in_progress_state_id,
            }
        }
        p_res = await self.execute_query(parent_mutation, parent_vars)
        parent_issue = p_res.get("issueCreate", {}).get("issue", {})
        parent_id = parent_issue.get("id")

        # 2. Create Child Subtasks concurrently via asyncio.gather
        child_mutation = """
        mutation CreateSubtask($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier title url }
            }
        }
        """

        async def _create_child(index: int, subtask_title: str) -> Dict[str, Any]:
            child_vars = {
                "input": {
                    "teamId": team_id,
                    "projectId": project_id,
                    "parentId": parent_id,
                    "title": f"[{index}/{len(subtasks)}] {subtask_title[:220]}",
                    "stateId": planned_state_id,
                }
            }
            c_res = await self.execute_query(child_mutation, child_vars)
            c_issue = c_res.get("issueCreate", {}).get("issue", {})
            return {
                "id": c_issue.get("id"),
                "identifier": c_issue.get("identifier"),
                "title": c_issue.get("title"),
                "url": c_issue.get("url"),
            }

        child_coros = [_create_child(index, subtask_title) for index, subtask_title in enumerate(subtasks, start=1)]
        created_subtasks = list(await asyncio.gather(*child_coros)) if child_coros else []

        return {
            "parent": {
                "id": parent_issue.get("id"),
                "identifier": parent_issue.get("identifier"),
                "url": parent_issue.get("url"),
                "title": parent_issue.get("title"),
            },
            "subtasks": created_subtasks,
        }

    async def create_task_issue(
        self,
        title: str,
        description: str = "",
        parent_id: Optional[str] = None,
        state_key: str = "in progress",
    ) -> Dict[str, Any]:
        """
        Create a single tracking issue under the Golem project (optionally as a child of `parent_id`).
        Returns {id, identifier, url, title}.
        """
        project_id, team_id, states = await self.resolve_project_and_states()
        state_id = states.get(state_key.strip().lower()) or states.get("started")
        mutation = """
        mutation CreateTask($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue { id identifier url title }
            }
        }
        """
        issue_input: Dict[str, Any] = {
            "teamId": team_id,
            "projectId": project_id,
            "title": title[:255],
            "description": description,
        }
        if state_id:
            issue_input["stateId"] = state_id
        if parent_id:
            issue_input["parentId"] = parent_id
        res = await self.execute_query(mutation, {"input": issue_input})
        return res.get("issueCreate", {}).get("issue", {}) or {}

    async def add_comment(self, issue_key_or_id: str, body: str) -> bool:
        """Append a comment to an issue (accepts a CHO-123 key or a UUID)."""
        issue_id = await self._resolve_issue_id(issue_key_or_id)
        mutation = """
        mutation AddComment($input: CommentCreateInput!) {
            commentCreate(input: $input) { success }
        }
        """
        res = await self.execute_query(mutation, {"input": {"issueId": issue_id, "body": body}})
        return bool(res.get("commentCreate", {}).get("success"))

    async def find_thread_issue(self, thread_ref: str) -> Optional[Dict[str, Any]]:
        """
        Find the most recently updated top-level issue tagged with `thread_ref`
        (the `Thread: <ref>` line Golem writes into issue descriptions). None if the thread has no issue yet.
        """
        if not thread_ref:
            return None
        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query FindThreadIssue($projectId: String!, $ref: String!) {
            project(id: $projectId) {
                issues(first: 1, orderBy: updatedAt, filter: {
                    parent: { null: true },
                    description: { contains: $ref }
                }) {
                    nodes { id identifier title url state { name type } }
                }
            }
        }
        """
        data = await self.execute_query(query, {"projectId": project_id, "ref": f"Thread: {thread_ref}"})
        nodes = data.get("project", {}).get("issues", {}).get("nodes", [])
        return nodes[0] if nodes else None

    async def get_thread_notes(self, thread_ref: str, max_comments: int = 6) -> Optional[Dict[str, Any]]:
        """
        Return the thread's Linear issue with its subtask states and the latest comments, so a new turn
        can restore what earlier turns already discovered. None if the thread has no Linear issue.
        """
        issue = await self.find_thread_issue(thread_ref)
        if not issue:
            return None
        query = """
        query ThreadNotes($id: String!, $last: Int!) {
            issue(id: $id) {
                identifier
                title
                state { name }
                children { nodes { identifier title state { name } } }
                comments(last: $last) { nodes { body createdAt } }
            }
        }
        """
        data = await self.execute_query(query, {"id": issue["id"], "last": max(1, int(max_comments))})
        full = data.get("issue") or {}
        return {
            "identifier": full.get("identifier") or issue.get("identifier"),
            "title": full.get("title") or issue.get("title"),
            "state": (full.get("state") or issue.get("state") or {}).get("name", "Unknown"),
            "subtasks": [
                f"[{c.get('identifier', '')}] {c.get('title', '')} -> {c.get('state', {}).get('name', 'Unknown')}"
                for c in full.get("children", {}).get("nodes", [])
            ],
            "comments": [
                {"body": c.get("body", ""), "created_at": c.get("createdAt", "")}
                for c in full.get("comments", {}).get("nodes", [])
            ],
        }

    async def ensure_thread_issue(self, thread_ref: str, title: str, description: str = "") -> Dict[str, Any]:
        """Get the thread's tracking issue, creating it (tagged with `Thread: <ref>`) if none exists yet."""
        lock = _THREAD_ISSUE_LOCKS.setdefault(thread_ref, asyncio.Lock())
        async with lock:
            existing = await self.find_thread_issue(thread_ref)
            if existing:
                return existing
            body = f"Thread: {thread_ref}\n{description}".strip()
            return await self.create_task_issue(title, body)

    async def update_issue_state(self, issue_id: str, state_key: str) -> bool:
        _, _, states = await self.resolve_project_and_states()
        key_lower = state_key.strip().lower()
        target_state_id = states.get(key_lower)
        if not target_state_id:
            # Fallback to standard Linear state types
            if key_lower == "claimed fixed":
                target_state_id = states.get("in review")
            elif key_lower == "in progress":
                target_state_id = states.get("started")
            elif key_lower == "completed":
                target_state_id = states.get("done")
            elif key_lower == "planned":
                target_state_id = states.get("unstarted")

        if not target_state_id:
            raise LinearBridgeError(f"State '{state_key}' not found in team states.")

        mutation = """
        mutation UpdateState($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) { success }
        }
        """
        await self.execute_query(mutation, {"id": issue_id, "input": {"stateId": target_state_id}})
        return True

    async def settle_subtask_and_evaluate_parent(
        self,
        subtask_id: str,
        settlement_receipt: str,
    ) -> Dict[str, Any]:
        """
        Transitions a subtask to 'Claimed Fixed', checks sibling subtasks,
        and transitions the parent to 'Claimed Fixed' if all subtasks are settled.
        """
        # 1. Comment receipt on subtask
        comment_mutation = """
        mutation AddReceipt($input: CommentCreateInput!) {
            commentCreate(input: $input) { success }
        }
        """
        await self.execute_query(comment_mutation, {"input": {"issueId": subtask_id, "body": settlement_receipt}})

        # 2. Transition subtask to Claimed Fixed
        await self.update_issue_state(subtask_id, "claimed fixed")

        # 3. Fetch Parent and Sibling Subtask States
        query = """
        query InspectParentTree($id: String!) {
            issue(id: $id) {
                parent {
                    id
                    identifier
                    title
                    children {
                        nodes {
                            id
                            identifier
                            state { name type }
                        }
                    }
                }
            }
        }
        """
        data = await self.execute_query(query, {"id": subtask_id})
        parent = data.get("issue", {}).get("parent")

        if not parent:
            return {"parent_settled": False, "parent_id": None}

        children = parent.get("children", {}).get("nodes", [])
        all_settled = all(
            c.get("state", {}).get("name", "").strip().lower() in ["claimed fixed", "completed", "canceled"]
            for c in children
        )

        if all_settled:
            parent_receipt = (
                f"### All Subtasks Settled ({len(children)}/{len(children)})\n"
                f"Every planned milestone in this deliverable has reached `Claimed Fixed`.\n"
                f":bell: *Awaiting Carl's verification to transition to Completed.*"
            )
            await self.execute_query(comment_mutation, {"input": {"issueId": parent["id"], "body": parent_receipt}})
            await self.update_issue_state(parent["id"], "claimed fixed")
            return {
                "parent_settled": True,
                "parent_key": parent["identifier"],
                "parent_title": parent["title"],
            }

        return {"parent_settled": False, "parent_key": parent["identifier"]}

    async def approve_and_complete_tree(
        self,
        issue_key_or_id: str,
        signoff_comment: str = "Verified and approved by Carl.",
    ) -> bool:
        """
        Transitions parent issue and all subtasks to 'Completed' upon human approval.
        """
        issue_id = await self._resolve_issue_id(issue_key_or_id)

        # Query issue and children
        query = """
        query GetTree($id: String!) {
            issue(id: $id) {
                id
                children { nodes { id } }
            }
        }
        """
        data = await self.execute_query(query, {"id": issue_id})
        target_ids = [issue_id]
        if data.get("issue", {}).get("children", {}).get("nodes"):
            target_ids.extend([c["id"] for c in data["issue"]["children"]["nodes"]])

        # Mark all completed
        for target in target_ids:
            comment_mutation = """
            mutation Signoff($input: CommentCreateInput!) {
                commentCreate(input: $input) { success }
            }
            """
            await self.execute_query(comment_mutation, {"input": {"issueId": target, "body": signoff_comment}})
            await self.update_issue_state(target, "completed")

        return True

    async def get_project_task_tree(self, limit: int = 10) -> List[Dict[str, Any]]:
        project_id, _, _ = await self.resolve_project_and_states()
        query = """
        query GetTree($projectId: String!, $first: Int!) {
            project(id: $projectId) {
                issues(first: $first, filter: { parent: { null: true } }, orderBy: updatedAt) {
                    nodes {
                        id
                        identifier
                        title
                        state { name type }
                        children {
                            nodes {
                                id
                                identifier
                                title
                                state { name type }
                            }
                        }
                    }
                }
            }
        }
        """
        data = await self.execute_query(query, {"projectId": project_id, "first": limit})
        return data.get("project", {}).get("issues", {}).get("nodes", [])

    async def _resolve_issue_id(self, key_or_id: str) -> str:
        if "-" not in key_or_id:
            return key_or_id
        query = "query GetIssue($key: String!) { issue(id: $key) { id } }"
        data = await self.execute_query(query, {"key": key_or_id.upper()})
        issue = data.get("issue")
        if not issue:
            raise LinearBridgeError(f"Issue '{key_or_id}' not found.")
        return issue["id"]

    @staticmethod
    def resolve_key_from_plan_cache(
        plan_cache: Optional[Dict[str, Any]],
        key_or_index: Any = None,
        want_parent: bool = False,
    ) -> str:
        return resolve_key_from_plan_cache(plan_cache=plan_cache, key_or_index=key_or_index, want_parent=want_parent)


def resolve_key_from_plan_cache(
    plan_cache: Optional[Dict[str, Any]],
    key_or_index: Any = None,
    want_parent: bool = False,
) -> str:
    """
    Resolves a Linear issue key from a plan cache dictionary:
      - Returns parent_key / parent.identifier when want_parent=True and key_or_index is empty.
      - Resolves subtask identifier (e.g. CHO-211) when key_or_index is:
          * a 1-based integer index (1, "1", "[1/3]")
          * a subtask title prefix or substring match
          * an existing Linear key (CHO-xxx)
    """
    raw_str = str(key_or_index).strip() if key_or_index is not None else ""

    # Case 1: want_parent with empty key_or_index
    if want_parent and not raw_str:
        if plan_cache:
            parent = plan_cache.get("parent")
            if isinstance(parent, dict) and parent.get("identifier"):
                return str(parent["identifier"])
            if plan_cache.get("parent_key"):
                return str(plan_cache["parent_key"])
        return ""

    # Case 2: key_or_index already has standard Linear issue key format (e.g. CHO-123)
    if re.match(r"^[A-Za-z]+-\d+$", raw_str):
        return raw_str.upper()

    subtasks = []
    if plan_cache:
        st = plan_cache.get("subtasks")
        if isinstance(st, list):
            subtasks = st

    # Case 3: 1-based integer index, e.g. 1, "1", "[1/3]", "1/3"
    idx_match = re.match(r"^\[?(\d+)(?:/\d+)?\]?$", raw_str)
    if idx_match and subtasks:
        try:
            idx = int(idx_match.group(1))
            if 1 <= idx <= len(subtasks):
                child = subtasks[idx - 1]
                if isinstance(child, dict) and child.get("identifier"):
                    return str(child["identifier"])
        except (ValueError, IndexError):
            pass

    # Case 4: matching subtask title prefix or substring
    if raw_str and subtasks:
        query = raw_str.lower()
        for child in subtasks:
            if isinstance(child, dict):
                ctitle = str(child.get("title", "")).lower()
                clean_title = re.sub(r"^\[\d+/\d+\]\s*", "", ctitle)
                if ctitle.startswith(query) or clean_title.startswith(query):
                    if child.get("identifier"):
                        return str(child["identifier"])

        for child in subtasks:
            if isinstance(child, dict):
                ctitle = str(child.get("title", "")).lower()
                if query in ctitle and child.get("identifier"):
                    return str(child["identifier"])

    # Fallback: if want_parent and nothing resolved, return parent if available
    if want_parent and plan_cache:
        parent = plan_cache.get("parent")
        if isinstance(parent, dict) and parent.get("identifier"):
            return str(parent["identifier"])
        if plan_cache.get("parent_key"):
            return str(plan_cache["parent_key"])

    return raw_str


__all__ = [
    "LinearBridge",
    "LinearBridgeError",
    "LINEAR_GRAPHQL_ENDPOINT",
    "resolve_key_from_plan_cache",
]
