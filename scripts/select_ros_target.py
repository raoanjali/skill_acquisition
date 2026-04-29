#!/usr/bin/env python3
"""Select and optionally run the best compatible ROS target from the catalog.

The selector is intentionally read-only unless `--run` is passed. Its primary
job is to turn a natural-language request into a target selection by combining:

* platform compatibility checks from `package_catalog.py`,
* SQLite FTS search over target metadata,
* optional OpenAI model reranking over compatible targets, and
* input inference from explicit `--set` values, quoted strings, and optional
  OpenAI-proposed manifest input values.

`skill_acq.py` imports `select_best_target()` directly. This file remains
runnable as a CLI for debugging and machine-readable `--json` output.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from package_catalog import (
    DEFAULT_DB_PATH,
    DEFAULT_GLOBAL_CATALOG_PATH,
    collect_system_profile,
    compatibility_reasons,
    compute_fts_scores,
    ensure_catalog,
    hard_compatibility_reasons,
    load_global_catalog,
    load_all_targets,
    open_catalog,
    robot_compatibility_reasons,
    tokenize,
)
from run_ros_target import CommandError, run_target


DEFAULT_OPENAI_MODEL = "gpt-4o-2024-08-06"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_MIN_CATALOG_SCORE = 0.03


@dataclass
class SelectionOptions:
    """Inputs needed to select a package target without invoking this file as a script."""

    request: str
    db_path: Path | str = DEFAULT_DB_PATH
    global_catalog_path: Path | str = DEFAULT_GLOBAL_CATALOG_PATH
    explicit_values: dict[str, str] = field(default_factory=dict)
    hardware: list[str] = field(default_factory=list)
    robot_type: str = ""
    robot_mode: str = ""
    robot_has_estop: str = "unknown"
    robot_capabilities: list[str] = field(default_factory=list)
    robot_sensors: list[str] = field(default_factory=list)
    robot_actuators: list[str] = field(default_factory=list)
    robot_frames: list[str] = field(default_factory=list)
    top_k: int = 3
    selection_backend: str = "openai"
    openai_model: str = field(
        default_factory=lambda: os.environ.get("SKILL_ACQ_OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
    )
    llm_candidate_limit: int = 10
    min_catalog_score: float = DEFAULT_MIN_CATALOG_SCORE


@dataclass
class SelectionResult:
    """Structured selector output used by both the CLI and `skill_acq.py`."""

    payload: dict[str, Any]
    best: dict[str, Any] | None
    compatible: list[dict[str, Any]]
    incompatible: list[dict[str, Any]]
    inferred_values: dict[str, str]
    missing: list[dict[str, object]]
    system_profile: dict[str, Any]
    llm_selection: dict[str, object] | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter the package catalog by compatibility, rank matching targets "
            "for a natural-language request, and optionally execute the best match."
        )
    )
    parser.add_argument("request", help="Natural-language description of the action to run.")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to the SQLite package catalog database.",
    )
    parser.add_argument(
        "--global-catalog-path",
        default=str(DEFAULT_GLOBAL_CATALOG_PATH),
        help="Path or HTTP(S) URL to the global JSON package catalog used when no local match is found.",
    )
    parser.add_argument(
        "--hardware",
        action="append",
        default=[],
        help="Available hardware capability, such as camera or lidar. Repeat as needed.",
    )
    parser.add_argument(
        "--robot-type",
        default="",
        help="Robot type available on this system, such as turtlebot or ur5.",
    )
    parser.add_argument(
        "--robot-mode",
        default="",
        help="Robot operating mode, such as simulation or physical.",
    )
    parser.add_argument(
        "--robot-has-estop",
        choices=("true", "false", "unknown"),
        default="unknown",
        help="Whether the robot has an available emergency stop.",
    )
    parser.add_argument(
        "--robot-capability",
        action="append",
        default=[],
        help="Available robot capability. Repeat as needed.",
    )
    parser.add_argument(
        "--robot-sensor",
        action="append",
        default=[],
        help="Available robot sensor. Repeat as needed.",
    )
    parser.add_argument(
        "--robot-actuator",
        action="append",
        default=[],
        help="Available robot actuator. Repeat as needed.",
    )
    parser.add_argument(
        "--robot-frame",
        action="append",
        default=[],
        help="Available robot coordinate frame. Repeat as needed.",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Explicit input values to pass to the selected target.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=3,
        help="How many ranked candidates to show.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute the best matching target with run_ros_target.py.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a machine-readable selection result and do not run anything.",
    )
    parser.add_argument(
        "--selection-backend",
        choices=("auto", "catalog", "openai"),
        default="openai",
        help=(
            "Selection backend. Defaults to 'openai'. Use 'catalog' for deterministic "
            "offline ranking, or 'auto' to use OpenAI only when OPENAI_API_KEY is set."
        ),
    )
    parser.add_argument(
        "--openai-model",
        default=os.environ.get("SKILL_ACQ_OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
        help=(
            "OpenAI model used when --selection-backend is 'openai' or auto uses OpenAI. "
            f"Defaults to {DEFAULT_OPENAI_MODEL}."
        ),
    )
    parser.add_argument(
        "--llm-candidate-limit",
        type=int,
        default=10,
        help="Maximum number of compatible catalog candidates to send to the OpenAI model.",
    )
    parser.add_argument(
        "--min-catalog-score",
        type=float,
        default=DEFAULT_MIN_CATALOG_SCORE,
        help=(
            "Minimum deterministic catalog score required before catalog mode accepts "
            "a candidate instead of falling through to the global catalog or no match."
        ),
    )
    return parser.parse_args()


def parse_set_values(items: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Expected NAME=VALUE format for --set, got: {item}")
        name, value = item.split("=", 1)
        name = name.strip()
        if not name:
            raise ValueError(f"Expected NAME=VALUE format for --set, got: {item}")
        values[name] = value
    return values


def build_robot_profile_from_values(
    robot_type: str = "",
    robot_mode: str = "",
    robot_has_estop: str = "unknown",
    capabilities: list[str] | None = None,
    sensors: list[str] | None = None,
    actuators: list[str] | None = None,
    frames: list[str] | None = None,
) -> dict[str, object]:
    """Collect robot facts supplied by the caller for requirement filtering."""

    return {
        "type": robot_type.strip().lower(),
        "mode": robot_mode.strip().lower(),
        "has_estop": (
            True
            if robot_has_estop == "true"
            else False
            if robot_has_estop == "false"
            else "unknown"
        ),
        "capabilities": capabilities or [],
        "sensors": sensors or [],
        "actuators": actuators or [],
        "frames": frames or [],
    }


def build_robot_profile(args: argparse.Namespace) -> dict[str, object]:
    return build_robot_profile_from_values(
        robot_type=args.robot_type,
        robot_mode=args.robot_mode,
        robot_has_estop=args.robot_has_estop,
        capabilities=args.robot_capability,
        sensors=args.robot_sensor,
        actuators=args.robot_actuator,
        frames=args.robot_frame,
    )


def row_value(row, key: str, default=None):
    """Read a value from either sqlite3.Row or a dict-like global catalog row."""

    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    if isinstance(row, dict):
        return row.get(key, default)
    return default


def score_candidates(rows, request: str, system_profile: dict[str, object]):
    request_tokens = set(tokenize(request))
    candidates = []
    compatible_ids = set()

    for row in rows:
        package_platform = json.loads(row["package_platform_requirements_json"])
        target_platform = json.loads(row["target_platform_requirements_json"])
        package_system = json.loads(row["package_system_requirements_json"])
        target_system = json.loads(row["target_system_requirements_json"])
        package_robot = json.loads(row["package_robot_requirements_json"])
        target_robot = json.loads(row["target_robot_requirements_json"])
        package_hard = json.loads(row["package_hard_requirements_json"])
        target_hard = json.loads(row["target_hard_requirements_json"])

        reasons = []
        reasons.extend(compatibility_reasons(package_platform, system_profile))
        reasons.extend(compatibility_reasons(target_platform, system_profile))
        reasons.extend(compatibility_reasons(package_system, system_profile))
        reasons.extend(compatibility_reasons(target_system, system_profile))
        reasons.extend(robot_compatibility_reasons(package_robot, system_profile))
        reasons.extend(robot_compatibility_reasons(target_robot, system_profile))
        reasons.extend(hard_compatibility_reasons(package_hard, system_profile))
        reasons.extend(hard_compatibility_reasons(target_hard, system_profile))
        reasons = list(dict.fromkeys(reasons))

        candidate = {
            "row": row,
            "compatible": not reasons,
            "compatibility_reasons": reasons,
        }
        candidates.append(candidate)
        if not reasons:
            compatible_ids.add(int(row["id"]))

    return candidates, compatible_ids, request_tokens


def apply_simple_ranking(candidates, request: str, request_tokens):
    """Rank non-SQLite candidates, such as entries from the global JSON catalog."""

    for candidate in candidates:
        row = candidate["row"]
        doc_tokens = set(tokenize(row["search_text"]))
        overlap_score = 0.0
        if request_tokens and doc_tokens:
            overlap_score = len(request_tokens & doc_tokens) / len(request_tokens | doc_tokens)

        exact_bonus = exact_request_bonus(row, request)

        candidate["score"] = overlap_score + exact_bonus

    compatible = [candidate for candidate in candidates if candidate["compatible"]]
    compatible.sort(
        key=lambda candidate: (
            candidate["score"],
            candidate["row"]["package_name"],
            candidate["row"]["target_name"],
        ),
        reverse=True,
    )
    incompatible = [candidate for candidate in candidates if not candidate["compatible"]]
    return compatible, incompatible


def apply_ranking(connection, candidates, compatible_ids: set[int], request: str, request_tokens):
    fts_scores = compute_fts_scores(connection, request, compatible_ids)

    for candidate in candidates:
        row = candidate["row"]
        target_id = int(row["id"])
        doc_tokens = set(tokenize(row["search_text"]))
        overlap_score = 0.0
        if request_tokens and doc_tokens:
            overlap_score = len(request_tokens & doc_tokens) / len(request_tokens | doc_tokens)

        exact_bonus = exact_request_bonus(row, request)

        candidate["score"] = (
            0.65 * fts_scores.get(target_id, 0.0)
            + 0.35 * overlap_score
            + exact_bonus
        )

    compatible = [candidate for candidate in candidates if candidate["compatible"]]
    compatible.sort(
        key=lambda candidate: (
            candidate["score"],
            candidate["row"]["package_name"],
            candidate["row"]["target_name"],
        ),
        reverse=True,
    )
    incompatible = [candidate for candidate in candidates if not candidate["compatible"]]
    return compatible, incompatible


def exact_request_bonus(row, request: str) -> float:
    lowered_request = request.lower()
    exact_bonus = 0.0

    target_name = row["target_name"].lower()
    if target_name in lowered_request or target_name.replace("_", " ") in lowered_request:
        exact_bonus += 0.2

    package_name = row["package_name"].lower()
    if package_name in lowered_request or package_name.replace("_", " ") in lowered_request:
        exact_bonus += 0.1

    if any(
        server_name in lowered_request or server_name.replace("_", " ") in lowered_request
        for server_name in extract_action_names(row)
    ):
        exact_bonus += 0.2

    keywords = json.loads(row["keywords_json"])
    if any(
        isinstance(keyword, str)
        and (" " in keyword or "_" in keyword)
        and keyword.lower() in lowered_request
        for keyword in keywords
    ):
        exact_bonus += 0.2

    return exact_bonus


def extract_action_names(row) -> list[str]:
    action_servers = json.loads(row["action_servers_json"])
    names: list[str] = []
    for action_server in action_servers:
        if isinstance(action_server, dict):
            name = action_server.get("name")
            if isinstance(name, str):
                names.append(name.lower())
    return names


def input_requires_user_value(requirement: dict[str, object]) -> bool:
    """Return true when a manifest input should not silently fall back to a default."""

    semantic_role = str(requirement.get("semantic_role", "")).strip().lower()
    name = str(requirement.get("name", "")).strip().lower()
    return (
        bool(requirement.get("required", False))
        or bool(requirement.get("requires_user_value", False))
        or semantic_role == "output_topic"
        or name in {"publish_topic", "output_topic"}
    )


def infer_missing_inputs(request: str, row, explicit_values: dict[str, str]) -> dict[str, str]:
    values = dict(explicit_values)
    input_requirements = json.loads(row["input_requirements_json"])

    quoted_matches = re.findall(r'"([^"]+)"|\'([^\']+)\'', request)
    quoted_values = [first or second for first, second in quoted_matches if first or second]
    required_string_inputs = [
        requirement
        for requirement in input_requirements
        if isinstance(requirement, dict)
        and input_requires_user_value(requirement)
        and requirement.get("type") == "string"
    ]
    inferred_required_name = None
    if len(required_string_inputs) == 1:
        name = required_string_inputs[0].get("name")
        if isinstance(name, str):
            inferred_required_name = name

    for requirement in input_requirements:
        if not isinstance(requirement, dict):
            continue
        name = requirement.get("name")
        if not isinstance(name, str) or name in values:
            continue
        if not input_requires_user_value(requirement):
            continue
        semantic_role = str(requirement.get("semantic_role", "")).strip().lower()
        if (
            requirement.get("type") == "string"
            and semantic_role in {"text_to_transform", "input_text", "text"}
            and quoted_values
        ):
            values[name] = quoted_values[0]
            continue
        if requirement.get("type") == "string" and name == inferred_required_name and quoted_values:
            values[name] = quoted_values[0]

    return values


def missing_required_inputs(row, values: dict[str, str]) -> list[dict[str, object]]:
    input_requirements = json.loads(row["input_requirements_json"])
    missing = []
    for requirement in input_requirements:
        if not isinstance(requirement, dict):
            continue
        name = requirement.get("name")
        if not isinstance(name, str):
            continue
        if input_requires_user_value(requirement) and name not in values:
            missing.append(requirement)
    return missing


def apply_llm_input_values(
    row,
    values: dict[str, str],
    explicit_values: dict[str, str],
    llm_selection: dict[str, object] | None,
) -> dict[str, str]:
    """Merge model-proposed input values without overriding explicit user values."""

    if not llm_selection:
        return values
    raw_items = llm_selection.get("input_values", [])
    if not isinstance(raw_items, list):
        return values

    allowed_names = {
        requirement.get("name")
        for requirement in json.loads(row["input_requirements_json"])
        if isinstance(requirement, dict) and isinstance(requirement.get("name"), str)
    }
    merged = dict(values)
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if not isinstance(name, str) or name not in allowed_names or name in explicit_values:
            continue
        if value is None:
            continue
        merged[name] = str(value)
    return merged


def print_candidate(candidate, rank: int) -> None:
    row = candidate["row"]
    action_servers = json.loads(row["action_servers_json"])
    action_names = []
    for action_server in action_servers:
        if isinstance(action_server, dict) and isinstance(action_server.get("name"), str):
            action_names.append(action_server["name"])

    print(f"{rank}. {row['package_name']}::{row['target_name']}  score={candidate['score']:.3f}")
    print(f"   summary: {row['summary']}")
    if action_names:
        print(f"   action servers: {', '.join(action_names)}")
    print(f"   repo: {row['repo_url']}")


def candidate_for_llm(candidate, index: int) -> dict[str, object]:
    """Convert a compatible candidate into compact model-readable metadata."""
    row = candidate["row"]
    return {
        "candidate_id": index,
        "catalog_score": candidate["score"],
        "package_name": row["package_name"],
        "target_name": row["target_name"],
        "summary": row["summary"],
        "description": row["description"],
        "language_representation": row["language_representation"],
        "action_servers": json.loads(row["action_servers_json"]),
        "input_requirements": json.loads(row["input_requirements_json"]),
        "keywords": json.loads(row["keywords_json"]),
        "examples": json.loads(row["examples_json"]),
        "platform_requirements": {
            "package": json.loads(row["package_platform_requirements_json"]),
            "target": json.loads(row["target_platform_requirements_json"]),
        },
        "system_requirements": {
            "package": json.loads(row["package_system_requirements_json"]),
            "target": json.loads(row["target_system_requirements_json"]),
        },
        "robot_requirements": {
            "package": json.loads(row["package_robot_requirements_json"]),
            "target": json.loads(row["target_robot_requirements_json"]),
        },
        "hard_requirements": {
            "package": json.loads(row["package_hard_requirements_json"]),
            "target": json.loads(row["target_hard_requirements_json"]),
        },
    }


def should_use_openai_backend(selection_backend: str) -> bool:
    """Return whether this invocation should call OpenAI for final selection."""
    has_key = bool(os.environ.get("OPENAI_API_KEY"))
    if selection_backend == "openai" and not has_key:
        raise ValueError("OPENAI_API_KEY must be set when --selection-backend openai is used.")
    return selection_backend == "openai" or (selection_backend == "auto" and has_key)


def best_catalog_candidate(
    compatible: list[dict[str, object]],
    min_score: float,
) -> dict[str, object] | None:
    if not compatible:
        return None
    best = compatible[0]
    if float(best.get("score", 0.0)) < min_score:
        return None
    return best


def choose_with_openai(
    request: str,
    compatible: list[dict[str, object]],
    model: str,
    candidate_limit: int,
) -> tuple[dict[str, object] | None, dict[str, object]]:
    """Ask an OpenAI model to pick the best compatible package target.

    The model receives only catalog candidates that already passed local
    compatibility checks. Its response is constrained to a small JSON object, and
    the selected package/target/action server is validated against the candidate
    list before being trusted.
    """

    candidates_for_model = [
        candidate_for_llm(candidate, index)
        for index, candidate in enumerate(compatible[: max(candidate_limit, 1)])
    ]
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You select the best ROS package target for a user's request. "
                    "Choose only from the provided compatible candidates. If none of "
                    "the candidates can satisfy the request, return selected=false "
                    "with empty package_name, target_name, and action_server_name. "
                    "When selected=true, also provide any manifest input values you "
                    "can infer unambiguously from the user's request. Do not invent "
                    "values and do not include inputs that are not in the selected "
                    "candidate's input_requirements. "
                    "Return strict JSON only."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "request": request,
                        "compatible_candidates": candidates_for_model,
                    },
                    indent=2,
                    sort_keys=True,
                ),
            },
        ],
        "temperature": 0,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "skill_selection",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "selected": {"type": "boolean"},
                        "package_name": {"type": "string"},
                        "target_name": {"type": "string"},
                        "action_server_name": {"type": "string"},
                        "input_values": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "value": {"type": "string"},
                                },
                                "required": ["name", "value"],
                                "additionalProperties": False,
                            },
                        },
                        "confidence": {"type": "number"},
                        "rationale": {"type": "string"},
                    },
                    "required": [
                        "selected",
                        "package_name",
                        "target_name",
                        "action_server_name",
                        "input_values",
                        "confidence",
                        "rationale",
                    ],
                    "additionalProperties": False,
                },
            },
        },
    }

    request_data = json.dumps(payload).encode("utf-8")
    request_obj = urllib.request.Request(
        OPENAI_CHAT_COMPLETIONS_URL,
        data=request_data,
        headers={
            "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request_obj, timeout=60) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI selection request failed: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI selection request failed: {exc.reason}") from exc

    message = response_payload["choices"][0]["message"]
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("OpenAI selection response did not include JSON content.")
    selection = json.loads(content)
    if not selection.get("selected"):
        return None, selection

    selected_package = selection.get("package_name")
    selected_target = selection.get("target_name")
    selected_action = selection.get("action_server_name")
    for candidate in compatible[: max(candidate_limit, 1)]:
        row = candidate["row"]
        action_names = [
            action_server.get("name")
            for action_server in json.loads(row["action_servers_json"])
            if isinstance(action_server, dict)
        ]
        if (
            row["package_name"] == selected_package
            and row["target_name"] == selected_target
            and selected_action in action_names
        ):
            return candidate, selection

    raise RuntimeError(
        "OpenAI selected a package, target, or action server that was not in the "
        "compatible candidate list."
    )


def run_selected_target(row, values: dict[str, str]) -> int:
    local_path = str(row_value(row, "package_local_path", "") or "")
    source = "local" if local_path and Path(local_path).exists() else "cloud"
    print("[selector] Running selected target through the runner helper")
    try:
        result = run_target(
            source=source,
            repo_url=row["repo_url"] if source == "cloud" else None,
            repo_ref=row_value(row, "repo_ref", "") or None,
            package_path=local_path if source == "local" else None,
            target=row["target_name"],
            values=values,
        )
    except (CommandError, FileNotFoundError, ValueError) as exc:
        print(f"[selector error] {exc}", file=sys.stderr)
        return 1

    client_result = result.get("client_result", {})
    if client_result.get("stdout"):
        print("[client stdout]")
        print(client_result["stdout"])
    if client_result.get("stderr"):
        print("[client stderr]")
        print(client_result["stderr"])
    return 0


def selection_payload(
    candidate,
    values: dict[str, str],
    missing: list[dict[str, object]],
    system_profile: dict[str, object],
    selection_backend: str = "catalog",
    llm_selection: dict[str, object] | None = None,
) -> dict[str, object]:
    row = candidate["row"]
    action_servers = json.loads(row["action_servers_json"])
    return {
        "found": True,
        "score": candidate["score"],
        "selection_backend": selection_backend,
        "llm_selection": llm_selection,
        "system_profile": system_profile,
        "package_name": row["package_name"],
        "target_name": row["target_name"],
        "catalog_source": row_value(row, "catalog_source", "local"),
        "repo_url": row["repo_url"],
        "repo_ref": row_value(row, "repo_ref", ""),
        "source": json.loads(row_value(row, "source_json", "{}") or "{}"),
        "local_path": row["package_local_path"],
        "manifest_path": row["package_manifest_path"],
        "summary": row["summary"],
        "action_servers": action_servers,
        "selected_action_server": (
            llm_selection.get("action_server_name")
            if llm_selection
            else action_servers[0].get("name")
            if action_servers and isinstance(action_servers[0], dict)
            else ""
        ),
        "input_requirements": json.loads(row["input_requirements_json"]),
        "robot_requirements": {
            "package": json.loads(row["package_robot_requirements_json"]),
            "target": json.loads(row["target_robot_requirements_json"]),
        },
        "hard_requirements": {
            "package": json.loads(row["package_hard_requirements_json"]),
            "target": json.loads(row["target_hard_requirements_json"]),
        },
        "values": values,
        "missing_required_inputs": missing,
    }


def select_best_target(options: SelectionOptions) -> SelectionResult:
    """Select the best package target without shelling out to this script.

    The function is side-effect free except for optional OpenAI/global-catalog
    network calls. It does not install packages or start ROS processes; callers
    should pass the returned payload to the runner layer.
    """

    if options.selection_backend not in {"auto", "catalog", "openai"}:
        raise ValueError(
            "selection_backend must be one of 'auto', 'catalog', or 'openai', "
            f"got {options.selection_backend!r}."
        )

    system_profile = collect_system_profile(extra_hardware=options.hardware)
    system_profile["robot"] = build_robot_profile_from_values(
        robot_type=options.robot_type,
        robot_mode=options.robot_mode,
        robot_has_estop=options.robot_has_estop,
        capabilities=options.robot_capabilities,
        sensors=options.robot_sensors,
        actuators=options.robot_actuators,
        frames=options.robot_frames,
    )

    db_path = Path(options.db_path).resolve()
    ensure_catalog(db_path)
    compatible: list[dict[str, Any]] = []
    incompatible: list[dict[str, Any]] = []
    if db_path.exists():
        connection = open_catalog(db_path)
        try:
            rows = load_all_targets(connection)
            if rows:
                candidates, compatible_ids, request_tokens = score_candidates(
                    rows,
                    options.request,
                    system_profile,
                )
                compatible, incompatible = apply_ranking(
                    connection,
                    candidates,
                    compatible_ids,
                    options.request,
                    request_tokens,
                )
        finally:
            connection.close()

    local_compatible = compatible
    local_incompatible = incompatible
    global_compatible: list[dict[str, object]] = []
    global_incompatible: list[dict[str, object]] = []

    def load_ranked_global_candidates() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        global_rows = load_global_catalog(options.global_catalog_path)
        if not global_rows:
            return [], []
        global_candidates, _, global_request_tokens = score_candidates(
            global_rows,
            options.request,
            system_profile,
        )
        return apply_simple_ranking(
            global_candidates,
            options.request,
            global_request_tokens,
        )

    selection_backend = "catalog"
    llm_selection = None
    best = None
    used_global_catalog = False
    openai_required = options.selection_backend == "openai"

    try:
        if should_use_openai_backend(options.selection_backend):
            selection_backend = "openai-local"
            if local_compatible:
                best, llm_selection = choose_with_openai(
                    request=options.request,
                    compatible=local_compatible,
                    model=options.openai_model,
                    candidate_limit=options.llm_candidate_limit,
                )
            if best is None:
                global_compatible, global_incompatible = load_ranked_global_candidates()
                if global_compatible:
                    selection_backend = "openai-global"
                    used_global_catalog = True
                    best, llm_selection = choose_with_openai(
                        request=options.request,
                        compatible=global_compatible,
                        model=options.openai_model,
                        candidate_limit=options.llm_candidate_limit,
                    )
        else:
            best = best_catalog_candidate(local_compatible, options.min_catalog_score)
            if best is not None:
                compatible = local_compatible
                incompatible = local_incompatible
            else:
                global_compatible, global_incompatible = load_ranked_global_candidates()
                best = best_catalog_candidate(global_compatible, options.min_catalog_score)
                if best is not None:
                    compatible = global_compatible
                    incompatible = global_incompatible
                    used_global_catalog = True
    except (RuntimeError, ValueError) as exc:
        if openai_required:
            raise
        print(
            f"[selector] OpenAI selection failed; falling back to catalog ranking: {exc}",
            file=sys.stderr,
        )
        best = best_catalog_candidate(local_compatible, options.min_catalog_score)
        if best is not None:
            compatible = local_compatible
            incompatible = local_incompatible
            selection_backend = "catalog"
        else:
            global_compatible, global_incompatible = load_ranked_global_candidates()
            best = best_catalog_candidate(global_compatible, options.min_catalog_score)
            if best is not None:
                compatible = global_compatible
                incompatible = global_incompatible
                selection_backend = "catalog"
                used_global_catalog = True

    if best is not None:
        if used_global_catalog:
            compatible = global_compatible
            incompatible = global_incompatible
        else:
            compatible = local_compatible
            incompatible = local_incompatible

    if best is None:
        combined_incompatible = local_incompatible + global_incompatible
        any_below_threshold = bool(local_compatible or global_compatible)
        payload = {
            "found": False,
            "system_profile": system_profile,
            "reason": (
                f"no catalog candidate scored at least {options.min_catalog_score}"
                if any_below_threshold and not should_use_openai_backend(options.selection_backend)
                else "no compatible targets"
            ),
            "incompatible": [
                {
                    "package_name": candidate["row"]["package_name"],
                    "target_name": candidate["row"]["target_name"],
                    "compatibility_reasons": candidate["compatibility_reasons"],
                }
                for candidate in combined_incompatible[: options.top_k]
            ],
        }
        return SelectionResult(
            payload=payload,
            best=None,
            compatible=[],
            incompatible=combined_incompatible,
            inferred_values={},
            missing=[],
            system_profile=system_profile,
            llm_selection=llm_selection,
        )

    best_row = best["row"]
    inferred_values = infer_missing_inputs(options.request, best_row, options.explicit_values)
    inferred_values = apply_llm_input_values(
        best_row,
        inferred_values,
        options.explicit_values,
        llm_selection,
    )
    missing = missing_required_inputs(best_row, inferred_values)
    payload = selection_payload(
        best,
        inferred_values,
        missing,
        system_profile,
        selection_backend=selection_backend,
        llm_selection=llm_selection,
    )

    return SelectionResult(
        payload=payload,
        best=best,
        compatible=compatible,
        incompatible=incompatible,
        inferred_values=inferred_values,
        missing=missing,
        system_profile=system_profile,
        llm_selection=llm_selection,
    )


def main() -> int:
    try:
        args = parse_args()
        explicit_values = parse_set_values(args.set)
        options = SelectionOptions(
            request=args.request,
            db_path=args.db_path,
            global_catalog_path=args.global_catalog_path,
            explicit_values=explicit_values,
            hardware=args.hardware,
            robot_type=args.robot_type,
            robot_mode=args.robot_mode,
            robot_has_estop=args.robot_has_estop,
            robot_capabilities=args.robot_capability,
            robot_sensors=args.robot_sensor,
            robot_actuators=args.robot_actuator,
            robot_frames=args.robot_frame,
            top_k=args.top_k,
            selection_backend=args.selection_backend,
            openai_model=args.openai_model,
            llm_candidate_limit=args.llm_candidate_limit,
            min_catalog_score=args.min_catalog_score,
        )
        result = select_best_target(options)
    except (RuntimeError, ValueError) as exc:
        print(f"[selector error] {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result.payload, indent=2, sort_keys=True))
        if not result.payload.get("found"):
            return 1
        return 0 if not result.missing else 2

    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        print(
            f"[selector] Local catalog database not found at {db_path}; "
            "checking global catalog.",
            file=sys.stderr,
        )

    print("[system profile]")
    print(json.dumps(result.system_profile, indent=2, sort_keys=True))

    if not result.payload.get("found"):
        print("[selector] No compatible targets were found.")
        if result.incompatible:
            print("[selector] Incompatible candidates:")
            for candidate in result.incompatible[: args.top_k]:
                row = candidate["row"]
                print(
                    f"- {row['package_name']}::{row['target_name']}: "
                    + "; ".join(candidate["compatibility_reasons"])
                )
        return 1

    best = result.best
    if best is None:
        print("[selector] No compatible targets were found.")
        return 1

    inferred_values = result.inferred_values
    missing = result.missing

    print("[selector] Top compatible matches:")
    for index, candidate in enumerate(result.compatible[: args.top_k], start=1):
        print_candidate(candidate, index)

    print("[selector] Best match:")
    print_candidate(best, 1)
    if result.llm_selection:
        print(
            "[selector] OpenAI rationale: "
            f"{result.llm_selection.get('rationale', '')}"
        )
    if inferred_values:
        print("[selector] Provided or inferred inputs:")
        for name, value in sorted(inferred_values.items()):
            print(f"- {name}={value}")

    if missing:
        print("[selector] Missing required inputs:")
        for requirement in missing:
            example = requirement.get("example", "")
            suffix = f" Example: {example}" if example else ""
            print(f"- {requirement['name']}: {requirement.get('description', '')}{suffix}")
        if args.run:
            print("[selector] Refusing to run until all required inputs are provided.", file=sys.stderr)
            return 1
        return 0

    if not args.run:
        print("[selector] Dry run only. Re-run with --run to execute the selected target.")
        return 0

    return run_selected_target(best["row"], inferred_values)


if __name__ == "__main__":
    raise SystemExit(main())
