#!/usr/bin/env python3
"""Select a cataloged ROS skill and run it through the runner helper.

This script is the public entry point for the `skill_acq` ROS package. It keeps
the orchestration deliberately small:

1. Pass the user's natural-language request to the selector helper.
2. Read the selector payload to get package, target, and input values.
3. Call the runner helper to stage/build/start/call the selected target.

The lower-level modules still provide their own CLI wrappers for debugging, but
the main pipeline uses normal Python calls instead of shelling out to sibling
scripts.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from package_catalog import DEFAULT_DB_PATH, DEFAULT_GLOBAL_CATALOG_PATH
from run_ros_target import DEFAULT_WORKSPACE_DIR, CommandError, run_target
from select_ros_target import (
    DEFAULT_MIN_CATALOG_SCORE,
    SelectionOptions,
    parse_set_values,
    select_best_target,
)


def resolve_catalog_location(value: str) -> str:
    """Return an absolute local path or preserve an HTTP(S) catalog URL."""
    if value.startswith(("http://", "https://")):
        return value
    return str(Path(value).resolve())


def infer_catalog_roots(db_path: str) -> list[Path]:
    """Infer source workspace roots only for the known source-tree catalog layout."""

    resolved_db_path = Path(db_path).expanduser().resolve()
    if (
        resolved_db_path.name == "package_catalog.db"
        and resolved_db_path.parent.name == "skill_acq"
        and "install" not in resolved_db_path.parts
    ):
        return [resolved_db_path.parent.parent]
    return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Select a skill from package_catalog.db using natural language, then run it "
            "with the runner helper."
        )
    )
    parser.add_argument(
        "request",
        help='Natural-language skill request, for example: reverse the string "hello"',
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to the local package catalog database.",
    )
    parser.add_argument(
        "--global-catalog-path",
        default=str(DEFAULT_GLOBAL_CATALOG_PATH),
        help="Path or HTTP(S) URL to the global package catalog queried when no local package matches.",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Explicit input value for the selected target. Repeat for multiple values.",
    )
    parser.add_argument(
        "--hardware",
        action="append",
        default=[],
        help="Available hardware capability to pass to the selector helper.",
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
        help="Available robot capability. Repeat for multiple values.",
    )
    parser.add_argument(
        "--robot-sensor",
        action="append",
        default=[],
        help="Available robot sensor. Repeat for multiple values.",
    )
    parser.add_argument(
        "--robot-actuator",
        action="append",
        default=[],
        help="Available robot actuator. Repeat for multiple values.",
    )
    parser.add_argument(
        "--robot-frame",
        action="append",
        default=[],
        help="Available robot coordinate frame. Repeat for multiple values.",
    )
    parser.add_argument(
        "--workspace-dir",
        default=str(DEFAULT_WORKSPACE_DIR),
        help="Directory to use as the ROS runner workspace.",
    )
    parser.add_argument(
        "--ros-setup",
        default=None,
        help="Path to the ROS 2 underlay setup.bash. Auto-detected if omitted.",
    )
    parser.add_argument(
        "--leave-processes-running",
        action="store_true",
        help="Ask the runner helper to leave target background processes running.",
    )
    parser.add_argument(
        "--selection-backend",
        choices=("auto", "catalog", "openai"),
        default="openai",
        help=(
            "How the selector helper should choose among compatible candidates. "
            "Defaults to OpenAI. Use 'catalog' for deterministic offline ranking."
        ),
    )
    parser.add_argument(
        "--openai-model",
        default=None,
        help=(
            "OpenAI model for selector reranking. Defaults to select_ros_target.py's "
            "GPT-4-class model setting."
        ),
    )
    parser.add_argument(
        "--min-catalog-score",
        type=float,
        default=DEFAULT_MIN_CATALOG_SCORE,
        help=(
            "Minimum deterministic catalog score required before catalog mode accepts "
            "a local or global candidate."
        ),
    )
    return parser.parse_args()


def select_target(args: argparse.Namespace) -> dict[str, object]:
    """Ask the selector helper for the best catalog target."""

    options_kwargs = {
        "request": args.request,
        "db_path": Path(args.db_path).resolve(),
        "global_catalog_path": resolve_catalog_location(args.global_catalog_path),
        "explicit_values": parse_set_values(args.set),
        "hardware": args.hardware,
        "robot_type": args.robot_type,
        "robot_mode": args.robot_mode,
        "robot_has_estop": args.robot_has_estop,
        "robot_capabilities": args.robot_capability,
        "robot_sensors": args.robot_sensor,
        "robot_actuators": args.robot_actuator,
        "robot_frames": args.robot_frame,
        "top_k": 1,
        "selection_backend": args.selection_backend,
        "min_catalog_score": args.min_catalog_score,
    }
    if args.openai_model:
        options_kwargs["openai_model"] = args.openai_model

    result = select_best_target(SelectionOptions(**options_kwargs))
    return result.payload


def run_selected_target(
    selection: dict[str, object],
    args: argparse.Namespace,
) -> dict[str, object]:
    """Install or reuse the selected package, then run its target."""

    local_path_text = str(selection.get("local_path") or "")
    local_path = Path(local_path_text) if local_path_text else None
    source = "local" if local_path is not None and local_path.exists() else "cloud"

    values_raw = selection.get("values", {})
    if not isinstance(values_raw, dict):
        raise ValueError("Selector payload field 'values' must be an object.")
    values = {str(name): str(value) for name, value in values_raw.items()}

    workspace_dir = Path(args.workspace_dir).expanduser().resolve()
    if source == "local":
        print(f"[skill_acq] Using local package at {local_path}")
    else:
        print(f"[skill_acq] Downloading package from {selection['repo_url']}")
        if selection.get("repo_ref"):
            print(f"[skill_acq] Requested package ref: {selection['repo_ref']}")
    print(f"[skill_acq] Installing or reusing package in runner workspace {workspace_dir}")
    print(f"[skill_acq] Starting target '{selection['target_name']}'")
    return run_target(
        source=source,
        workspace_dir=workspace_dir,
        ros_setup=Path(args.ros_setup).resolve() if args.ros_setup else None,
        repo_url=str(selection["repo_url"]) if source == "cloud" else None,
        repo_ref=str(selection.get("repo_ref") or "") or None,
        package_path=local_path if source == "local" else None,
        target=str(selection["target_name"]),
        values=values,
        leave_processes_running=args.leave_processes_running,
        catalog_roots=infer_catalog_roots(args.db_path),
    )


def main() -> int:
    try:
        args = parse_args()
        print(f"[skill_acq] Looking for package locally in {Path(args.db_path).resolve()}")
        print(
            "[skill_acq] If no local action satisfies the request, "
            f"checking global catalog {resolve_catalog_location(args.global_catalog_path)}"
        )
        selection = select_target(args)
        if not selection.get("found"):
            reason = selection.get("reason", "selection failed")
            raise RuntimeError(f"No compatible package target was found: {reason}")

        catalog_source = selection.get("catalog_source", "local")
        if catalog_source == "local":
            print("[skill_acq] Found a matching local package target")
        else:
            print("[skill_acq] Found a matching global package target")

        missing = selection.get("missing_required_inputs", [])
        if missing:
            print("[skill_acq] The selected target needs more input:", file=sys.stderr)
            for requirement in missing:
                if isinstance(requirement, dict):
                    name = requirement.get("name", "<unknown>")
                    description = requirement.get("description", "")
                    print(f"- {name}: {description}", file=sys.stderr)
            return 1

        print(
            "[skill_acq] Selected "
            f"{selection['package_name']}::{selection['target_name']} "
            f"via {selection.get('selection_backend', 'catalog')}"
        )
        if selection.get("selected_action_server"):
            print(f"[skill_acq] Selected action server: {selection['selected_action_server']}")
        run_result = run_selected_target(selection, args)
        client_result = run_result.get("client_result", {})
        if isinstance(client_result, dict):
            if client_result.get("stdout"):
                print("[client stdout]")
                print(client_result["stdout"])
            if client_result.get("stderr"):
                print("[client stderr]")
                print(client_result["stderr"])
        return 0
    except (CommandError, FileNotFoundError, RuntimeError, ValueError, KeyError) as exc:
        print(f"[skill_acq error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
