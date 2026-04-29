#!/usr/bin/env python3
"""Helpers for indexing and selecting ROS package targets.

This module is intentionally shared by all skill acquisition entry points:

* `build_package_catalog.py` uses it to scan `package_runner.json` files and
  rebuild the SQLite catalog.
* `select_ros_target.py` uses it to filter and rank compatible targets.
* `run_ros_target.py` uses it to refresh the catalog after a cloud package is
  installed.

The database is packaged next to the ROS package metadata at
`skill_acq/package_catalog.db`, while this helper module lives in
`skill_acq/scripts/`. Keep `DEFAULT_DB_PATH` anchored to the package root so the
same default works when running from source and after `colcon build` installs
the scripts into `install/skill_acq/lib/skill_acq`.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import sqlite3
import urllib.request
from pathlib import Path
from typing import Any


def default_db_path() -> Path:
    """Return the packaged catalog path for source and installed layouts.

    Source layout:
      skill_acq/scripts/package_catalog.py
      skill_acq/package_catalog.db

    Installed layout:
      install/skill_acq/lib/skill_acq/package_catalog.py
      install/skill_acq/share/skill_acq/package_catalog.db
    """

    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir.parent / "package_catalog.db",
        script_dir / "package_catalog.db",
    ]
    if len(script_dir.parents) >= 2:
        candidates.append(script_dir.parents[1] / "share" / "skill_acq" / "package_catalog.db")

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


DEFAULT_DB_PATH = default_db_path()
DEFAULT_GLOBAL_CATALOG_URL = "https://raw.githubusercontent.com/Nikkhil16/Demo/main/global_package_catalog.json"
DEFAULT_GLOBAL_CATALOG_PATH = os.environ.get(
    "SKILL_ACQ_GLOBAL_CATALOG_URL",
    DEFAULT_GLOBAL_CATALOG_URL,
)
MANIFEST_NAME = "package_runner.json"
DISCOVERY_SCHEMA_VERSIONS = {3}
GLOBAL_CATALOG_SCHEMA_VERSIONS = {1}
SKIP_PARTS = {
    "__pycache__",
    ".git",
    "build",
    "install",
    "log",
    "ros_runner_ws",
    "ros_agent_ws",
}

CREATE_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS target_fts;
DROP TABLE IF EXISTS targets;
DROP TABLE IF EXISTS packages;

CREATE TABLE packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_name TEXT NOT NULL UNIQUE,
    repo_url TEXT NOT NULL,
    local_path TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    summary TEXT NOT NULL,
    language_representation TEXT NOT NULL,
    keywords_json TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    platform_requirements_json TEXT NOT NULL,
    system_requirements_json TEXT NOT NULL,
    robot_requirements_json TEXT NOT NULL,
    hard_requirements_json TEXT NOT NULL,
    raw_manifest_json TEXT NOT NULL
);

CREATE TABLE targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id INTEGER NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    package_name TEXT NOT NULL,
    target_name TEXT NOT NULL,
    repo_url TEXT NOT NULL,
    summary TEXT NOT NULL,
    description TEXT NOT NULL,
    language_representation TEXT NOT NULL,
    action_servers_json TEXT NOT NULL,
    input_requirements_json TEXT NOT NULL,
    client_arguments_json TEXT NOT NULL,
    keywords_json TEXT NOT NULL,
    examples_json TEXT NOT NULL,
    package_platform_requirements_json TEXT NOT NULL,
    target_platform_requirements_json TEXT NOT NULL,
    package_system_requirements_json TEXT NOT NULL,
    target_system_requirements_json TEXT NOT NULL,
    package_robot_requirements_json TEXT NOT NULL,
    target_robot_requirements_json TEXT NOT NULL,
    package_hard_requirements_json TEXT NOT NULL,
    target_hard_requirements_json TEXT NOT NULL,
    search_text TEXT NOT NULL,
    UNIQUE(package_name, target_name)
);

CREATE VIRTUAL TABLE target_fts USING fts5(
    target_id UNINDEXED,
    package_name,
    target_name,
    package_summary,
    target_summary,
    description,
    language_representation,
    keywords,
    action_servers,
    inputs,
    examples,
    tokenize='porter unicode61'
);
"""


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def normalize_os_name(value: str) -> str:
    normalized = value.strip().lower()
    if normalized.startswith("darwin"):
        return "macos"
    if normalized.startswith("win"):
        return "windows"
    if normalized.startswith("linux"):
        return "linux"
    return normalized


def normalize_architecture(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "aarch64": "arm64",
    }
    return aliases.get(normalized, normalized)


def collect_system_profile(extra_hardware: list[str] | None = None) -> dict[str, Any]:
    system_os = normalize_os_name(platform.system())
    architecture = normalize_architecture(platform.machine())
    platform_tags = {system_os}

    os_release = parse_os_release()
    if os_release:
        os_id = os_release.get("ID", "").strip().lower()
        version_id = os_release.get("VERSION_ID", "").strip().lower()
        if os_id:
            platform_tags.add(os_id)
        if os_id and version_id:
            platform_tags.add(f"{os_id}-{version_id}")

    available_commands = sorted(
        command
        for command in ["git", "python3", "pip3", "colcon", "ros2", "rosdep"]
        if shutil.which(command) is not None
    )
    hardware = sorted({item.strip().lower() for item in extra_hardware or [] if item.strip()})
    memory_mb = 0
    try:
        memory_mb = int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024 * 1024))
    except (AttributeError, OSError, ValueError):
        pass
    disk_free_mb = int(shutil.disk_usage(Path.cwd()).free / (1024 * 1024))

    return {
        "os": system_os,
        "architecture": architecture,
        "platform_tags": sorted(platform_tags),
        "ros_distro": os.environ.get("ROS_DISTRO", "").strip().lower(),
        "available_commands": available_commands,
        "hardware": hardware,
        "has_gpu": shutil.which("nvidia-smi") is not None or Path("/dev/dri").exists(),
        "memory_mb": memory_mb,
        "disk_free_mb": disk_free_mb,
        "network_access": os.environ.get("SKILL_ACQ_NETWORK_ACCESS", "unknown").strip().lower(),
        "ros": {
            "nodes": parse_env_list("SKILL_ACQ_ROS_NODES"),
            "topics": parse_env_list("SKILL_ACQ_ROS_TOPICS"),
            "services": parse_env_list("SKILL_ACQ_ROS_SERVICES"),
            "actions": parse_env_list("SKILL_ACQ_ROS_ACTIONS"),
        },
        "robot": {},
    }


def parse_env_list(name: str) -> list[str]:
    raw_value = os.environ.get(name, "")
    return [
        item.strip()
        for item in raw_value.split(",")
        if item.strip()
    ]


def parse_os_release() -> dict[str, str]:
    path = Path("/etc/os-release")
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.startswith("#"):
            continue
        key, raw_value = line.split("=", 1)
        values[key] = raw_value.strip().strip('"')
    return values


def find_manifest_files(roots: list[Path]) -> list[Path]:
    manifests: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        for manifest in root.rglob(MANIFEST_NAME):
            if any(part in SKIP_PARTS for part in manifest.parts):
                continue
            resolved = manifest.resolve()
            if resolved not in seen:
                manifests.append(resolved)
                seen.add(resolved)
    return sorted(manifests)


def build_catalog(manifest_paths: list[Path], db_path: Path) -> tuple[int, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.executescript(CREATE_SCHEMA_SQL)
        package_count = 0
        target_count = 0
        for manifest_path in manifest_paths:
            package_record, target_records = extract_manifest_records(manifest_path)
            package_id = insert_package(connection, package_record)
            package_count += 1
            for target_record in target_records:
                insert_target(connection, package_id, target_record)
                target_count += 1
        connection.commit()
        return package_count, target_count
    finally:
        connection.close()


def ensure_catalog(db_path: Path) -> None:
    """Create an empty catalog database when one does not already exist."""

    if db_path.exists():
        return
    build_catalog([], db_path)


def open_catalog(db_path: Path) -> sqlite3.Connection:
    ensure_catalog(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def extract_manifest_records(manifest_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema_version = manifest.get("schema_version")
    if schema_version not in DISCOVERY_SCHEMA_VERSIONS:
        raise ValueError(
            f"{manifest_path}: discovery tooling expects schema_version in "
            f"{sorted(DISCOVERY_SCHEMA_VERSIONS)}, got {schema_version!r}."
        )

    package_name = require_string(manifest, "package_name", manifest_path)
    repo_url = require_string(manifest, "repo_url", manifest_path)
    discovery = require_object(manifest, "discovery", manifest_path)
    targets = require_object(manifest, "targets", manifest_path)

    package_summary = require_string(discovery, "summary", manifest_path)
    package_language = require_string(discovery, "language_representation", manifest_path)
    package_keywords = require_list(discovery, "keywords", manifest_path)
    capabilities = require_list(discovery, "capabilities", manifest_path)
    package_platform_requirements = require_object(
        discovery, "platform_requirements", manifest_path
    )
    package_system_requirements = require_object(
        discovery, "system_requirements", manifest_path
    )
    package_robot_requirements = optional_object(discovery, "robot_requirements")
    package_hard_requirements = optional_object(discovery, "hard_requirements")

    package_record = {
        "package_name": package_name,
        "repo_url": repo_url,
        "local_path": str(manifest_path.parent.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "schema_version": schema_version,
        "summary": package_summary,
        "language_representation": package_language,
        "keywords": package_keywords,
        "capabilities": capabilities,
        "platform_requirements": package_platform_requirements,
        "system_requirements": package_system_requirements,
        "robot_requirements": package_robot_requirements,
        "hard_requirements": package_hard_requirements,
        "raw_manifest": manifest,
    }

    target_records: list[dict[str, Any]] = []
    for target_name, target in sorted(targets.items()):
        if not isinstance(target, dict):
            raise ValueError(f"{manifest_path}: target '{target_name}' must be an object.")

        summary = require_string(target, "summary", manifest_path)
        description = require_string(target, "description", manifest_path)
        language_representation = require_string(
            target, "language_representation", manifest_path
        )
        keywords = require_list(target, "keywords", manifest_path)
        examples = require_list(target, "examples", manifest_path)
        action_servers = require_list(target, "action_servers", manifest_path)
        target_platform_requirements = require_object(
            target, "platform_requirements", manifest_path
        )
        target_system_requirements = require_object(
            target, "system_requirements", manifest_path
        )
        target_robot_requirements = optional_object(target, "robot_requirements")
        target_hard_requirements = optional_object(target, "hard_requirements")
        input_requirements = require_list(target, "input_requirements", manifest_path)
        client = require_object(target, "client", manifest_path)
        client_arguments = client.get("arguments", [])
        if not isinstance(client_arguments, list):
            raise ValueError(
                f"{manifest_path}: target '{target_name}' client.arguments must be a list."
            )

        search_text = build_search_text(
            package_summary=package_summary,
            package_language=package_language,
            package_keywords=package_keywords,
            capabilities=capabilities,
            target_name=target_name,
            target_summary=summary,
            target_description=description,
            target_language=language_representation,
            keywords=keywords,
            action_servers=action_servers,
            input_requirements=input_requirements,
            package_robot_requirements=package_robot_requirements,
            target_robot_requirements=target_robot_requirements,
            package_hard_requirements=package_hard_requirements,
            target_hard_requirements=target_hard_requirements,
            examples=examples,
        )

        target_records.append(
            {
                "package_name": package_name,
                "target_name": target_name,
                "repo_url": repo_url,
                "package_summary": package_summary,
                "summary": summary,
                "description": description,
                "language_representation": language_representation,
                "keywords": keywords,
                "examples": examples,
                "action_servers": action_servers,
                "input_requirements": input_requirements,
                "client_arguments": client_arguments,
                "package_platform_requirements": package_platform_requirements,
                "target_platform_requirements": target_platform_requirements,
                "package_system_requirements": package_system_requirements,
                "target_system_requirements": target_system_requirements,
                "package_robot_requirements": package_robot_requirements,
                "target_robot_requirements": target_robot_requirements,
                "package_hard_requirements": package_hard_requirements,
                "target_hard_requirements": target_hard_requirements,
                "search_text": search_text,
            }
        )

    return package_record, target_records


def build_search_text(**parts: Any) -> str:
    chunks: list[str] = []
    for value in parts.values():
        chunks.extend(flatten_strings(value))
    return " ".join(chunk for chunk in chunks if chunk).strip()


def flatten_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        items: list[str] = []
        for nested in value.values():
            items.extend(flatten_strings(nested))
        return items
    if isinstance(value, list):
        items: list[str] = []
        for nested in value:
            items.extend(flatten_strings(nested))
        return items
    return []


def insert_package(connection: sqlite3.Connection, record: dict[str, Any]) -> int:
    cursor = connection.execute(
        """
        INSERT INTO packages (
            package_name,
            repo_url,
            local_path,
            manifest_path,
            schema_version,
            summary,
            language_representation,
            keywords_json,
            capabilities_json,
            platform_requirements_json,
            system_requirements_json,
            robot_requirements_json,
            hard_requirements_json,
            raw_manifest_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["package_name"],
            record["repo_url"],
            record["local_path"],
            record["manifest_path"],
            record["schema_version"],
            record["summary"],
            record["language_representation"],
            json_dumps(record["keywords"]),
            json_dumps(record["capabilities"]),
            json_dumps(record["platform_requirements"]),
            json_dumps(record["system_requirements"]),
            json_dumps(record["robot_requirements"]),
            json_dumps(record["hard_requirements"]),
            json_dumps(record["raw_manifest"]),
        ),
    )
    return int(cursor.lastrowid)


def insert_target(
    connection: sqlite3.Connection,
    package_id: int,
    record: dict[str, Any],
) -> None:
    cursor = connection.execute(
        """
        INSERT INTO targets (
            package_id,
            package_name,
            target_name,
            repo_url,
            summary,
            description,
            language_representation,
            action_servers_json,
            input_requirements_json,
            client_arguments_json,
            keywords_json,
            examples_json,
            package_platform_requirements_json,
            target_platform_requirements_json,
            package_system_requirements_json,
            target_system_requirements_json,
            package_robot_requirements_json,
            target_robot_requirements_json,
            package_hard_requirements_json,
            target_hard_requirements_json,
            search_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            package_id,
            record["package_name"],
            record["target_name"],
            record["repo_url"],
            record["summary"],
            record["description"],
            record["language_representation"],
            json_dumps(record["action_servers"]),
            json_dumps(record["input_requirements"]),
            json_dumps(record["client_arguments"]),
            json_dumps(record["keywords"]),
            json_dumps(record["examples"]),
            json_dumps(record["package_platform_requirements"]),
            json_dumps(record["target_platform_requirements"]),
            json_dumps(record["package_system_requirements"]),
            json_dumps(record["target_system_requirements"]),
            json_dumps(record["package_robot_requirements"]),
            json_dumps(record["target_robot_requirements"]),
            json_dumps(record["package_hard_requirements"]),
            json_dumps(record["target_hard_requirements"]),
            record["search_text"],
        ),
    )
    target_id = int(cursor.lastrowid)
    connection.execute(
        """
        INSERT INTO target_fts (
            target_id,
            package_name,
            target_name,
            package_summary,
            target_summary,
            description,
            language_representation,
            keywords,
            action_servers,
            inputs,
            examples
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target_id,
            record["package_name"],
            record["target_name"],
            record["package_summary"],
            record["summary"],
            record["description"],
            record["language_representation"],
            " ".join(flatten_strings(record["keywords"])),
            " ".join(flatten_strings(record["action_servers"])),
            " ".join(flatten_strings(record["input_requirements"])),
            " ".join(flatten_strings(record["examples"])),
        ),
    )


def require_string(obj: dict[str, Any], key: str, manifest_path: Path) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{manifest_path}: field '{key}' must be a non-empty string.")
    return value


def require_object(obj: dict[str, Any], key: str, manifest_path: Path) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{manifest_path}: field '{key}' must be an object.")
    return value


def optional_object(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key, {})
    if isinstance(value, dict):
        return value
    raise ValueError(f"Optional field '{key}' must be an object when provided.")


def require_list(obj: dict[str, Any], key: str, manifest_path: Path) -> list[Any]:
    value = obj.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{manifest_path}: field '{key}' must be a list.")
    return value


def load_all_targets(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        connection.execute(
            """
            SELECT
                targets.*,
                packages.summary AS package_summary,
                packages.language_representation AS package_language_representation,
                packages.keywords_json AS package_keywords_json,
                packages.capabilities_json AS package_capabilities_json,
                packages.local_path AS package_local_path,
                packages.manifest_path AS package_manifest_path,
                packages.robot_requirements_json AS package_robot_requirements_json,
                packages.hard_requirements_json AS package_hard_requirements_json
            FROM targets
            JOIN packages ON packages.id = targets.package_id
            ORDER BY targets.package_name, targets.target_name
            """
        )
    )


def compute_fts_scores(
    connection: sqlite3.Connection,
    query: str,
    allowed_target_ids: set[int],
) -> dict[int, float]:
    tokens = tokenize(query)
    if not tokens or not allowed_target_ids:
        return {}

    match_query = " OR ".join(tokens)
    rows = connection.execute(
        """
        SELECT target_id, bm25(target_fts, 0.0, 0.8, 0.8, 0.7, 1.3, 1.1, 1.2, 0.8, 0.8, 0.6, 0.6) AS score
        FROM target_fts
        WHERE target_fts MATCH ?
        """,
        (match_query,),
    ).fetchall()

    result: dict[int, float] = {}
    for row in rows:
        target_id = int(row["target_id"])
        if target_id not in allowed_target_ids:
            continue
        raw_score = float(row["score"])
        result[target_id] = 1.0 / (1.0 + max(raw_score, 0.0))
    return result


def compatibility_reasons(
    requirements: dict[str, Any],
    system_profile: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []

    allowed_os = [normalize_os_name(value) for value in requirements.get("os", [])]
    if allowed_os and system_profile["os"] not in allowed_os:
        reasons.append(f"os must be one of {allowed_os}")

    allowed_architectures = [
        normalize_architecture(value) for value in requirements.get("architectures", [])
    ]
    if allowed_architectures and system_profile["architecture"] not in allowed_architectures:
        reasons.append(f"architecture must be one of {allowed_architectures}")

    allowed_platform_tags = {
        value.strip().lower() for value in requirements.get("platform_tags", [])
    }
    if allowed_platform_tags and not (
        allowed_platform_tags & set(system_profile["platform_tags"])
    ):
        reasons.append(f"platform tags must include one of {sorted(allowed_platform_tags)}")

    allowed_ros_distros = {
        value.strip().lower() for value in requirements.get("ros_distros", [])
    }
    if allowed_ros_distros and system_profile["ros_distro"] not in allowed_ros_distros:
        reasons.append(f"ros distro must be one of {sorted(allowed_ros_distros)}")

    required_commands = {
        command.strip() for command in requirements.get("commands", []) if command.strip()
    }
    missing_commands = sorted(required_commands - set(system_profile["available_commands"]))
    if missing_commands:
        reasons.append(f"missing commands: {missing_commands}")

    required_hardware = {
        item.strip().lower() for item in requirements.get("hardware", []) if item.strip()
    }
    missing_hardware = sorted(required_hardware - set(system_profile["hardware"]))
    if missing_hardware:
        reasons.append(f"missing hardware: {missing_hardware}")

    if requirements.get("requires_gpu") and not system_profile["has_gpu"]:
        reasons.append("requires a GPU")

    return reasons


def robot_compatibility_reasons(
    requirements: dict[str, Any],
    system_profile: dict[str, Any],
) -> list[str]:
    """Return unmet robot requirements for a package or target.

    Robot requirements are optional and intentionally generic at this stage. A
    registry entry can describe required robot type, capabilities, sensors,
    actuators, frames, or free-form notes. Only structured list fields are used
    for hard filtering; notes are informational.
    """

    robot_profile = system_profile.get("robot", {})
    if not isinstance(robot_profile, dict):
        robot_profile = {}

    reasons: list[str] = []
    robot_type = str(robot_profile.get("type", "")).strip().lower()
    allowed_robot_types = {
        value.strip().lower() for value in requirements.get("robot_types", []) if value.strip()
    }
    if allowed_robot_types and robot_type not in allowed_robot_types:
        reasons.append(f"robot type must be one of {sorted(allowed_robot_types)}")

    for field in ["capabilities", "sensors", "actuators", "frames"]:
        required_values = {
            value.strip().lower() for value in requirements.get(field, []) if value.strip()
        }
        available_values = {
            value.strip().lower()
            for value in robot_profile.get(field, [])
            if isinstance(value, str) and value.strip()
        }
        missing_values = sorted(required_values - available_values)
        if missing_values:
            reasons.append(f"missing robot {field}: {missing_values}")

    return reasons


def hard_compatibility_reasons(
    requirements: dict[str, Any],
    system_profile: dict[str, Any],
) -> list[str]:
    """Return unmet generic hard constraints.

    These checks intentionally cover only objective constraints that can be
    validated from the local profile or explicit caller-provided robot facts.
    Descriptive fields such as notes or risk labels are preserved in the catalog
    for LLM selection and user review, but they are not used for hard filtering.
    """

    reasons: list[str] = []
    resource = requirements.get("resource_requirements", {})
    if isinstance(resource, dict):
        min_memory_mb = int(resource.get("min_memory_mb", 0) or 0)
        if min_memory_mb and system_profile.get("memory_mb", 0) < min_memory_mb:
            reasons.append(f"requires at least {min_memory_mb} MB memory")

        min_disk_free_mb = int(resource.get("min_disk_free_mb", 0) or 0)
        if min_disk_free_mb and system_profile.get("disk_free_mb", 0) < min_disk_free_mb:
            reasons.append(f"requires at least {min_disk_free_mb} MB free disk")

        required_accelerators = {
            str(item).strip().lower()
            for item in resource.get("accelerators", [])
            if str(item).strip()
        }
        if "gpu" in required_accelerators and not system_profile.get("has_gpu", False):
            reasons.append("requires accelerator: gpu")

    runtime = requirements.get("runtime_requirements", {})
    if isinstance(runtime, dict):
        required_env = {
            str(item).strip() for item in runtime.get("required_env", []) if str(item).strip()
        }
        missing_env = sorted(name for name in required_env if not os.environ.get(name))
        if missing_env:
            reasons.append(f"missing environment variables: {missing_env}")

        if runtime.get("requires_sudo"):
            reasons.append("requires sudo/root access")

        ros_profile = system_profile.get("ros", {})
        if not isinstance(ros_profile, dict):
            ros_profile = {}
        ros_requirement_fields = {
            "required_ros_nodes": "nodes",
            "required_topics": "topics",
            "required_services": "services",
            "required_actions": "actions",
        }
        for requirement_field, profile_field in ros_requirement_fields.items():
            required_values = {
                str(item).strip()
                for item in runtime.get(requirement_field, [])
                if str(item).strip()
            }
            if not required_values:
                continue
            available_values = {
                str(item).strip()
                for item in ros_profile.get(profile_field, [])
                if str(item).strip()
            }
            missing_values = sorted(required_values - available_values)
            if missing_values:
                reasons.append(f"missing ROS {profile_field}: {missing_values}")

    network = requirements.get("network_requirements", {})
    if isinstance(network, dict) and network.get("requires_internet"):
        network_access = str(system_profile.get("network_access", "unknown")).lower()
        if network_access in {"false", "0", "no", "off", "none"}:
            reasons.append("requires internet/network access")

    safety = requirements.get("safety_requirements", {})
    if isinstance(safety, dict):
        robot_profile = system_profile.get("robot", {})
        if not isinstance(robot_profile, dict):
            robot_profile = {}

        robot_mode = str(robot_profile.get("mode", "")).strip().lower()
        if safety.get("requires_physical_robot") and robot_mode != "physical":
            reasons.append("requires a physical robot mode")

        allowed_modes = {
            str(item).strip().lower()
            for item in safety.get("allowed_robot_modes", [])
            if str(item).strip()
        }
        if allowed_modes and robot_mode and robot_mode not in allowed_modes:
            reasons.append(f"robot mode must be one of {sorted(allowed_modes)}")

        if safety.get("requires_estop") and robot_profile.get("has_estop") is not True:
            reasons.append("requires robot e-stop availability")

    data = requirements.get("data_requirements", {})
    if isinstance(data, dict):
        required_credentials = {
            str(item).strip()
            for item in data.get("requires_credentials", [])
            if str(item).strip()
        }
        missing_credentials = sorted(
            credential for credential in required_credentials if not os.environ.get(credential)
        )
        if missing_credentials:
            reasons.append(f"missing credential environment variables: {missing_credentials}")

        required_datasets = {
            str(item).strip()
            for item in data.get("requires_datasets", [])
            if str(item).strip()
        }
        missing_datasets = sorted(
            dataset for dataset in required_datasets if not Path(dataset).exists()
        )
        if missing_datasets:
            reasons.append(f"missing datasets: {missing_datasets}")

    return reasons


def load_global_catalog(location: str | Path) -> list[dict[str, Any]]:
    """Load global package targets from a JSON registry file.

    The global catalog is not a source of installed packages. It is a registry
    of cloneable skill package addresses plus enough discovery and requirement
    metadata to decide whether a package is worth installing.
    """

    location_text = str(location)
    if location_text.startswith(("http://", "https://")):
        with urllib.request.urlopen(location_text, timeout=30) as response:
            catalog = json.loads(response.read().decode("utf-8"))
        location_label = location_text
    else:
        path = Path(location)
        if not path.exists():
            return []
        catalog = json.loads(path.read_text(encoding="utf-8"))
        location_label = str(path)

    schema_version = catalog.get("global_catalog_schema_version", catalog.get("schema_version"))
    if schema_version not in GLOBAL_CATALOG_SCHEMA_VERSIONS:
        raise ValueError(
            f"{location_label}: global catalog schema_version must be one of "
            f"{sorted(GLOBAL_CATALOG_SCHEMA_VERSIONS)}, got {schema_version!r}."
        )

    packages = catalog.get("packages", [])
    if not isinstance(packages, list):
        raise ValueError(f"{location_label}: field 'packages' must be a list.")

    targets: list[dict[str, Any]] = []
    next_id = -1
    for package in packages:
        if not isinstance(package, dict):
            raise ValueError(f"{location_label}: each global package entry must be an object.")
        package_name = require_string(package, "package_name", Path(location_label))
        source = package.get("source", {})
        if not isinstance(source, dict):
            raise ValueError(f"{location_label}: package '{package_name}' source must be an object.")
        repo_url = str(source.get("url") or package.get("repo_url") or "").strip()
        if not repo_url:
            raise ValueError(f"{location_label}: field 'repo_url' must be a non-empty string.")
        source_ref = str(
            source.get("commit")
            or source.get("tag")
            or source.get("ref")
            or ""
        ).strip()
        package_summary = require_string(package, "summary", Path(location_label))
        package_language = require_string(package, "language_representation", Path(location_label))
        package_keywords = package.get("keywords", [])
        package_capabilities = package.get("capabilities", [])
        package_platform = package.get("platform_requirements", {})
        package_system = package.get("system_requirements", {})
        package_robot = package.get("robot_requirements", {})
        package_hard = package.get("hard_requirements", {})
        package_targets = package.get("targets", [])
        if not isinstance(package_targets, list):
            raise ValueError(f"{location_label}: package '{package_name}' targets must be a list.")

        for target in package_targets:
            if not isinstance(target, dict):
                raise ValueError(f"{location_label}: target for package '{package_name}' must be an object.")
            target_name = require_string(target, "target_name", Path(location_label))
            action_servers = target.get("action_servers", [])
            input_requirements = target.get("input_requirements", [])
            target_keywords = target.get("keywords", [])
            target_examples = target.get("examples", [])
            target_summary = require_string(target, "summary", Path(location_label))
            target_description = require_string(target, "description", Path(location_label))
            target_language = require_string(target, "language_representation", Path(location_label))
            search_text = build_search_text(
                package_summary=package_summary,
                package_language=package_language,
                package_keywords=package_keywords,
                capabilities=package_capabilities,
                target_name=target_name,
                target_summary=target_summary,
                target_description=target_description,
                target_language=target_language,
                keywords=target_keywords,
                action_servers=action_servers,
                input_requirements=input_requirements,
                examples=target_examples,
                package_robot_requirements=package_robot,
                target_robot_requirements=target.get("robot_requirements", {}),
                package_hard_requirements=package_hard,
                target_hard_requirements=target.get("hard_requirements", {}),
            )
            targets.append(
                {
                    "id": next_id,
                    "package_id": next_id,
                    "package_name": package_name,
                    "target_name": target_name,
                    "repo_url": repo_url,
                    "summary": target_summary,
                    "description": target_description,
                    "language_representation": target_language,
                    "action_servers_json": json_dumps(action_servers),
                    "input_requirements_json": json_dumps(input_requirements),
                    "client_arguments_json": json_dumps(target.get("client_arguments", [])),
                    "keywords_json": json_dumps(target_keywords),
                    "examples_json": json_dumps(target_examples),
                    "package_platform_requirements_json": json_dumps(package_platform),
                    "target_platform_requirements_json": json_dumps(
                        target.get("platform_requirements", {})
                    ),
                    "package_system_requirements_json": json_dumps(package_system),
                    "target_system_requirements_json": json_dumps(
                        target.get("system_requirements", {})
                    ),
                    "package_robot_requirements_json": json_dumps(package_robot),
                    "target_robot_requirements_json": json_dumps(
                        target.get("robot_requirements", {})
                    ),
                    "package_hard_requirements_json": json_dumps(package_hard),
                    "target_hard_requirements_json": json_dumps(
                        target.get("hard_requirements", {})
                    ),
                    "search_text": search_text,
                    "package_summary": package_summary,
                    "package_language_representation": package_language,
                    "package_keywords_json": json_dumps(package_keywords),
                    "package_capabilities_json": json_dumps(package_capabilities),
                    "package_local_path": "",
                    "package_manifest_path": "",
                    "source_json": json_dumps(source),
                    "repo_ref": source_ref,
                    "catalog_source": "global",
                }
            )
            next_id -= 1

    return targets
