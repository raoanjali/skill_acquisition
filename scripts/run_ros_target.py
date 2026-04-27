#!/usr/bin/env python3
"""Deterministically stage, build, and run a ROS package target.

For `--source cloud`, the target repository must contain a `package_runner.json`
file at its root. For `--source local`, the provided package path must point to
the local package root containing `package_runner.json`.

This script is the execution layer for `skill_acq`. It owns the side effects:
copying or cloning a package into a runner workspace, installing manifest-declared
system and Python dependencies, building it with colcon, starting any target
background processes, and calling the target client. Before building it validates
that ROS can find the package in the runner overlay and checks a source stamp;
the build is skipped only when the installed package still matches the staged
source.

Example:
  python3 run_ros_target.py \
    --source cloud \
    --repo-url https://github.com/Nikkhil16/reverse_string_action \
    --target reverse_string \
    --set input_string="hello world"

  python3 run_ros_target.py \
    --source local \
    --package-path /abs/path/to/reverse_string_action \
    --start-only \
    --leave-processes-running \
    --target reverse_string

  python3 run_ros_target.py \
    --source local \
    --package-path /abs/path/to/reverse_string_action \
    --target reverse_string \
    --set input_string="hello world"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from package_catalog import DEFAULT_DB_PATH, build_catalog, find_manifest_files

DEFAULT_WORKSPACE_DIR = Path(
    os.environ.get("SKILL_ACQ_RUNNER_WS", "~/.ros/skill_acq/ros_runner_ws")
).expanduser()
DEFAULT_ROS_LOG_DIR = Path("/tmp/ros_logs")
MANIFEST_NAME = "package_runner.json"
SUPPORTED_SCHEMA_VERSIONS = {2, 3}
STAMP_DIR_NAME = ".skill_acq"
SOURCE_HASH_SKIP_PARTS = {
    ".git",
    "__pycache__",
    "build",
    "install",
    "log",
}
PIP_INSTALL_ARGS_ENV = "SKILL_ACQ_PIP_INSTALL_ARGS"
ROSDEP_INSTALL_ARGS_ENV = "SKILL_ACQ_ROSDEP_INSTALL_ARGS"


class CommandError(RuntimeError):
    """Raised when a local command fails."""


def quote(value: str) -> str:
    return shlex.quote(value)


def shell_join(tokens: list[str]) -> str:
    return " ".join(quote(token) for token in tokens)


def tail(text: str, max_chars: int = 4000) -> str:
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def run_bash(command: str, cwd: Path, timeout: int | None = None) -> dict[str, str | int]:
    completed = subprocess.run(
        ["bash", "-lc", command],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    result = {
        "command": command,
        "cwd": str(cwd),
        "returncode": completed.returncode,
        "stdout": tail(completed.stdout),
        "stderr": tail(completed.stderr),
    }
    if completed.returncode != 0:
        raise CommandError(json.dumps(result, indent=2))
    return result


def detect_ros_setup() -> Path:
    ros_distro = os.environ.get("ROS_DISTRO")
    if ros_distro:
        candidate = Path("/opt/ros") / ros_distro / "setup.bash"
        if candidate.exists():
            return candidate

    setups = sorted(Path("/opt/ros").glob("*/setup.bash"))
    if setups:
        return setups[-1]

    raise FileNotFoundError(
        "Unable to find a ROS 2 underlay. Set ROS_DISTRO or pass --ros-setup."
    )


def repo_dir_name_from_url(repo_url: str) -> str:
    path = urlparse(repo_url).path.rstrip("/")
    if not path:
        raise ValueError(f"Unable to determine repository name from URL: {repo_url}")
    name = path.split("/")[-1]
    if name.endswith(".git"):
        name = name[:-4]
    if not name:
        raise ValueError(f"Unable to determine repository name from URL: {repo_url}")
    return name


def normalize_repo_url(repo_url: str) -> str:
    """Normalize Git remotes enough to catch accidental repo-directory reuse."""

    normalized = repo_url.strip()
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    return normalized.rstrip("/")


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


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    raise ValueError(f"Expected a boolean value, got: {value}")


class RosTargetRunner:
    def __init__(
        self,
        source: str,
        workspace_dir: Path,
        ros_setup: Path,
        repo_url: str | None = None,
        repo_ref: str | None = None,
        package_path: Path | None = None,
        leave_processes_running: bool = False,
        catalog_roots: list[Path] | None = None,
    ) -> None:
        self.source = source
        self.repo_url = repo_url
        self.repo_ref = repo_ref
        self.package_path = package_path.resolve() if package_path else None
        self.workspace_dir = workspace_dir
        self.src_dir = workspace_dir / "src"
        self.catalog_roots = [root.resolve() for root in catalog_roots or []]
        if self.source == "cloud":
            if not self.repo_url:
                raise ValueError("Cloud source requires --repo-url.")
            repo_dir_name = repo_dir_name_from_url(self.repo_url)
        elif self.source == "local":
            if self.package_path is None:
                raise ValueError("Local source requires --package-path.")
            repo_dir_name = self.package_path.name
        else:
            raise ValueError(f"Unsupported source {self.source!r}.")
        self.repo_dir = self.src_dir / repo_dir_name
        self.ros_setup = ros_setup
        self.leave_processes_running = leave_processes_running
        self.manifest: dict | None = None
        self.package_name = ""
        self.overlay_setup = workspace_dir / "install" / "setup.bash"
        self.ros_log_dir = DEFAULT_ROS_LOG_DIR
        self.started_processes: dict[str, subprocess.Popen[str]] = {}
        self.process_log_handles: dict[str, object] = {}

    def prepare_source(self) -> None:
        self.src_dir.mkdir(parents=True, exist_ok=True)
        if self.source == "cloud":
            self._prepare_cloud_source()
            return
        if self.source == "local":
            self._prepare_local_source()
            return
        raise ValueError(f"Unsupported source {self.source!r}.")

    def _prepare_cloud_source(self) -> None:
        if self.repo_dir.exists():
            git_dir = self.repo_dir / ".git"
            if not git_dir.exists():
                raise ValueError(
                    f"Cloud source directory already exists but is not a git clone: {self.repo_dir}"
                )
            remote_result = run_bash(
                "git config --get remote.origin.url",
                cwd=self.repo_dir,
                timeout=30,
            )
            remote_url = str(remote_result["stdout"]).strip()
            if normalize_repo_url(remote_url) != normalize_repo_url(str(self.repo_url)):
                raise ValueError(
                    f"Existing clone at {self.repo_dir} points to {remote_url}, "
                    f"not {self.repo_url}."
                )
            print(f"[runner] Reusing verified clone at {self.repo_dir}")
            self._checkout_repo_ref_if_requested()
            return

        print(f"[runner] Cloning {self.repo_url}")
        run_bash(
            f"git clone --depth 1 {quote(self.repo_url)} {quote(str(self.repo_dir))}",
            cwd=self.workspace_dir,
            timeout=120,
        )
        self._checkout_repo_ref_if_requested()

    def _checkout_repo_ref_if_requested(self) -> None:
        if not self.repo_ref:
            return
        print(f"[runner] Fetching cloud package ref {self.repo_ref}")
        run_bash(
            f"git fetch --depth 1 origin {quote(self.repo_ref)}",
            cwd=self.repo_dir,
            timeout=120,
        )
        run_bash(
            "git checkout --detach FETCH_HEAD",
            cwd=self.repo_dir,
            timeout=60,
        )

    def _prepare_local_source(self) -> None:
        if self.package_path is None:
            raise ValueError("Local source requires --package-path.")
        if not self.package_path.is_absolute():
            raise ValueError("--package-path must be an absolute path.")
        if not self.package_path.exists():
            raise FileNotFoundError(f"Local package path does not exist: {self.package_path}")
        if not self.package_path.is_dir():
            raise ValueError(f"Local package path must be a directory: {self.package_path}")

        manifest_path = self.package_path / MANIFEST_NAME
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Expected {MANIFEST_NAME} in local package path {self.package_path}."
            )

        if self.repo_dir.exists():
            shutil.rmtree(self.repo_dir)

        print(f"[runner] Staging local package from {self.package_path}")
        shutil.copytree(self.package_path, self.repo_dir, symlinks=True)

    def load_manifest(self) -> None:
        manifest_path = self.repo_dir / MANIFEST_NAME
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Expected {MANIFEST_NAME} at {manifest_path}, but it was not found."
            )

        self.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        schema_version = self.manifest.get("schema_version")
        if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"Unsupported manifest schema_version {schema_version!r}. "
                f"This runner expects one of {sorted(SUPPORTED_SCHEMA_VERSIONS)}."
            )

        self.package_name = self._require_string(self.manifest, "package_name")
        overlay_setup = self.manifest.get("overlay_setup", "install/setup.bash")
        self.overlay_setup = self.workspace_dir / overlay_setup

        self._validate_rosdep_install()
        self._validate_python_requirements()
        self._validate_step(self._manifest_section("build"), "build")

        processes = self._manifest_section("processes")
        for process_name, process_step in processes.items():
            if not isinstance(process_step, dict):
                raise ValueError(f"Manifest process '{process_name}' must be an object.")
            self._validate_step(process_step, f"processes.{process_name}")

        targets = self._manifest_section("targets")
        if not targets:
            raise ValueError("Manifest field 'targets' must define at least one target.")
        for target_name, target in targets.items():
            self._validate_target(target_name, target)

    def build_package(self) -> None:
        build_step = self._manifest_section("build")
        self.install_ros_dependencies()
        self.install_python_requirements()
        print(f"[runner] Building {self.package_name}")
        self._run_step(
            build_step,
            use_overlay=False,
            timeout=900,
        )
        if not self.overlay_setup.exists():
            raise FileNotFoundError(
                f"Build completed but overlay setup was not created at {self.overlay_setup}."
            )
        self.validate_installation()
        self._write_install_stamp()
        if self.source == "cloud":
            self.update_local_catalog()

    def validate_installation(self) -> None:
        """Fail if the built overlay cannot provide the requested ROS package."""
        if not self.is_package_installed():
            raise FileNotFoundError(
                "Build completed, but ROS could not find package "
                f"'{self.package_name}' after sourcing {self.overlay_setup}."
            )
        print(f"[runner] Validated installation for package '{self.package_name}'")

    def install_ros_dependencies(self) -> None:
        if not self._rosdep_install_enabled():
            return
        if shutil.which("rosdep") is None:
            raise CommandError(
                "Manifest requests rosdep dependency installation, but `rosdep` is not available. "
                "Install python3-rosdep or disable rosdep_install for this package."
            )

        print(f"[runner] Installing ROS/system dependencies for {self.package_name} with rosdep")
        command_tokens = [
            "rosdep",
            "install",
            "--from-paths",
            str(self.repo_dir),
            "--ignore-src",
            "-r",
            "-y",
            *self._rosdep_install_args(),
        ]
        command = " && ".join(
            [
                f"source {quote(str(self.ros_setup))}",
                shell_join(command_tokens),
            ]
        )
        run_bash(command, cwd=self.workspace_dir, timeout=900)

    def install_python_requirements(self) -> None:
        requirement_files = self._python_requirement_files()
        if not requirement_files:
            return

        self._ensure_pip_available()
        pip_args = self._pip_install_args()
        for requirement_file in requirement_files:
            print(f"[runner] Installing Python requirements from {requirement_file}")
            command_tokens = [
                "python3",
                "-m",
                "pip",
                "--disable-pip-version-check",
                "install",
                *pip_args,
                "-r",
                str(requirement_file),
            ]
            run_bash(shell_join(command_tokens), cwd=self.repo_dir, timeout=900)

    def _ensure_pip_available(self) -> None:
        completed = subprocess.run(
            ["python3", "-m", "pip", "--version"],
            cwd=str(self.repo_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return

        detail = (completed.stderr or completed.stdout).strip()
        message = (
            "Manifest declares Python requirements, but `python3 -m pip` is not available. "
            "Install python3-pip or run skill_acq from a Python environment that provides pip."
        )
        if detail:
            message += f"\n{detail}"
        raise CommandError(message)

    def _pip_install_args(self) -> list[str]:
        if PIP_INSTALL_ARGS_ENV in os.environ:
            return shlex.split(os.environ[PIP_INSTALL_ARGS_ENV])
        if os.environ.get("VIRTUAL_ENV"):
            return []
        return ["--user"]

    def _rosdep_install_args(self) -> list[str]:
        if ROSDEP_INSTALL_ARGS_ENV in os.environ:
            return shlex.split(os.environ[ROSDEP_INSTALL_ARGS_ENV])
        return []

    def is_package_installed(self) -> bool:
        if not self.overlay_setup.exists():
            return False

        check_command = " && ".join(
            [
                f"export ROS_LOG_DIR={quote(str(self.ros_log_dir))}",
                f"source {quote(str(self.ros_setup))}",
                f"source {quote(str(self.overlay_setup))}",
                shell_join(["ros2", "pkg", "prefix", self.package_name]),
            ]
        )
        completed = subprocess.run(
            ["bash", "-lc", check_command],
            cwd=str(self.workspace_dir),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        return completed.returncode == 0

    def is_installation_current(self) -> bool:
        """Return true only when ROS sees the package and the source stamp matches."""

        if not self.is_package_installed():
            return False

        stamp = self._read_install_stamp()
        if not stamp:
            return False
        expected_signature = self._current_source_signature()
        return (
            stamp.get("package_name") == self.package_name
            and stamp.get("source") == self.source
            and stamp.get("repo_url") == self.repo_url
            and stamp.get("repo_ref") == self.repo_ref
            and stamp.get("source_signature") == expected_signature
        )

    def list_targets(self) -> list[tuple[str, str]]:
        targets = self._manifest_section("targets")
        listed: list[tuple[str, str]] = []
        for target_name, target in targets.items():
            description = target.get("description", "")
            listed.append((target_name, description))
        return listed

    def resolve_target_name(self, requested_target: str | None) -> str:
        targets = self._manifest_section("targets")
        if requested_target:
            if requested_target not in targets:
                available = ", ".join(sorted(targets))
                raise ValueError(
                    f"Unknown target '{requested_target}'. Available targets: {available}"
                )
            return requested_target

        if len(targets) == 1:
            return next(iter(targets))

        available = ", ".join(sorted(targets))
        raise ValueError(
            "This repository defines multiple targets. "
            f"Choose one with --target. Available targets: {available}"
        )

    def resolve_values(
        self,
        target_name: str,
        provided_values: dict[str, str],
        include_client_arguments: bool = True,
        require_required_values: bool = True,
    ) -> dict[str, str]:
        target = self._target(target_name)
        specs = self._target_value_specs(target, include_client_arguments=include_client_arguments)
        values = dict(provided_values)
        missing: list[str] = []

        for spec in specs:
            name = self._require_string(spec, "name")
            if name in values:
                values[name] = self._normalize_argument_value(spec, values[name])
            elif self._requires_user_value(spec):
                missing.append(name)
            elif "default" in spec:
                values[name] = self._normalize_argument_value(spec, spec["default"])
            elif require_required_values and bool(spec.get("required", False)):
                missing.append(name)

        if missing:
            raise ValueError(
                "Missing required target values: "
                + ", ".join(sorted(missing))
                + ". Provide them with --set NAME=VALUE."
            )

        return values

    def start_target_processes(self, target_name: str, values: dict[str, str]) -> None:
        target = self._target(target_name)
        process_names = target.get("start", [])
        if not isinstance(process_names, list):
            raise ValueError(f"Target '{target_name}' field 'start' must be a list.")

        for process_name in process_names:
            if not isinstance(process_name, str) or not process_name:
                raise ValueError(
                    f"Target '{target_name}' field 'start' must contain process names."
                )
            self._start_process(process_name, values)

    def call_target(self, target_name: str, values: dict[str, str]) -> dict[str, str | int]:
        target = self._target(target_name)
        client_step = target["client"]
        client_command = self._render_command_tokens(client_step["command"], values)
        client_command.extend(self._expand_client_arguments(client_step, values))

        print(f"[runner] Calling target '{target_name}' for {self.package_name}")
        return self._run_step(
            client_step,
            values=values,
            extra_tokens=client_command[len(client_step["command"]) :],
            use_overlay=True,
            timeout=120,
        )

    def cleanup(self) -> None:
        if self.leave_processes_running:
            return

        for process_name in reversed(list(self.started_processes.keys())):
            process = self.started_processes[process_name]
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            self._close_log_handle(process_name)

    def _start_process(self, process_name: str, values: dict[str, str]) -> None:
        process = self.started_processes.get(process_name)
        if process and process.poll() is None:
            print(f"[runner] Process '{process_name}' is already running")
            return

        process_step = self._process(process_name)
        startup_delay = float(process_step.get("startup_delay_sec", 3.0))
        cwd = self._resolve_cwd(process_step, values)
        command = self._compose_shell_command(
            process_step["command"],
            values=values,
            use_overlay=True,
        )

        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.workspace_dir / f"{process_name}.log"
        log_handle = open(log_path, "a", encoding="utf-8")
        self.process_log_handles[process_name] = log_handle

        print(f"[runner] Starting process '{process_name}' from {cwd}")
        print(f"[runner] Process log: {log_path}")
        process = subprocess.Popen(
            ["bash", "-lc", command],
            cwd=str(cwd),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self.started_processes[process_name] = process

        time.sleep(startup_delay)
        if process.poll() is not None:
            log_handle.flush()
            log_tail = log_path.read_text(encoding="utf-8")[-4000:]
            raise CommandError(
                f"Process '{process_name}' exited early.\nLog tail:\n{log_tail}"
            )

    def _close_log_handle(self, process_name: str) -> None:
        log_handle = self.process_log_handles.pop(process_name, None)
        if log_handle is not None:
            log_handle.close()

    def update_local_catalog(self) -> None:
        manifest_path = self.repo_dir / MANIFEST_NAME
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Expected {MANIFEST_NAME} at {manifest_path}, but it was not found."
            )

        resolved_manifest_path = manifest_path.resolve()
        cloud_package_name = self._manifest_package_name(resolved_manifest_path)
        manifests_by_package: dict[str, Path] = {}

        for existing_manifest_path in self._catalog_manifest_paths():
            self._add_manifest_if_usable(
                manifests_by_package,
                existing_manifest_path,
                replace_package=cloud_package_name,
            )
        for discovered_manifest_path in find_manifest_files([self.src_dir, *self.catalog_roots]):
            self._add_manifest_if_usable(
                manifests_by_package,
                discovered_manifest_path,
                replace_package=cloud_package_name,
            )
        self._add_manifest_if_usable(
            manifests_by_package,
            resolved_manifest_path,
            replace_package=None,
        )

        manifest_paths = sorted(manifests_by_package.values())

        package_count, target_count = build_catalog(manifest_paths, DEFAULT_DB_PATH)
        print(
            "[runner] Updated local package catalog: "
            f"{package_count} package(s), {target_count} target(s)"
        )

    def _catalog_manifest_paths(self) -> list[Path]:
        if not DEFAULT_DB_PATH.exists():
            return []
        try:
            connection = sqlite3.connect(DEFAULT_DB_PATH)
            connection.row_factory = sqlite3.Row
            try:
                rows = connection.execute(
                    "SELECT manifest_path FROM packages ORDER BY package_name"
                ).fetchall()
            finally:
                connection.close()
        except sqlite3.Error:
            return []

        manifests: list[Path] = []
        for row in rows:
            manifest_path = Path(str(row["manifest_path"]))
            if manifest_path.exists():
                manifests.append(manifest_path.resolve())
        return manifests

    def _add_manifest_if_usable(
        self,
        manifests_by_package: dict[str, Path],
        manifest_path: Path,
        replace_package: str | None,
    ) -> None:
        resolved_manifest_path = manifest_path.resolve()
        if not resolved_manifest_path.exists():
            return
        try:
            package_name = self._manifest_package_name(resolved_manifest_path)
        except (json.JSONDecodeError, ValueError):
            return
        if replace_package and package_name == replace_package:
            return
        manifests_by_package[package_name] = resolved_manifest_path

    def _stamp_path(self) -> Path:
        return self.workspace_dir / STAMP_DIR_NAME / f"{self.package_name}.stamp.json"

    def _read_install_stamp(self) -> dict[str, object]:
        stamp_path = self._stamp_path()
        if not stamp_path.exists():
            return {}
        try:
            stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return stamp if isinstance(stamp, dict) else {}

    def _write_install_stamp(self) -> None:
        stamp_path = self._stamp_path()
        stamp_path.parent.mkdir(parents=True, exist_ok=True)
        stamp = {
            "package_name": self.package_name,
            "source": self.source,
            "repo_url": self.repo_url,
            "repo_ref": self.repo_ref,
            "source_signature": self._current_source_signature(),
        }
        stamp_path.write_text(json.dumps(stamp, indent=2, sort_keys=True), encoding="utf-8")

    def _current_source_signature(self) -> str:
        if self.source == "cloud":
            result = run_bash("git rev-parse HEAD", cwd=self.repo_dir, timeout=30)
            commit = str(result["stdout"]).strip()
            return f"git:{normalize_repo_url(str(self.repo_url))}:{commit}"
        return f"tree:{self._hash_source_tree(self.repo_dir)}"

    def _hash_source_tree(self, root: Path) -> str:
        digest = hashlib.sha256()
        for path in sorted(root.rglob("*")):
            if any(part in SOURCE_HASH_SKIP_PARTS for part in path.relative_to(root).parts):
                continue
            if path.is_symlink():
                relative = path.relative_to(root).as_posix()
                digest.update(relative.encode("utf-8"))
                digest.update(b"\0symlink\0")
                digest.update(os.readlink(path).encode("utf-8"))
                digest.update(b"\0")
                continue
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()

    def _manifest_package_name(self, manifest_path: Path) -> str:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        value = manifest.get("package_name")
        if not isinstance(value, str) or not value:
            raise ValueError(f"{manifest_path}: field 'package_name' must be a non-empty string.")
        return value

    def _validate_rosdep_install(self) -> None:
        self._rosdep_install_enabled()

    def _rosdep_install_enabled(self) -> bool:
        if self.manifest is None:
            raise RuntimeError("Manifest has not been loaded.")
        value = self.manifest.get("rosdep_install", False)
        if not isinstance(value, bool):
            raise ValueError("Manifest field 'rosdep_install' must be a boolean.")
        return value

    def _validate_python_requirements(self) -> None:
        self._python_requirement_entries()

    def _python_requirement_entries(self) -> list[str]:
        if self.manifest is None:
            raise RuntimeError("Manifest has not been loaded.")
        value = self.manifest.get("python_requirements", [])
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("Manifest field 'python_requirements' must be a list of paths.")

        entries: list[str] = []
        for entry in value:
            if not isinstance(entry, str) or not entry.strip():
                raise ValueError(
                    "Manifest field 'python_requirements' must contain non-empty path strings."
                )
            entries.append(entry.strip())
        return entries

    def _python_requirement_files(self) -> list[Path]:
        repo_root = self.repo_dir.resolve()
        requirement_files: list[Path] = []
        for entry in self._python_requirement_entries():
            relative_path = Path(entry)
            if relative_path.is_absolute():
                raise ValueError(
                    "Manifest field 'python_requirements' must use paths relative to the package root."
                )

            requirement_file = (repo_root / relative_path).resolve()
            try:
                requirement_file.relative_to(repo_root)
            except ValueError as exc:
                raise ValueError(
                    f"Python requirements path escapes package root: {entry}"
                ) from exc

            if not requirement_file.is_file():
                raise FileNotFoundError(
                    f"Python requirements file declared in manifest was not found: {entry}"
                )
            requirement_files.append(requirement_file)
        return requirement_files

    def _manifest_section(self, key: str) -> dict:
        if self.manifest is None:
            raise RuntimeError("Manifest has not been loaded.")
        value = self.manifest.get(key)
        if not isinstance(value, dict):
            raise ValueError(f"Manifest field '{key}' must be an object.")
        return value

    def _process(self, process_name: str) -> dict:
        processes = self._manifest_section("processes")
        if process_name not in processes:
            raise ValueError(f"Unknown process '{process_name}' referenced by target.")
        process = processes[process_name]
        if not isinstance(process, dict):
            raise ValueError(f"Manifest process '{process_name}' must be an object.")
        return process

    def _target(self, target_name: str) -> dict:
        targets = self._manifest_section("targets")
        if target_name not in targets:
            raise ValueError(f"Unknown target '{target_name}'.")
        target = targets[target_name]
        if not isinstance(target, dict):
            raise ValueError(f"Manifest target '{target_name}' must be an object.")
        return target

    def _validate_target(self, target_name: str, target: object) -> None:
        if not isinstance(target, dict):
            raise ValueError(f"Manifest target '{target_name}' must be an object.")

        start = target.get("start", [])
        if not isinstance(start, list):
            raise ValueError(f"Target '{target_name}' field 'start' must be a list.")
        for process_name in start:
            if not isinstance(process_name, str) or not process_name:
                raise ValueError(
                    f"Target '{target_name}' field 'start' must contain process names."
                )
            self._process(process_name)

        client = target.get("client")
        if not isinstance(client, dict):
            raise ValueError(f"Target '{target_name}' must define a 'client' object.")
        self._validate_step(client, f"targets.{target_name}.client")

        input_requirements = target.get("input_requirements", [])
        if not isinstance(input_requirements, list):
            raise ValueError(
                f"Target '{target_name}' field 'input_requirements' must be a list."
            )

        arguments = client.get("arguments", [])
        if not isinstance(arguments, list):
            raise ValueError(
                f"Target '{target_name}' client field 'arguments' must be a list."
            )

    def _validate_step(self, step: dict, label: str) -> None:
        self._require_string(step, "working_directory")
        self._require_string(step, "cwd")
        command = step.get("command")
        if not isinstance(command, list) or not command or not all(
            isinstance(token, str) for token in command
        ):
            raise ValueError(f"Manifest step '{label}' must define a non-empty command list.")

    def _require_string(self, obj: dict, key: str) -> str:
        value = obj.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Manifest field '{key}' must be a non-empty string.")
        return value

    def _resolve_cwd(self, step: dict, values: dict[str, str] | None = None) -> Path:
        base_kind = step["working_directory"]
        cwd_value = step["cwd"]
        if values is not None:
            cwd_value = self._render_token(cwd_value, values)
        relative = Path(cwd_value)
        if base_kind == "workspace_root":
            return (self.workspace_dir / relative).resolve()
        if base_kind == "repo_root":
            return (self.repo_dir / relative).resolve()
        raise ValueError(
            f"Unsupported working_directory '{base_kind}'. "
            "Expected 'workspace_root' or 'repo_root'."
        )

    def _compose_shell_command(
        self,
        command_tokens: list[str],
        values: dict[str, str] | None = None,
        use_overlay: bool = True,
    ) -> str:
        self.ros_log_dir.mkdir(parents=True, exist_ok=True)
        segments = [f"export ROS_LOG_DIR={quote(str(self.ros_log_dir))}"]
        segments.append(f"source {quote(str(self.ros_setup))}")
        if use_overlay:
            segments.append(f"source {quote(str(self.overlay_setup))}")
        rendered_tokens = self._render_command_tokens(command_tokens, values or {})
        segments.append(shell_join(rendered_tokens))
        return " && ".join(segments)

    def _run_step(
        self,
        step: dict,
        values: dict[str, str] | None = None,
        extra_tokens: list[str] | None = None,
        use_overlay: bool = True,
        timeout: int | None = None,
    ) -> dict[str, str | int]:
        command_tokens = self._render_command_tokens(step["command"], values or {})
        if extra_tokens:
            command_tokens.extend(extra_tokens)

        return run_bash(
            self._compose_shell_command(command_tokens, values={}, use_overlay=use_overlay),
            cwd=self._resolve_cwd(step, values or {}),
            timeout=timeout,
        )

    def _render_command_tokens(
        self,
        command_tokens: list[str],
        values: dict[str, str],
    ) -> list[str]:
        return [self._render_token(token, values) for token in command_tokens]

    def _render_token(self, token: str, values: dict[str, str]) -> str:
        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in values:
                raise ValueError(
                    f"Missing value for placeholder '{name}'. Provide it with --set {name}=VALUE."
                )
            return values[name]

        return pattern.sub(replace, token)

    def _expand_client_arguments(
        self,
        client_step: dict,
        values: dict[str, str],
    ) -> list[str]:
        specs = client_step.get("arguments", [])
        if not isinstance(specs, list):
            raise ValueError("Manifest client.arguments must be a list.")

        expanded: list[str] = []
        missing: list[str] = []

        for spec in specs:
            if not isinstance(spec, dict):
                raise ValueError("Each client argument specification must be an object.")

            name = self._require_string(spec, "name")
            flag = spec.get("flag")
            required = bool(spec.get("required", False))

            if name in values:
                normalized_value = self._normalize_argument_value(spec, values[name])
            elif "default" in spec:
                normalized_value = self._normalize_argument_value(spec, spec["default"])
            else:
                if required:
                    missing.append(name)
                continue

            if spec.get("type", "string") == "bool":
                parsed_bool = parse_bool(normalized_value)
                if flag:
                    if parsed_bool:
                        expanded.append(flag)
                else:
                    expanded.append("true" if parsed_bool else "false")
                continue

            if flag:
                expanded.extend([flag, normalized_value])
            else:
                expanded.append(normalized_value)

        if missing:
            raise ValueError(
                "Missing required client values: "
                + ", ".join(sorted(missing))
                + ". Provide them with --set NAME=VALUE."
            )

        return expanded

    def _target_value_specs(
        self,
        target: dict,
        include_client_arguments: bool = True,
    ) -> list[dict]:
        specs_by_name: dict[str, dict] = {}

        for spec in target.get("input_requirements", []):
            if not isinstance(spec, dict):
                raise ValueError("Each target input requirement must be an object.")
            name = self._require_string(spec, "name")
            specs_by_name[name] = dict(spec)

        if not include_client_arguments:
            return list(specs_by_name.values())

        client = target.get("client", {})
        client_arguments = client.get("arguments", [])
        if not isinstance(client_arguments, list):
            raise ValueError("Manifest client.arguments must be a list.")

        for spec in client_arguments:
            if not isinstance(spec, dict):
                raise ValueError("Each client argument specification must be an object.")
            name = self._require_string(spec, "name")
            merged = dict(specs_by_name.get(name, {}))
            merged.update(spec)
            specs_by_name[name] = merged

        return list(specs_by_name.values())

    def _requires_user_value(self, spec: dict) -> bool:
        """Return true when a value must be provided instead of defaulted."""

        semantic_role = str(spec.get("semantic_role", "")).strip().lower()
        name = str(spec.get("name", "")).strip().lower()
        return (
            bool(spec.get("requires_user_value", False))
            or semantic_role == "output_topic"
            or name in {"publish_topic", "output_topic"}
        )

    def _normalize_argument_value(self, spec: dict, raw_value: object) -> str:
        arg_type = spec.get("type", "string")
        if arg_type == "bool":
            normalized_value = "true" if parse_bool(str(raw_value)) else "false"
        elif arg_type == "int":
            normalized_value = str(int(raw_value))
        elif arg_type == "float":
            normalized_value = str(float(raw_value))
        else:
            normalized_value = str(raw_value)

        choices = spec.get("choices")
        if choices is not None and normalized_value not in choices:
            raise ValueError(
                f"Argument '{self._require_string(spec, 'name')}' must be one of {choices}, "
                f"got {normalized_value!r}."
            )

        return normalized_value


def run_target(
    source: str,
    workspace_dir: Path | str = DEFAULT_WORKSPACE_DIR,
    ros_setup: Path | str | None = None,
    repo_url: str | None = None,
    repo_ref: str | None = None,
    package_path: Path | str | None = None,
    target: str | None = None,
    values: dict[str, str] | None = None,
    list_targets: bool = False,
    start_only: bool = False,
    leave_processes_running: bool = False,
    catalog_roots: list[Path | str] | None = None,
) -> dict[str, Any]:
    """Stage, build, start, and optionally call a package target.

    This is the importable runner entry point used by `skill_acq.py`. The CLI
    below is intentionally a thin wrapper around this function so script-based
    debugging and library-style orchestration stay in sync.
    """

    ros_setup_path = Path(ros_setup).resolve() if ros_setup else detect_ros_setup()
    package_path_obj = Path(package_path).expanduser() if package_path else None
    resolved_values = dict(values or {})

    if source == "cloud" and not repo_url:
        raise ValueError("--repo-url is required when source='cloud' is used.")
    if source == "local" and package_path_obj is None:
        raise ValueError("--package-path is required when source='local' is used.")

    runner = RosTargetRunner(
        source=source,
        workspace_dir=Path(workspace_dir).resolve(),
        ros_setup=ros_setup_path,
        repo_url=repo_url,
        repo_ref=repo_ref,
        package_path=package_path_obj,
        leave_processes_running=leave_processes_running,
        catalog_roots=[Path(root).resolve() for root in catalog_roots or []],
    )

    try:
        runner.prepare_source()
        runner.load_manifest()

        if list_targets:
            return {
                "package_name": runner.package_name,
                "targets": runner.list_targets(),
            }

        target_name = runner.resolve_target_name(target)
        target_values = runner.resolve_values(
            target_name,
            resolved_values,
            include_client_arguments=not start_only,
            require_required_values=not start_only,
        )
        if runner.is_installation_current():
            print(
                f"[runner] Package '{runner.package_name}' is already installed "
                "and matches the staged source; skipping build"
            )
        else:
            print(f"[runner] Installing package '{runner.package_name}' into {runner.workspace_dir}")
            runner.build_package()
        runner.start_target_processes(target_name, target_values)

        result: dict[str, Any] = {
            "package_name": runner.package_name,
            "target_name": target_name,
            "values": target_values,
            "start_only": start_only,
        }

        if start_only:
            print(f"[runner] Started target '{target_name}' processes only")
            return result

        result["client_result"] = runner.call_target(target_name, target_values)
        return result
    finally:
        runner.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage a ROS package from a local path or GitHub and run one target from its "
            "package_runner.json contract."
        )
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=("local", "cloud"),
        help="Package source type: 'local' uses --package-path, 'cloud' uses --repo-url.",
    )
    parser.add_argument(
        "--repo-url",
        default=None,
        help="GitHub repository URL for the ROS package when --source cloud is used.",
    )
    parser.add_argument(
        "--repo-ref",
        default=None,
        help="Optional git branch, tag, or commit to fetch and checkout for --source cloud.",
    )
    parser.add_argument(
        "--package-path",
        default=None,
        help="Absolute path to the local package root when --source local is used.",
    )
    parser.add_argument(
        "--target",
        default=None,
        help=(
            "Manifest target name to run. Optional only when the manifest "
            "defines exactly one target."
        ),
    )
    parser.add_argument(
        "--list-targets",
        action="store_true",
        help="List the targets declared by the repository and exit.",
    )
    parser.add_argument(
        "--start-only",
        action="store_true",
        help="Build the package and start the target processes, but do not call the client.",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=(
            "Provide values for manifest-declared client arguments. "
            "Repeat for multiple values."
        ),
    )
    parser.add_argument(
        "--workspace-dir",
        default=str(DEFAULT_WORKSPACE_DIR),
        help="Directory to use as the temporary ROS workspace.",
    )
    parser.add_argument(
        "--catalog-root",
        action="append",
        default=[],
        help=(
            "Additional workspace root to scan when refreshing the local catalog after "
            "a cloud install. Repeat for multiple roots."
        ),
    )
    parser.add_argument(
        "--ros-setup",
        default=None,
        help="Path to the ROS 2 underlay setup.bash. Auto-detected if omitted.",
    )
    parser.add_argument(
        "--leave-processes-running",
        action="store_true",
        help="Do not stop started background processes when this script exits.",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        values = parse_set_values(args.set)
        result = run_target(
            source=args.source,
            workspace_dir=args.workspace_dir,
            ros_setup=args.ros_setup,
            repo_url=args.repo_url,
            repo_ref=args.repo_ref,
            package_path=args.package_path,
            target=args.target,
            values=values,
            list_targets=args.list_targets,
            start_only=args.start_only,
            leave_processes_running=args.leave_processes_running,
            catalog_roots=args.catalog_root,
        )

        if args.list_targets:
            for target_name, description in result["targets"]:
                if description:
                    print(f"{target_name}: {description}")
                else:
                    print(target_name)
            return 0

        if args.start_only:
            return 0

        client_result = result["client_result"]

        if client_result["stdout"]:
            print("[client stdout]")
            print(client_result["stdout"])
        if client_result["stderr"]:
            print("[client stderr]")
            print(client_result["stderr"])

        return 0
    except (CommandError, FileNotFoundError, ValueError) as exc:
        print(f"[runner error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
