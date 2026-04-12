#!/usr/bin/env python3
"""Build the local package catalog database from `package_runner.json` manifests.

Run this whenever new local packages are added to the workspace or when package
metadata changes. Cloud installs are handled automatically by `run_ros_target.py`
after a successful build, but explicit rebuilds are useful during development.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from package_catalog import DEFAULT_DB_PATH, build_catalog, find_manifest_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan directories for package_runner.json manifests and build a "
            "SQLite catalog database for package discovery."
        )
    )
    parser.add_argument(
        "--root",
        action="append",
        default=[],
        help=(
            "Directory to scan for package manifests. Repeat for multiple roots. "
            "Defaults to the current workspace."
        ),
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Where to write the SQLite catalog database.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    roots = [Path(root).resolve() for root in args.root] or [Path.cwd().resolve()]
    manifest_paths = find_manifest_files(roots)
    if not manifest_paths:
        print("No package_runner.json manifests were found.")
        return 1

    db_path = Path(args.db_path).resolve()
    package_count, target_count = build_catalog(manifest_paths, db_path)
    print(
        f"Indexed {package_count} package(s) and {target_count} target(s) into {db_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
