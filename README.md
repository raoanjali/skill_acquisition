# skill_acq : What this package is and is not

`skill_acq` is a ROS2 package for **runtime skill acquisition**. It allows an upstream planner, agent, or executive layer to request a capability in natural language. `skill_acq` checks whether a compatible implementation already exists locally, and, if not, it acquires a compatible skill from a curated global catalog. The selected skill is then installed, validated, brought online, and made immediately available for execution via a ROS2 action. Once a skill has been acquired successfully, it is written back into the local catalog so future requests can resolve it locally.

The goal of this package is not to be another task planner or robot executive. This package is **not** a task decomposition framework, a symbolic planner, or a general robot executive. It does not decide the full task sequence for the robot. Instead, its purpose is narrower: **turn a missing capability into a runnable ROS interface at runtime**. This package is also **not** a generic code-generation system or an unrestricted package search engine. The model does not invent packages or generate arbitrary runtime shell logic. It chooses among catalog candidates, and the executable behavior is defined by each skill package’s manifest.

You can find a demo here: https://www.youtube.com/watch?v=NsvAFBiPR-U 

In short:

- **Upstream system**: decides what capability is needed next
- **`skill_acq`**: decides how to make that capability runnable on this robot

## Architecture
`skill_acq` sits at the boundary between **missing-capability detection** and **runtime execution**. It can be used from multiple upstream autonomy stacks, but its boundary stays the same in all cases: it is invoked when some other module has already decided that the robot needs a capability, but there is no compatible live provider for that capability on the robot right now.


## Capability discovery
`skill_acq` uses a local-first discovery model.

Local discovery
The first step is always to check the local capability catalog for installed skills that are already available on the robot. This local-first behavior matters for two reasons:
1. It avoids unnecessary downloads
2. It lets the robot’s capability surface grow persistently over time

Global discovery
- If no compatible local match exists, skill_acq searches a curated global catalog of approved skills. This catalog is not a raw index of arbitrary repositories. It is a reviewed registry of known skills that include the metadata required for compatibility checking and runtime bringup.
- Discovery is not purely semantic. A package that sounds correct in natural language is not useful if it cannot run on the target robot. For that reason, discovery is split into two stages:
1. hard filtering
2. semantic selection among compatible candidates

Capability contracts
Every skill must declare a machine-readable capability contract. In v1, compatibility is expressed primarily through a single platform_name.
This keeps the interface simple for demo use and makes it easy for upstream modules to call skill_acq without needing to provide full embodiment metadata.
v1 contract philosophy
For the first version, the caller only needs to provide:
platform_name
requested capability
optional execution parameters
Future versions can extend this to support richer platform variants, sensor configurations, and compute requirements.
Example capability contract
{
 "skill_id": "come_to_user",
 "description": "Navigate toward the user until within a handoff distance.",
 "platform_name": ["mars"],
 "ros_distro": ["humble"],
 "interfaces_exposed": {
   "actions": ["come_to_user"]
 },
 "launch_target": "come_to_user.launch.py",
 "validation": {
   "type": "action_server_available",
   "name": "/come_to_user"
 }
}
Hard filtering fields for v1
At minimum, a skill should declare:
platform_name
supported ROS distribution
exposed ROS interface
launch target
validation method
The purpose of the contract is not just to describe what the skill does, but to make it possible to filter out skills that are incompatible before any semantic matching occurs.



## Package Contents

- `scripts/skill_acq.py`: Main user-facing entry point. It accepts a natural-language request, calls the selector helper, and runs the selected target through the runner helper.
- `scripts/select_ros_target.py`: Provides the importable `select_best_target()` helper, reads `package_catalog.db`, filters targets by system compatibility, optionally asks an OpenAI model to choose among compatible candidates, and can still run as a standalone CLI.
- `scripts/run_ros_target.py`: Provides the importable `run_target()` helper, stages a package from a local path or cloud repo, checks whether it is already installed in the runner workspace, builds if needed, starts target processes, calls the target client, and can still run as a standalone CLI.
- `scripts/build_package_catalog.py`: Rebuilds the local catalog by scanning for `package_runner.json` manifests.
- `scripts/package_catalog.py`: Shared library for catalog schema creation, manifest extraction, compatibility checks, full-text ranking helpers, and DB access.
- `package_catalog.db`: Packaged SQLite catalog used by default. It can start empty; cloud installs add successful packages later.
- `global_package_catalog.json`: Packaged seed registry of cloneable skill packages used when the local catalog has no satisfying action. The default runtime global catalog URL is `https://raw.githubusercontent.com/Nikkhil16/Demo/main/global_package_catalog.json`.

## How The Pipeline Works

1. A skill package provides a `package_runner.json` manifest.
2. `build_package_catalog.py` indexes local manifests into `package_catalog.db`.
3. The user calls `skill_acq.py` with a natural-language request.
4. `skill_acq.py` calls the `select_best_target()` helper with that request.
5. `select_ros_target.py` filters the local catalog by hard constraints such as OS, ROS distro, required commands, hardware, robot requirements, and safety/resource requirements.
6. The LLM is asked whether any compatible local action can satisfy the request. If yes, that package target is selected.
7. If the LLM says no local action satisfies the request, `select_ros_target.py` loads `global_package_catalog.json`, filters it by the same hard constraints, and asks the LLM to choose from those global candidates.
8. `skill_acq.py` prints each major pipeline step, then calls the `run_target()` helper with the selected package and target. Local selections use the local package path; global selections use the package `repo_url` and optional git ref from the global catalog.
9. `run_ros_target.py` stages the package into the runner workspace, skips the build only if ROS can find the package and the install stamp still matches the staged source, otherwise installs manifest-declared Python requirements, builds it, validates that ROS can find the package after sourcing the runner overlay, starts required processes, and runs the target client.

The action call is manifest-driven. The LLM chooses the package, target, and action server, but it does not generate arbitrary shell scripts. The actual client command comes from `package_runner.json`, which is safer and repeatable. The selector and runner scripts are still available as CLI tools for debugging, but the main `skill_acq.py` pipeline now uses Python helper functions instead of shelling out to sibling scripts.

Skill packages can ask the runner to install ROS/system dependencies from `package.xml` with top-level `"rosdep_install": true` in `package_runner.json`. During installation, the runner runs `rosdep install --from-paths <staged-package> --ignore-src -r -y` before `colcon build`. Set `SKILL_ACQ_ROSDEP_INSTALL_ARGS` to append extra rosdep arguments.

Skill packages can declare pip requirements with a top-level `python_requirements` list in `package_runner.json`, for example `"python_requirements": ["requirements.txt"]`. During installation, the runner installs each listed file from the staged package root with `python3 -m pip install --user -r ...` before running `colcon build`. Set `SKILL_ACQ_PIP_INSTALL_ARGS` to override the pip arguments, for example `SKILL_ACQ_PIP_INSTALL_ARGS=""` inside a virtual environment.

The default runner workspace is `~/.ros/skill_acq/ros_runner_ws`. Override it with `--workspace-dir` or by setting `SKILL_ACQ_RUNNER_WS`. Downloaded cloud packages are staged and built there, not in your main ROS workspace.

## Build

From the ROS workspace root:

```bash
colcon build --packages-select skill_acq
source install/setup.bash
```

For a local development demo where `reverse_string_action` is also present in the same workspace, you may build both packages. For the intended cloud-install test, build only `skill_acq` and let the global catalog fetch `reverse_string_action`.

## Rebuild The Local Catalog

The catalog database is generated from `package_runner.json` files. Rebuild it after adding or editing local skill manifests:

```bash
ros2 run skill_acq build_package_catalog.py --root /home/nikhil/workspace/RoboUniversity/RoboUniversity
```

The default catalog path is `skill_acq/package_catalog.db` when running from source. When installed, the database is installed under the package share directory, but the scripts still work with an explicit `--db-path` if you want to control the catalog location.

## Run A Skill From Natural Language

Example request:

```bash
ros2 run skill_acq skill_acq.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string
```

Expected result for the `reverse_string_action` example is a successful target call that reports:

```text
Reversed string: 'olleh'
```

You can also run from source without installing the `skill_acq` package:

```bash
python3 /home/nikhil/workspace/RoboUniversity/RoboUniversity/skill_acq/scripts/skill_acq.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string
```

For the reverse-string example, the output topic must be supplied by the user. Use `--set publish_topic=/your_topic` to choose where the server publishes the reversed string.

During execution, `skill_acq.py` prints progress messages such as "Looking for package locally", "Found a matching global package target", "Downloading package from ...", and "Installing or reusing package ...". These messages are meant to make it clear whether the pipeline is selecting, downloading, building, starting the server, or calling the client.

## OpenAI-Based Target Selection

The selector always starts with the local catalog and local compatibility checks. It does not ask a model to invent packages. The OpenAI step only chooses among candidates that already exist in `package_catalog.db` or in the configured global catalog, and that passed system compatibility checks.

Default behavior:

- `--selection-backend openai`: require OpenAI and fail if `OPENAI_API_KEY` is missing.
- `--selection-backend catalog`: never call OpenAI.
- `--selection-backend auto`: use OpenAI if `OPENAI_API_KEY` is available; otherwise use catalog ranking.

Set your API key in the shell before running:

```bash
export OPENAI_API_KEY="your_api_key_here"
```

Then run:

```bash
ros2 run skill_acq skill_acq.py \
  'reverse the string "hello"' \
  --selection-backend openai \
  --set publish_topic=/rev_string
```

The default OpenAI model is a GPT-4-class model configured by `select_ros_target.py`. You can override it:

```bash
ros2 run skill_acq skill_acq.py \
  'reverse the string "hello"' \
  --selection-backend openai \
  --openai-model gpt-4o-2024-08-06 \
  --set publish_topic=/rev_string
```

You can also set a default model with:

```bash
export SKILL_ACQ_OPENAI_MODEL="gpt-4o-2024-08-06"
```

The OpenAI response is validated before use. The selected package name, target name, and action server name must match one of the compatible catalog candidates. When the model can infer manifest-declared input values unambiguously from the request, those values are used unless the user already supplied an explicit `--set NAME=VALUE`.

Catalog-only mode uses a minimum semantic score so an unrelated local package is not run just because it is technically compatible. The default is `0.03`:

```bash
ros2 run skill_acq skill_acq.py \
  'reverse the string "hello"' \
  --selection-backend catalog \
  --min-catalog-score 0.03 \
  --set publish_topic=/rev_string
```

## Global Package Catalog

The global catalog is a JSON registry of packages that may not be installed locally yet. It is not a build artifact and it is not the installed package itself. Each entry should provide enough metadata to decide whether the package can be installed and whether one of its action servers can satisfy a user request.

The local packaged seed catalog lives at:

```text
skill_acq/global_package_catalog.json
```

The selector accepts either a local file path or an HTTP(S) URL:

```bash
ros2 run skill_acq skill_acq.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string \
  --global-catalog-path https://raw.githubusercontent.com/Nikkhil16/Demo/main/global_package_catalog.json
```

That GitHub raw URL is the default, so you only need `--global-catalog-path` if you want to test a different registry. You can also override the default for a whole shell session:

```bash
export SKILL_ACQ_GLOBAL_CATALOG_URL="https://raw.githubusercontent.com/Nikkhil16/Demo/main/global_package_catalog.json"
```

Recommended hosted location:

- Use a separate GitHub repository for the global registry when you want the package list to be maintained independently from the `skill_acq` package code.
- Keep the registry as a plain JSON file. The current default expects it at the repo root as `global_package_catalog.json` in `https://github.com/Nikkhil16/Demo`.
- Serve it through GitHub raw content or GitHub Pages when clients need to fetch updates.
- Pin or version the registry schema with `global_catalog_schema_version` or `schema_version`.
- Include a `source` object for each package with `type`, `url`, and `manifest_path`. Use a stable `commit`, `tag`, or explicit `ref` when you want to pin the checkout. `default_branch` is treated as descriptive metadata; if no pin is supplied, Git clones the repository's actual default branch.
- Prefer pull requests for adding packages so entries can be reviewed for safety, requirements, and installability.

Keeping the global registry separate is a good fit for your flow: the robot can fetch a simple JSON package list while the `skill_acq` package keeps the code that validates, installs, and runs entries. The main requirement is to version the registry schema and review new entries carefully because a registry entry can cause code to be cloned and built.

## Hard Constraints To Put In Manifests

The current manifest schema now supports these hard-constraint groups:

- `platform_requirements`: OS, Ubuntu/platform tags, architecture, ROS distro, and similar platform facts.
- `system_requirements`: required commands, generic hardware tags, GPU flag, and notes.
- `robot_requirements`: robot types, required robot capabilities, sensors, actuators, TF frames, and notes.
- `hard_requirements.resource_requirements`: memory, free disk, accelerators, and GPU needs.
- `hard_requirements.runtime_requirements`: required environment variables, sudo/root needs, realtime kernel needs, required ROS nodes, topics, services, and actions.
- `hard_requirements.network_requirements`: internet requirement and outbound hosts.
- `hard_requirements.safety_requirements`: whether a physical robot is required, allowed robot modes, motion risk, e-stop requirement, and supervision expectation.
- `hard_requirements.data_requirements`: credentials or datasets required before execution.
- `input_requirements[].constraints`: input ranges, length limits, formats, units, choices, or semantic role.
- `action_servers[]`: action name, interface, goal/result/feedback fields, published/subscribed topics, services, TF frames, and side effects.

Only objectively checkable fields should be hard filters. Free-form notes are useful for the LLM and user review, but they should not silently block a package unless the selector can verify them.

The selector can verify required ROS graph entities from comma-separated environment variables when a manifest declares them:

```bash
export SKILL_ACQ_ROS_NODES="/camera_node,/planner"
export SKILL_ACQ_ROS_TOPICS="/image,/tf"
export SKILL_ACQ_ROS_SERVICES="/reset"
export SKILL_ACQ_ROS_ACTIONS="/navigate_to_pose"
```

For `hard_requirements.data_requirements`, `requires_credentials` should list required environment variable names and `requires_datasets` should list dataset paths that must exist locally. Safety checks fail closed for `requires_physical_robot` unless `--robot-mode physical` is provided, and for `requires_estop` unless `--robot-has-estop true` is provided.

## Selecting Without Running

To inspect the selected target:

```bash
ros2 run skill_acq select_ros_target.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string \
  --top-k 3
```

For machine-readable output:

```bash
ros2 run skill_acq select_ros_target.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string \
  --json \
  --top-k 1
```

Force deterministic catalog ranking:

```bash
ros2 run skill_acq select_ros_target.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string \
  --selection-backend catalog \
  --top-k 3
```

Force OpenAI selection:

```bash
ros2 run skill_acq select_ros_target.py \
  'reverse the string "hello"' \
  --set publish_topic=/rev_string \
  --selection-backend openai \
  --json \
  --top-k 1
```

## Installed Package Detection

`run_ros_target.py` automatically checks whether the selected package is already installed in its runner workspace before building. There is no separate flag for this.

The check is:

1. Confirm the runner overlay exists at `<runner_ws>/install/setup.bash`.
2. Source the runner overlay and run `ros2 pkg prefix <package_name>`.
3. Compare the install stamp in `<runner_ws>/.skill_acq/` with the staged package source.

This means it does not treat the source folder or an old install directory as "installed". A package source directory can exist while ROS still cannot use it, and a stale install can exist after the source has changed. The package is considered reusable only when the runner overlay can provide it and the source stamp still matches.

## Cloud Installs And Catalog Updates

When `run_ros_target.py` is called with `--source cloud`, it clones the repo, optionally fetches and checks out `--repo-ref`, builds the package, and updates the local catalog after a successful build. This makes newly installed cloud skills discoverable in later `skill_acq.py` runs.

Cloud installs require network access for `git clone` and `git fetch`. If a clone directory already exists, the runner verifies that its `origin` URL matches the requested repo before reusing it. Catalog updates preserve existing package entries from the current catalog and add the newly installed cloud package instead of rebuilding from only the runner script directory.

## Notes For Adding New Skills

To add another skill package:

1. Put a ROS package in the workspace or publish it as a cloneable repo.
2. Add a schema version 3 `package_runner.json` at the package or repo root.
3. Include discovery metadata, target metadata, required inputs, process startup commands, and client commands.
4. Rebuild the catalog:

```bash
ros2 run skill_acq build_package_catalog.py --root /path/to/workspace
```

5. Test selection:

```bash
ros2 run skill_acq select_ros_target.py 'your natural language request' --top-k 3
```

6. Test execution:

```bash
ros2 run skill_acq skill_acq.py 'your natural language request'
```

## Current Example

The hosted global catalog in `https://github.com/Nikkhil16/Demo` includes `reverse_string_action::reverse_string`, which:

- starts the `/reverse_string` action server,
- sends a string goal,
- receives the reversed string as a result,
- publishes the reversed string on the user-specified topic.

On a fresh machine where the local catalog is empty, the first run should select this global entry, clone `https://github.com/Nikkhil16/reverse_string_action`, build it in the runner workspace, and then add it to the local catalog for later runs.
