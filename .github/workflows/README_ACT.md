# Running GitHub Actions Locally with nektos/act

This guide explains how to run GitHub Actions workflows locally using [nektos/act](https://github.com/nektos/act), which allows you to test workflows before pushing to GitHub.

## Prerequisites

- Docker must be installed and running
- Install the `act` binary, `curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash`

## Quick Start

### List Available Workflows

```bash
../act/bin/act --list
```

### Run the Dev Versions Workflow

To run the complete dev_versions workflow:

```bash
../act/bin/act -W .github/workflows/dev_versions.yml --matrix python-version:3.15t --matrix os:ubuntu-latest -P  ubuntu-latest=catthehacker/ubuntu:act-24.04 --reuse
```

`--matrix` values have to exist in the workflow. `dev_versions.yml` currently defines
`python-version` as `3.14` and `3.15t` only, across `ubuntu-latest`, `macos-latest` and
`windows-latest`; `3.12` and `3.14t` are not in it. Re-read the matrix before copying a
command from here.

### 3. Run Specific Jobs

`dev_versions.yml` has four jobs: `dev_versions`, `musl`, `graalpy` and `benchmarks`. The
last two are calls into the reusable workflows `graalpy.yml` and `benchmarks.yml`, and all
three are chained behind `dev_versions` with `needs:`. To run just the first:

```bash
../act/bin/act -W .github/workflows/dev_versions.yml --matrix python-version:3.14 -P  ubuntu-latest=catthehacker/ubuntu:act-latest -j dev_versions --artifact-server-path /tmp/artifacts --reuse
```

### Run the GraalPy job on its own

`graalpy.yml` also accepts `workflow_dispatch`, so it runs standalone without the CPython
matrix in front of it. The stock runner image has no cmake, so build a derived one first.

```bash
../act/bin/act -W .github/workflows/graalpy.yml -j graalpy   -P ubuntu-latest=<image with cmake, ninja and build-essential> --reuse
```

## Common Options

- -n : dry run
- -v : verbose
- --reuse

## Known act gaps

Three things bite locally that do not bite on a GitHub runner:

- **`catthehacker/ubuntu:act-24.04` has no `cmake`**, so any job that invokes CMake outside
  scikit-build-core's own bootstrap dies with `cmake: command not found`. Derive an image:
  `FROM catthehacker/ubuntu:act-24.04` plus
  `apt-get install -y cmake ninja-build build-essential`, then pass it with
  `-P ubuntu-latest=<that image>`.
- **`--reuse` reuses the container, not the image.** After changing the `-P` image you must
  `docker rm -f` the existing `act-...` container or act silently keeps the old one.
- **In a git worktree, `setuptools-scm` cannot see `.git`**, because it is a file pointing at
  a gitdir outside the bind mount, and the build fails with "unable to detect version".
  Pass `--env SETUPTOOLS_SCM_PRETEND_VERSION=0.1.dev0`. `actions/checkout` on a real runner
  produces a normal `.git` directory, so this is local-only.
