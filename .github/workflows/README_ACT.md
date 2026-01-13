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
../act/bin/act -W .github/workflows/dev_versions.yml --matrix python-version:3.14t --matrix os:ubuntu-latest -P  ubuntu-latest=catthehacker/ubuntu:act-24.04 --reuse
```

### 3. Run Specific Jobs

To run just the core tests without nightly builds:

```bash
../act/bin/act -W .github/workflows/dev_versions.yml --matrix python-version:3.12 -P  ubuntu-latest=catthehacker/ubuntu:act-latest dev_versions --artifact-server-path /tmp/artifacts --reuse
```

## Common Options

- -n : dry run
- -v : verbose
- --reuse

## This Project
### Benchmarks
`../act/bin/act -W .github/workflows/benchmarks.yml  -P  ubuntu-latest=catthehacker/ubuntu:act-24.04 --reuse`

### ASAN
`../act/bin/act -W .github/workflows/sanitizers.yml --matrix sanitizer:ASAN -P ubuntu-latest=catthehacker/ubuntu:act-24.04 --reuse`

### TSAN
`../act/bin/act -W .github/workflows/sanitizers.yml --matrix sanitizer:TSAN -P ubuntu-latest=catthehacker/ubuntu:act-24.04 --reuse`

