"""Tests for _duckdb_runtime: resolution order, cache naming, no-download mode."""

from __future__ import annotations

import os

import pytest

from bareduckdb import _duckdb_runtime as rt
from bareduckdb import _duckdb_fetch as fetch

LIB_NAME = fetch.shared_lib_name()


def _touch_lib(directory, name=LIB_NAME):
    directory.mkdir(parents=True, exist_ok=True)
    lib = directory / name
    lib.write_bytes(b"not a real library")
    return lib


def test_cache_dir_for_includes_channel_version_and_artifact():
    cache_dir = rt.cache_dir_for("preview", "latest", "windows-amd64")
    name = cache_dir.name
    assert "preview" in name
    assert "latest" in name
    assert "windows-amd64" in name
    assert cache_dir.parent == rt.user_cache_root()


def test_cache_dir_for_differs_across_versions():
    a = rt.cache_dir_for("preview", "latest", "windows-amd64")
    b = rt.cache_dir_for("stable", "1.9.0", "windows-amd64")
    assert a != b


def test_env_var_directory_wins_over_cache(tmp_path, monkeypatch):
    env_dir = tmp_path / "env_lib"
    env_lib = _touch_lib(env_dir)

    cache_dir = tmp_path / "cache"
    _touch_lib(cache_dir)
    in_tree = tmp_path / "in_tree_missing"

    monkeypatch.setenv(rt.ENV_LIB, str(env_dir))
    monkeypatch.delenv(rt.ENV_NO_DOWNLOAD, raising=False)
    monkeypatch.setattr(rt, "_in_tree_libs_dir", lambda: in_tree)
    monkeypatch.setattr(rt, "cache_dir_for", lambda *a, **k: cache_dir)

    resolved = rt.resolve_duckdb_lib()
    assert resolved == env_lib


def test_env_var_file_path_is_accepted_directly(tmp_path, monkeypatch):
    env_dir = tmp_path / "env_lib"
    env_lib = _touch_lib(env_dir)

    monkeypatch.setenv(rt.ENV_LIB, str(env_lib))

    resolved = rt.resolve_duckdb_lib()
    assert resolved == env_lib


def test_env_var_missing_path_raises_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv(rt.ENV_LIB, str(tmp_path / "does_not_exist"))

    with pytest.raises(rt.DuckDBLibraryNotFoundError, match=rt.ENV_LIB):
        rt.resolve_duckdb_lib()


def test_in_tree_libs_wins_over_cache_when_no_env_var(tmp_path, monkeypatch):
    in_tree = tmp_path / "in_tree"
    in_tree_lib = _touch_lib(in_tree)

    cache_dir = tmp_path / "cache"
    _touch_lib(cache_dir)

    monkeypatch.delenv(rt.ENV_LIB, raising=False)
    monkeypatch.delenv(rt.ENV_NO_DOWNLOAD, raising=False)
    monkeypatch.setattr(rt, "_in_tree_libs_dir", lambda: in_tree)
    monkeypatch.setattr(rt, "cache_dir_for", lambda *a, **k: cache_dir)

    resolved = rt.resolve_duckdb_lib()
    assert resolved == in_tree_lib


def test_cache_wins_when_no_env_var_and_no_in_tree(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    cache_lib = _touch_lib(cache_dir)
    in_tree = tmp_path / "in_tree_missing"

    monkeypatch.delenv(rt.ENV_LIB, raising=False)
    monkeypatch.delenv(rt.ENV_NO_DOWNLOAD, raising=False)
    monkeypatch.setattr(rt, "_in_tree_libs_dir", lambda: in_tree)
    monkeypatch.setattr(rt, "cache_dir_for", lambda *a, **k: cache_dir)

    resolved = rt.resolve_duckdb_lib()
    assert resolved == cache_lib


def test_no_download_raises_clear_actionable_error(tmp_path, monkeypatch):
    in_tree = tmp_path / "in_tree_missing"
    cache_dir = tmp_path / "cache_missing"

    monkeypatch.delenv(rt.ENV_LIB, raising=False)
    monkeypatch.setenv(rt.ENV_NO_DOWNLOAD, "1")
    monkeypatch.setattr(rt, "_in_tree_libs_dir", lambda: in_tree)
    monkeypatch.setattr(rt, "cache_dir_for", lambda *a, **k: cache_dir)

    def _boom(*a, **k):
        raise AssertionError("download must not be attempted when BAREDUCKDB_NO_DOWNLOAD is set")

    monkeypatch.setattr(rt, "_download_into_cache", _boom)

    with pytest.raises(rt.DuckDBLibraryNotFoundError) as excinfo:
        rt.resolve_duckdb_lib()

    message = str(excinfo.value)
    assert rt.ENV_NO_DOWNLOAD in message
    assert rt.ENV_LIB in message
    assert str(cache_dir) in message
    assert "install_duckdb" in message


def test_no_download_error_names_a_set_but_wrong_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv(rt.ENV_LIB, str(tmp_path / "wrong"))
    # The invalid env var should fail before BAREDUCKDB_NO_DOWNLOAD is even checked.
    monkeypatch.setenv(rt.ENV_NO_DOWNLOAD, "1")

    with pytest.raises(rt.DuckDBLibraryNotFoundError, match=rt.ENV_LIB):
        rt.resolve_duckdb_lib()


@pytest.mark.skipif(
    os.environ.get("BAREDUCKDB_TEST_LIVE_DOWNLOAD") != "1",
    reason="live download test opts in via BAREDUCKDB_TEST_LIVE_DOWNLOAD=1",
)
def test_live_download_opt_in(tmp_path, monkeypatch):
    """Real network download, gated behind an explicit opt-in env var."""
    artifact = fetch.duckdb_artifact(None)
    lib = rt.download_lib(tmp_path, artifact=artifact, lib_name=LIB_NAME)
    assert lib.is_file()
    assert lib.stat().st_size > 0
