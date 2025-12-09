import os
import re
import shutil
import subprocess
import sys
import glob
import urllib.request
import zipfile
from pathlib import Path

from Cython.Build import cythonize
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py

from typing import Any

os.environ["CYTHON_FORCE_REGEN"] = "1"  # Slower but safer when moving submodules - always re-cythonize https://cython.readthedocs.io/en/latest/src/changes.html#alpha-11-2022-07-31

LINK_MODE = os.getenv("BAREDUCKDB_LINK_MODE", "dynamic")  # Dynamic linking against prebuilt .so
OPTIMIZATION_LEVEL = os.getenv("BAREDUCKDB_OPTIMIZATION", "balanced")

LATEST_DUCKDB_VERSION = "v1.4.3"

# The non-free-threaded builds will target this version. 
# The following two fields should match
PY_LIMITED_API_VERSION = 0x030c0000  # Python 3.12+
STABLE_PYTHON_VERSION = "cp312"

_IS_MACOS = sys.platform == "darwin"
_LIB_EXT = "dylib" if _IS_MACOS else "so"

_DUCKDB_LIB_DIR_NAME = f"duckdb_lib_{LATEST_DUCKDB_VERSION}"
_DUCKDB_LIB_DIR_PATH = Path(os.path.dirname(__file__))  / _DUCKDB_LIB_DIR_NAME

_DUCKDB_SHARED_LIB_PATH = _DUCKDB_LIB_DIR_PATH / f"libduckdb.{_LIB_EXT}"

_DUCKDB_SHARED_LIB = str(_DUCKDB_SHARED_LIB_PATH)
_DUCKDB_STATIC_LIB = str(_DUCKDB_LIB_DIR_PATH / "libduckdb_static.a")

_DUCKDB_LIB_DIR = str(_DUCKDB_LIB_DIR_PATH)

# Use submodule headers
_DUCKDB_INCLUDE_PATH = Path(os.path.dirname(__file__)) / "external/duckdb/src/include"
_DUCKDB_INCLUDE = str(_DUCKDB_INCLUDE_PATH)

# PyArrow C++ headers and libraries (or dataset module
try:
    import pyarrow
    PYARROW_INCLUDE = pyarrow.get_include()
    PYARROW_LIB_DIRS = pyarrow.get_library_dirs()
    ARROW_AVAILABLE = True
    print(f"PyArrow {pyarrow.__version__} found - dataset module will be built")
except ImportError:
    ARROW_AVAILABLE = False
    PYARROW_INCLUDE = None
    PYARROW_LIB_DIRS = None
    print("WARNING: PyArrow not found - dataset module will not be built")

def setup_compiler_cache():
    if shutil.which("sccache"):
        compiler_launcher = "sccache"
    elif shutil.which("ccache"):
        compiler_launcher = "ccache"
    else:
        compiler_launcher = None

    if compiler_launcher:
        print(f"{compiler_launcher=}")

        if "CC" not in os.environ:
            os.environ["CC"] = f"{compiler_launcher} gcc"
        if "CXX" not in os.environ:
            os.environ["CXX"] = f"{compiler_launcher} g++"
        print(f"Using CC={os.environ.get('CC')}, CXX={os.environ.get('CXX')}")
    else:
        print("Neither ccache nor sccache detected, no caching enabled")

    return compiler_launcher

def check_gcc_version(*, compiler_launcher, min_major=14, min_minor=0):
    """
    Note: This version is somewhat arbitrary: I used some new build flags from 14.0.
    Older GCC's will work, provided you remove the offending flags.

    On macOS, we skip this check as we use AppleClang to match DuckDB's prebuilt binaries.
    """

    # Skip GCC version check on macOS - we use AppleClang to match DuckDB
    if _IS_MACOS:
        print("macOS: Skipping GCC version check (using AppleClang to match DuckDB)")
        return

    cxx = os.environ.get("CXX", "g++")

    if compiler_launcher and cxx.startswith(compiler_launcher):
        cxx = cxx.split()[-1]

    try:
        # Get version from compiler
        result = subprocess.run(
            [cxx, "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        version_output = result.stdout

        # Parse version - works for both GCC and Clang masquerading as g++
        # GCC format: "g++ (GCC) 14.2.1 ..."
        # Clang format: "Apple clang version 15.0.0 ..."
        match = re.search(r'(?:gcc|g\+\+).*?(\d+)\.(\d+)', version_output, re.IGNORECASE)
        if not match:
            # Try Clang format
            match = re.search(r'clang version (\d+)\.(\d+)', version_output, re.IGNORECASE)

        if not match:
            print(f"Warning: Could not parse compiler version from: {version_output}")
            print("Proceeding anyway")
            return

        major = int(match.group(1))
        minor = int(match.group(2))

        if major < min_major or (major == min_major and minor < min_minor):
            raise RuntimeError(f"Failed compiler version check {major=}, {minor=}")

    except Exception as e:
        raise e



# DuckDB auto-download
def download_and_extract_duckdb():
    # TODO: Support nightlys

    
    if _DUCKDB_SHARED_LIB_PATH.exists():
        return
    else:
        print(f"{_DUCKDB_SHARED_LIB=} doesn't exist, downloading and extracting")

    if _IS_MACOS:
        url = f"https://install.duckdb.org/{LATEST_DUCKDB_VERSION}/libduckdb-osx-universal.zip"
    elif sys.platform == "linux":
        url = f"https://install.duckdb.org/{LATEST_DUCKDB_VERSION}/libduckdb-linux-amd64.zip"
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")

    _DUCKDB_LIB_DIR_PATH.mkdir(exist_ok=True)
    
    # Download to temporary file
    zip_path = _DUCKDB_LIB_DIR_PATH / "libduckdb.zip"

    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(_DUCKDB_LIB_DIR)

    assert _DUCKDB_SHARED_LIB_PATH.exists()

    print(f"Downloaded & extracted successfully: {zip_path=}")



assert _DUCKDB_INCLUDE_PATH.exists(), f"DuckDB headers not found at {_DUCKDB_INCLUDE_PATH}. Please initialize git submodules."

compiler_launcher = setup_compiler_cache()

check_gcc_version(compiler_launcher=compiler_launcher, min_major=14, min_minor=0)

download_and_extract_duckdb()


def copy_library_to_package():
    """
    Copy libduckdb.so into src/bareduckdb/_libs/ for wheel distribution.
    """
    package_libs_dir = Path(os.path.dirname(__file__)) / "src" / "bareduckdb" / "_libs"
    package_libs_dir.mkdir(exist_ok=True)

    target_lib = package_libs_dir / f"libduckdb.{_LIB_EXT}"
    shutil.copy2(_DUCKDB_SHARED_LIB, target_lib)

    return package_libs_dir


package_libs_dir = copy_library_to_package()

if LINK_MODE == "static":
    # Static linking with prebuilt libduckdb_static.a (GCC 12)
    # This avoids GCC 14 ABI incompatibility with prebuilt .so
    # Use --whole-archive to ensure all symbols are included
    extra_objects = []
    libraries = []
    base_link_args = [
        "-Wl,--whole-archive",
        str(_DUCKDB_STATIC_LIB),
        "-Wl,--no-whole-archive",
        "-lpthread",
        "-ldl"
    ]
elif LINK_MODE == "dynamic":
    # Dynamic linking: use -L and -l flags, not extra_objects
    extra_objects = []
    libraries = ["duckdb"]
    # Use $ORIGIN/@loader_path rpath for portable wheel - extensions in core/impl/ -> _libs/
    # macOS uses @loader_path, Linux uses $ORIGIN
    # Add library search path for build-time linking
    rpath_prefix = "@loader_path" if _IS_MACOS else "$ORIGIN"
    base_link_args = [
        "-lpthread",
        "-ldl",
        f"-Wl,-rpath,{rpath_prefix}/../../_libs",
        f"-L{package_libs_dir}",
    ]
else:
    raise ValueError(f"Unexpected {LINK_MODE=}")

IS_FREE_THREADED = hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()

if IS_FREE_THREADED:
    USE_LIMITED_API = False
    print(f"Building for free-threaded Python {sys.version_info.major}.{sys.version_info.minor}")
else:
    USE_LIMITED_API = True
    
    print(f"Building with {USE_LIMITED_API=}: stable ABI for {STABLE_PYTHON_VERSION=}")

if OPTIMIZATION_LEVEL == "debug":
    # Use -O1 for ASAN - provides better bug detection than -O0
    # -fno-optimize-sibling-calls: preserves stack frames for better ASAN traces
    compile_args = ["-std=c++17", "-O1", "-g", "-Wall", "-fno-optimize-sibling-calls"]
    link_args = base_link_args.copy()
    define_macros = [("CYTHON_USE_MODULE_STATE", "1")]
    # Don't use Py_LIMITED_API in debug mode
    # if USE_LIMITED_API:
    #     define_macros.append(("Py_LIMITED_API", f"{PY_LIMITED_API_VERSION:#x}"))
    cython_directives = {
        "language_level": 3,
        "embedsignature": True,
        "profile": True,
        "linetrace": True,
    }
elif OPTIMIZATION_LEVEL == "aggressive":
    # Aggressive is intended solely for performance experiments
    compile_args = ["-std=c++17", "-O3", "-Wall", "-flto=auto", "-fvisibility=hidden", "-DNDEBUG"] # "-march=native",
    link_args = base_link_args + ["-flto=auto"]
    define_macros = [
        ("CYTHON_USE_MODULE_STATE", "1"),
        ("NDEBUG", "1"),
    ]
    if USE_LIMITED_API:
        define_macros.append(("Py_LIMITED_API", f"{PY_LIMITED_API_VERSION:#x}"))
    cython_directives = {
        "language_level": 3,
        "boundscheck": False,
        "wraparound": False,
        "initializedcheck": False,
        "nonecheck": False,
        "cdivision": True,
        "embedsignature": False,
        "profile": False,
        "linetrace": False,
    }
else:  # default
    compile_args = ["-std=c++17", "-O3", "-Wall", "-flto=auto"]
    link_args = base_link_args + ["-flto=auto"]
    define_macros = [("CYTHON_USE_MODULE_STATE", "1")]
    if USE_LIMITED_API:
        define_macros.append(("Py_LIMITED_API", f"{PY_LIMITED_API_VERSION:#x}"))
    cython_directives = {
        "language_level": 3,
        "boundscheck": False,
        "wraparound": False,
        "cdivision": True,
        "nonecheck": False,
        "embedsignature": True,
        "profile": False,
        "linetrace": False,
    }

# Some tweaks to support TSAN/ASAN sanitizers, so they take precedence over our defaults
env_cflags = os.getenv("CFLAGS", "").split()
env_cxxflags = os.getenv("CXXFLAGS", "").split()
env_ldflags = os.getenv("LDFLAGS", "").split()
if env_cflags or env_cxxflags:
    extra_env_flags = env_cxxflags if env_cxxflags else env_cflags
    compile_args.extend(extra_env_flags)
    print(f"Added environment compile flags: {' '.join(extra_env_flags)}")
if env_ldflags:
    link_args.extend(env_ldflags)
    print(f"Added environment link flags: {' '.join(env_ldflags)}")

print(f"Building with link mode: {LINK_MODE}")
print(f"Building with optimization level: {OPTIMIZATION_LEVEL}")
print(f"Compile args: {' '.join(compile_args)}")
print(f"Link args: {' '.join(link_args)}")


# Core extensions - only dependency is duckdb
# Some other experiments use pyarrow, but this code should never require any other lib at build.

def get_args(name, sources) -> dict[str, Any]:

    args = {
        "name": name,
        "sources": sources,
        "include_dirs": ["src/bareduckdb/core/impl", _DUCKDB_INCLUDE],
        "extra_objects": extra_objects,
        "libraries": libraries,
        "library_dirs": [str(package_libs_dir)],
        "runtime_library_dirs": [],
        "extra_compile_args": compile_args,
        "extra_link_args": link_args,
        "language": "c++",
        "define_macros": define_macros,
    }

    if USE_LIMITED_API:
        args["py_limited_api"] = True

    return args

connection_kwargs = get_args(
    name="bareduckdb.core.impl.connection",
    sources=["src/bareduckdb/core/impl/connection.pyx"],
)

result_kwargs = get_args(
    name="bareduckdb.core.impl.result",
    sources=["src/bareduckdb/core/impl/result.pyx"],
)

python_to_value_kwargs = get_args(
    name="bareduckdb.core.impl.python_to_value",
    sources=["src/bareduckdb/core/impl/python_to_value.pyx"],
)

core_extensions = [
    Extension(**connection_kwargs),
    Extension(**result_kwargs),
    Extension(**python_to_value_kwargs),
]

# Dataset extension with PyArrow support
dataset_extensions = []
if ARROW_AVAILABLE:
    def find_pyarrow_libraries():
        if not PYARROW_LIB_DIRS:
            return None, ["arrow_python", "arrow"]

        lib_dir = PYARROW_LIB_DIRS[0]

        # Try unversioned symlinks
        arrow_lib = os.path.join(lib_dir, f"libarrow.{_LIB_EXT}")
        arrow_python_lib = os.path.join(lib_dir, f"libarrow_python.{_LIB_EXT}")

        if os.path.exists(arrow_lib) and os.path.exists(arrow_python_lib):
            print(f"Using PyArrow library names: -larrow -larrow_python")
            return None, ["arrow_python", "arrow"]

        # Versioned libraries
        if _IS_MACOS:
            arrow_libs = glob.glob(os.path.join(lib_dir, "libarrow.*.dylib"))
            arrow_python_libs = glob.glob(os.path.join(lib_dir, "libarrow_python.*.dylib"))
        else:
            arrow_libs = glob.glob(os.path.join(lib_dir, "libarrow.so.*"))
            arrow_python_libs = glob.glob(os.path.join(lib_dir, "libarrow_python.so.*"))

        if arrow_libs and arrow_python_libs:
            arrow_lib = sorted(arrow_libs, key=lambda x: x.count("."), reverse=True)[0]
            arrow_python_lib = sorted(arrow_python_libs, key=lambda x: x.count("."), reverse=True)[0]
            print(f"Using versioned PyArrow libraries: {os.path.basename(arrow_lib)}, {os.path.basename(arrow_python_lib)}")
            return [arrow_lib, arrow_python_lib], None

        return None, ["arrow_python", "arrow"]

    pyarrow_lib_files, pyarrow_libraries = find_pyarrow_libraries()

    # Configure dataset extension
    if pyarrow_lib_files:
        dataset_extra_objects = extra_objects + pyarrow_lib_files
        dataset_libraries = libraries
        dataset_library_dirs = [str(package_libs_dir)]
    else:
        dataset_extra_objects = extra_objects
        dataset_libraries = libraries + pyarrow_libraries
        dataset_library_dirs = [str(package_libs_dir)] + (PYARROW_LIB_DIRS if PYARROW_LIB_DIRS else [])
    
    dataset_runtime_dirs = []  # find PyArrow in venv

    # Dataset extension needs additional rpath to find PyArrow at runtime
    dataset_link_args = link_args.copy()
    if _IS_MACOS:
        # On macOS, add @loader_path to search for Arrow libs in pyarrow package
        dataset_link_args.append("-Wl,-rpath,@loader_path/../../../pyarrow")
    else:
        # On Linux, add $ORIGIN rpath for Arrow libs
        dataset_link_args.append("-Wl,-rpath,$ORIGIN/../../../pyarrow")

    dataset_kwargs = {
        "name": "bareduckdb.dataset.impl.dataset",
        "sources": ["src/bareduckdb/dataset/impl/dataset.pyx"],
        "include_dirs": [
            "src/bareduckdb/dataset/impl",
            "src/bareduckdb/core/impl",  # For cpp_helpers.hpp
            _DUCKDB_INCLUDE,
            PYARROW_INCLUDE,
        ],
        "extra_objects": dataset_extra_objects,
        "libraries": dataset_libraries,
        "library_dirs": dataset_library_dirs,
        "runtime_library_dirs": dataset_runtime_dirs,
        "extra_compile_args": compile_args,
        "extra_link_args": dataset_link_args,
        "language": "c++",
        "define_macros": define_macros,
    }

    if USE_LIMITED_API:
        dataset_kwargs["py_limited_api"] = True

    dataset_extensions.append(Extension(**dataset_kwargs))
    print(f"Dataset extension configured successfully")

# Combine all extensions
extensions = core_extensions + dataset_extensions


nthreads = int(os.getenv("CYTHON_NTHREADS", os.cpu_count() or 1))
print(f"Cythonizing with {nthreads} parallel jobs")
extensions = cythonize(
    extensions,
    compiler_directives=cython_directives,
    nthreads=nthreads,
)

# Validate that the library file exists
if LINK_MODE == "static":
    selected_lib = _DUCKDB_STATIC_LIB
    if not os.path.exists(selected_lib):
        raise FileNotFoundError(
            f"DuckDB library not found: {selected_lib}\n"
            f"Link mode: {LINK_MODE}\n"
            f"Please ensure the library is present in {_DUCKDB_LIB_DIR}"
        )
    print(f"Using DuckDB static lib: {selected_lib}")
elif LINK_MODE == "dynamic":
    # Check that the library exists in the package _libs directory
    lib_path = package_libs_dir / f"libduckdb.{_LIB_EXT}"
    if not lib_path.exists():
        raise FileNotFoundError(
            f"DuckDB library not found: {lib_path}\n"
            f"Link mode: {LINK_MODE}\n"
            f"Expected library in: {package_libs_dir}"
        )
    print(f"Using DuckDB lib: {lib_path}")

# Print debug info
print(f"Using DuckDB include: {_DUCKDB_INCLUDE}")


class ParallelBuildExt(build_ext):

    def build_extensions(self):
        max_jobs = int(os.getenv("MAX_JOBS", os.cpu_count() or 1))

        self.parallel = max_jobs
        print(f"Building C++ extensions with {max_jobs} parallel jobs")

        build_ext.build_extensions(self)

    def run(self):
        # Build extensions first
        super().run()

        # Add DuckDB version to _version.py
        self.add_duckdb_version()

    def add_duckdb_version(self):
        # editable: write to source dir, regular: write to build dir
        # Note: This also serves as a sanity check on the build
        source_version_file = os.path.join('src', 'bareduckdb', '_version.py')

        build_py = self.get_finalized_command('build_py')
        build_lib = build_py.build_lib
        build_version_file = os.path.join(build_lib, 'bareduckdb', '_version.py')

        if os.path.exists(build_version_file):
            version_file = build_version_file
            sys_path_dir = os.path.abspath(build_lib)
        elif os.path.exists(source_version_file):
            version_file = source_version_file
            sys_path_dir = os.path.abspath('src')
        else:
            raise ValueError("Unable to update _version file")

        duckdb_version = None
        import sys
        old_path = sys.path.copy()
        sys.path.insert(0, sys_path_dir)

        try:
            from bareduckdb.compat.connection_compat import Connection
            con = Connection()
            con.execute("SELECT version()")
            result = con.fetchone()
            duckdb_version = result[0] if result else None
            con.close()
            print(f"Successfully queried DuckDB version: {duckdb_version}")
        finally:
            sys.path = old_path
    

        # Fallback to LATEST_DUCKDB_VERSION if dynamic query failed
        if not duckdb_version:
            duckdb_version = LATEST_DUCKDB_VERSION.lstrip('v')

        with open(version_file, 'r') as f:
            content = f.read()
            if '__duckdb_version__' in content:
                return

        with open(version_file, 'a') as f:
            f.write(f'\n__duckdb_version__ = "{duckdb_version}"\n')

    def copy_extensions_to_source(self):
        """Copy extensions to source directory"""
        build_ext.copy_extensions_to_source(self)

        # For stable ABI builds, ensure wheels only contain .abi3.so extensions
        if USE_LIMITED_API:
            for pattern in ["src/bareduckdb/**/*.cpython-*.so", "build/**/*.cpython-*.so"]:
                for version_specific_so in glob.glob(pattern, recursive=True):
                    if os.path.exists(version_specific_so):
                        os.remove(version_specific_so)
                        print(f"Removed version-specific extension: {version_specific_so}")


# Only use py_limited_api for non-free-threaded builds
# TODO: Update when PEP-803 lands
bdist_wheel_options = {}
if not hasattr(sys, '_is_gil_enabled') or sys._is_gil_enabled():  # type: ignore[attr-defined]
    bdist_wheel_options["py_limited_api"] = STABLE_PYTHON_VERSION

setup(
    ext_modules=extensions,
    cmdclass={"build_ext": ParallelBuildExt},
    options={"bdist_wheel": bdist_wheel_options} if bdist_wheel_options else {},
)
