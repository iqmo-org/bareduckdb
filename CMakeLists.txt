cmake_minimum_required(VERSION 3.21...4.3)

project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES C)

# ---------------------------------------------------------------------------
# Options. Each falls back to its BAREDUCKDB_* environment variable for its
# CACHE default (only consulted on the *first* configure of a build dir, same
# as any CACHE default), so the old setup.py contract (e.g.
# BAREDUCKDB_OPTIMIZATION=debug for the sanitizer workflow) keeps working
# under `pip install --no-build-isolation` without extra CMake args.
# tools/fetch_duckdb.py reads the artifact/target-machine variables itself too.
# ---------------------------------------------------------------------------
if(DEFINED ENV{BAREDUCKDB_DUCKDB_CHANNEL})
  set(_bdb_channel_default "$ENV{BAREDUCKDB_DUCKDB_CHANNEL}")
else()
  set(_bdb_channel_default "preview")
endif()
set(BAREDUCKDB_DUCKDB_CHANNEL "${_bdb_channel_default}"
    CACHE STRING "DuckDB download channel: preview | stable")

if(DEFINED ENV{BAREDUCKDB_DUCKDB_VERSION})
  set(_bdb_version_default "$ENV{BAREDUCKDB_DUCKDB_VERSION}")
else()
  set(_bdb_version_default "latest")
endif()
set(BAREDUCKDB_DUCKDB_VERSION "${_bdb_version_default}"
    CACHE STRING "Version tag, used only by the stable channel")

set(BAREDUCKDB_DUCKDB_ARTIFACT "$ENV{BAREDUCKDB_DUCKDB_ARTIFACT}"
    CACHE STRING "Override the platform artifact name, e.g. linux-arm64-musl")

set(BAREDUCKDB_TARGET_MACHINE "$ENV{BAREDUCKDB_TARGET_MACHINE}"
    CACHE STRING "Override the target machine when cross-compiling")

set(BAREDUCKDB_DUCKDB_DIR "$ENV{BAREDUCKDB_DUCKDB_DIR}"
    CACHE PATH "Use an already-extracted DuckDB directory and skip the download")

if(DEFINED ENV{BAREDUCKDB_OPTIMIZATION})
  set(_bdb_optimization_default "$ENV{BAREDUCKDB_OPTIMIZATION}")
else()
  set(_bdb_optimization_default "balanced")
endif()
set(BAREDUCKDB_OPTIMIZATION "${_bdb_optimization_default}"
    CACHE STRING "balanced | aggressive | debug")

set(BAREDUCKDB_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(BAREDUCKDB_IMPL "${BAREDUCKDB_SRC}/bareduckdb/capi/impl")
set(BAREDUCKDB_INCLUDE "${BAREDUCKDB_SRC}/bareduckdb/capi/include")
set(BAREDUCKDB_LIBS "${BAREDUCKDB_SRC}/bareduckdb/_libs")

# ---------------------------------------------------------------------------
# Guard: a stale in-tree extension module would be copied into the wheel by
# wheel.packages and would ship alongside the one CMake just built. This is the
# artifact-clobbering failure from the setup.py build, inverted. Fail loudly.
# ---------------------------------------------------------------------------
file(GLOB_RECURSE _stray_ext
     "${BAREDUCKDB_SRC}/bareduckdb/*.pyd"
     "${BAREDUCKDB_SRC}/bareduckdb/*.so"
     "${BAREDUCKDB_SRC}/bareduckdb/*.dylib")
list(FILTER _stray_ext EXCLUDE REGEX "/_libs/")
if(_stray_ext)
  message(FATAL_ERROR
    "In-tree extension modules found under src/bareduckdb/. These are left over "
    "from a `build_ext --inplace` build and would be copied into the wheel.\n"
    "Delete them and rebuild:\n  ${_stray_ext}")
endif()

# ---------------------------------------------------------------------------
# Python. SKBUILD_SABI_COMPONENT is "Development.SABIModule" when the limited
# API is in play and the empty string otherwise, including on every
# free-threaded interpreter, so this one line covers both wheels.
# ---------------------------------------------------------------------------
find_package(
  Python
  COMPONENTS Interpreter Development.Module ${SKBUILD_SABI_COMPONENT}
  REQUIRED)

include(UseCython)

# ---------------------------------------------------------------------------
# Free-threading detection, queried from the interpreter itself rather than a
# CMake variable. There is no `Python_FREE_THREADED` variable in FindPython
# (checked: absent from every FindPython module shipped with this CMake, 4.3.3).
# Some recent FindPython versions define Py_GIL_DISABLED on the Python::Module
# target automatically when the found library is the *t* import library, but
# that is version-dependent and unverified across the CI matrix's CMake
# versions, so this project sets it explicitly and unconditionally on Windows
# free-threaded builds rather than relying on it.
# ---------------------------------------------------------------------------
execute_process(
  COMMAND "${Python_EXECUTABLE}" -c
          "import sysconfig, sys; sys.stdout.write('1' if sysconfig.get_config_var('Py_GIL_DISABLED') else '0')"
  OUTPUT_VARIABLE BAREDUCKDB_FREE_THREADED
  OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Free-threaded interpreter: ${BAREDUCKDB_FREE_THREADED}")

# ---------------------------------------------------------------------------
# DuckDB shared library. tools/fetch_duckdb.py owns artifact naming, musl
# detection, download with retry, tar.gz and zip extraction, and the ELF/PE
# arch check. It writes a small CMake fragment we include back.
# ---------------------------------------------------------------------------
set(_fetch_cmd
    "${Python_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/tools/fetch_duckdb.py"
    --channel "${BAREDUCKDB_DUCKDB_CHANNEL}"
    --version "${BAREDUCKDB_DUCKDB_VERSION}"
    --cache-dir "${CMAKE_CURRENT_SOURCE_DIR}/.duckdb-cache"
    --cmake-out "${CMAKE_CURRENT_BINARY_DIR}/duckdb_lib.cmake")
if(BAREDUCKDB_DUCKDB_ARTIFACT)
  list(APPEND _fetch_cmd --artifact "${BAREDUCKDB_DUCKDB_ARTIFACT}")
endif()
if(BAREDUCKDB_TARGET_MACHINE)
  list(APPEND _fetch_cmd --target-machine "${BAREDUCKDB_TARGET_MACHINE}")
endif()
if(BAREDUCKDB_DUCKDB_DIR)
  list(APPEND _fetch_cmd --use-dir "${BAREDUCKDB_DUCKDB_DIR}")
endif()

execute_process(
  COMMAND ${_fetch_cmd}
  RESULT_VARIABLE _fetch_rc
  COMMAND_ECHO STDOUT
  ECHO_OUTPUT_VARIABLE
  ECHO_ERROR_VARIABLE)
if(NOT _fetch_rc EQUAL 0)
  message(FATAL_ERROR "tools/fetch_duckdb.py failed with exit code ${_fetch_rc}")
endif()

# Defines DUCKDB_ARTIFACT, DUCKDB_SHARED_LIB, DUCKDB_SHARED_LIB_NAME,
# DUCKDB_IMPORT_LIB (Windows only), DUCKDB_LIB_DIR.
include("${CMAKE_CURRENT_BINARY_DIR}/duckdb_lib.cmake")
message(STATUS "DuckDB artifact: ${DUCKDB_ARTIFACT}")
message(STATUS "DuckDB library:  ${DUCKDB_SHARED_LIB}")

# The runtime library ships inside the package at bareduckdb/_libs/. Copying it
# into the source tree, exactly as setup.py:260-273 did, gives one path into the
# wheel (via wheel.packages) and keeps editable installs working unchanged.
file(MAKE_DIRECTORY "${BAREDUCKDB_LIBS}")
file(COPY_FILE "${DUCKDB_SHARED_LIB}"
     "${BAREDUCKDB_LIBS}/${DUCKDB_SHARED_LIB_NAME}"
     ONLY_IF_DIFFERENT)

add_library(duckdb::duckdb SHARED IMPORTED)
set_target_properties(duckdb::duckdb PROPERTIES
  IMPORTED_LOCATION "${DUCKDB_SHARED_LIB}")
if(WIN32)
  set_target_properties(duckdb::duckdb PROPERTIES
    IMPORTED_IMPLIB "${DUCKDB_IMPORT_LIB}")
endif()

# ---------------------------------------------------------------------------
# Cython compiler directives. Set in exactly one place, applied to every module.
# ---------------------------------------------------------------------------
set(BAREDUCKDB_CYTHON_ARGS
    -3
    -X language_level=3
    # Without this the Py_mod_gil slot declares Py_MOD_GIL_USED and the
    # interpreter silently re-enables the GIL on first import. setup.py never
    # set it; setup_capi.py did. It is not optional here.
    -X freethreading_compatible=True
    -X boundscheck=False
    -X wraparound=False
    -X nonecheck=False
    -X cdivision=True
    -X embedsignature=True
    -X profile=False
    -X linetrace=False)
if(BAREDUCKDB_OPTIMIZATION STREQUAL "debug")
  set(BAREDUCKDB_CYTHON_ARGS
      -3 -X language_level=3 -X freethreading_compatible=True
      -X embedsignature=True -X profile=True -X linetrace=True)
endif()

# ---------------------------------------------------------------------------
# One extension per .pyx under capi/impl. CONFIGURE_DEPENDS re-runs configure
# when the glob result changes, so result.pyx and arrow.pyx need no edit here.
# ---------------------------------------------------------------------------
file(GLOB BAREDUCKDB_PYX CONFIGURE_DEPENDS "${BAREDUCKDB_IMPL}/*.pyx")
if(NOT BAREDUCKDB_PYX)
  message(FATAL_ERROR "No .pyx files found under ${BAREDUCKDB_IMPL}")
endif()

foreach(_pyx IN LISTS BAREDUCKDB_PYX)
  cmake_path(GET _pyx STEM _name)

  cython_transpile("${_pyx}"
    LANGUAGE C
    MODULE_NAME "bareduckdb.capi.impl.${_name}"
    INCLUDE_DIRECTORIES "${BAREDUCKDB_SRC}"
    CYTHON_ARGS ${BAREDUCKDB_CYTHON_ARGS}
    OUTPUT_VARIABLE _generated_c)

  if(SKBUILD_SABI_VERSION)
    python_add_library(${_name} MODULE WITH_SOABI
                       USE_SABI ${SKBUILD_SABI_VERSION} "${_generated_c}")
    # Cython requires per-module state under the limited API
    target_compile_definitions(${_name} PRIVATE CYTHON_USE_MODULE_STATE=1)
  else()
    python_add_library(${_name} MODULE WITH_SOABI "${_generated_c}")
  endif()

  target_include_directories(${_name} PRIVATE
    "${BAREDUCKDB_INCLUDE}"
    "${BAREDUCKDB_IMPL}")

  target_link_libraries(${_name} PRIVATE duckdb::duckdb)

  # Windows free-threaded pyconfig.h does not define Py_GIL_DISABLED; it is
  # shared with the GIL build. setuptools supplied it
  # (_distutils/command/build_ext.py:357). Some newer FindPython versions add it
  # automatically to the Python::Module target when the *t* import library is
  # found, but that is not guaranteed across the CMake versions this project
  # builds with, so it is set explicitly here regardless. Without it Cython
  # compiles CYTHON_COMPILING_IN_CPYTHON_FREETHREADING as 0 and drops the
  # Py_mod_gil slot entirely.
  if(WIN32 AND BAREDUCKDB_FREE_THREADED STREQUAL "1")
    target_compile_definitions(${_name} PRIVATE Py_GIL_DISABLED=1)
  endif()

  if(MSVC)
    target_compile_options(${_name} PRIVATE /W3)
    if(BAREDUCKDB_OPTIMIZATION STREQUAL "aggressive")
      target_compile_options(${_name} PRIVATE /GL)
      target_link_options(${_name} PRIVATE /LTCG)
    endif()
  else()
    target_compile_options(${_name} PRIVATE -Wall)
    if(BAREDUCKDB_OPTIMIZATION STREQUAL "aggressive")
      target_compile_options(${_name} PRIVATE -flto=auto -fvisibility=hidden)
      target_link_options(${_name} PRIVATE -flto=auto)
    endif()
  endif()

  # Extension sits at bareduckdb/capi/impl/, library at bareduckdb/_libs/.
  # Two levels up, same depth as the old bareduckdb/core/impl/ layout.
  if(APPLE)
    set_target_properties(${_name} PROPERTIES
      INSTALL_RPATH "@loader_path/../../_libs"
      BUILD_WITH_INSTALL_RPATH ON
      INSTALL_RPATH_USE_LINK_PATH OFF)
  elseif(UNIX)
    set_target_properties(${_name} PROPERTIES
      INSTALL_RPATH "$ORIGIN/../../_libs"
      BUILD_WITH_INSTALL_RPATH ON
      INSTALL_RPATH_USE_LINK_PATH OFF)
  endif()

  install(TARGETS ${_name} DESTINATION bareduckdb/capi/impl)
endforeach()
