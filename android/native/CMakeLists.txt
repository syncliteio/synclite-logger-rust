cmake_minimum_required(VERSION 3.22.1)
project(synclite_android_cdc LANGUAGES C)

if(NOT ANDROID)
    message(FATAL_ERROR "Use the Android NDK toolchain to build this CDC sidecar")
endif()

set(_synclite_android_api "")
if(DEFINED ANDROID_PLATFORM AND NOT ANDROID_PLATFORM STREQUAL "")
    set(_synclite_android_api "${ANDROID_PLATFORM}")
elseif(DEFINED CMAKE_SYSTEM_VERSION AND NOT CMAKE_SYSTEM_VERSION STREQUAL "")
    set(_synclite_android_api "${CMAKE_SYSTEM_VERSION}")
endif()

if(_synclite_android_api MATCHES "^(android-)?([0-9]+)$")
    if(CMAKE_MATCH_2 LESS 26)
        message(FATAL_ERROR "SyncLite Android CDC requires API 26 or later")
    endif()
elseif(_synclite_android_api STREQUAL "")
    message(FATAL_ERROR "SyncLite Android CDC requires an Android API level of 26 or later")
else()
    message(FATAL_ERROR "Unsupported Android API format for SyncLite CDC: ${_synclite_android_api}")
endif()

if(NOT ANDROID_ABI MATCHES "^(arm64-v8a|x86_64)$")
    message(FATAL_ERROR "Unsupported SyncLite Android CDC ABI: ${ANDROID_ABI}")
endif()

set(SYNCLITE_NATIVE_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../SyncLiteNative"
    CACHE PATH "Directory containing the custom SyncLiteNative C sources")
foreach(source sqlite3.c sqlite3.h replayer.c replayer.h rust_bindings.c rust_bindings.h)
    if(NOT EXISTS "${SYNCLITE_NATIVE_SOURCE_DIR}/${source}")
        message(FATAL_ERROR "Missing SyncLiteNative source: ${SYNCLITE_NATIVE_SOURCE_DIR}/${source}")
    endif()
endforeach()

# This is a separate SQLite instance from rusqlite/libsqlite3-sys. Never link
# against the platform SQLite or export sqlite3_* for other DSOs to bind to.
add_library(synclitecdc SHARED
    "${SYNCLITE_NATIVE_SOURCE_DIR}/sqlite3.c"
    "${SYNCLITE_NATIVE_SOURCE_DIR}/replayer.c"
    "${SYNCLITE_NATIVE_SOURCE_DIR}/rust_bindings.c")
target_compile_features(synclitecdc PRIVATE c_std_99)
target_include_directories(synclitecdc PRIVATE "${SYNCLITE_NATIVE_SOURCE_DIR}")
target_compile_definitions(synclitecdc PRIVATE
    SQLITE_THREADSAFE=1 SQLITE_ENABLE_PREUPDATE_HOOK)
set_target_properties(synclitecdc PROPERTIES C_VISIBILITY_PRESET hidden)
target_link_libraries(synclitecdc PRIVATE dl m)
target_link_options(synclitecdc PRIVATE
    "-Wl,--no-undefined"
    "-Wl,-Bsymbolic-functions"
    "-Wl,-z,max-page-size=16384"
    "-Wl,--version-script=${CMAKE_CURRENT_LIST_DIR}/exports.map")
set_property(TARGET synclitecdc APPEND PROPERTY LINK_DEPENDS
    "${CMAKE_CURRENT_LIST_DIR}/exports.map")

# Fail the ABI build rather than shipping an apparently valid stock SQLite DSO
# or a sidecar missing one of the runtime's dynamically resolved entry points.
add_custom_command(TARGET synclitecdc POST_BUILD
    COMMAND "${CMAKE_COMMAND}"
        "-DLIBRARY=$<TARGET_FILE:synclitecdc>"
        "-DNM=${CMAKE_NM}"
        "-DREADELF=${CMAKE_READELF}"
        "-DHEADER=${SYNCLITE_NATIVE_SOURCE_DIR}/rust_bindings.h"
        -P "${CMAKE_CURRENT_LIST_DIR}/verify-cdc.cmake"
    VERBATIM)
set_property(TARGET synclitecdc APPEND PROPERTY LINK_DEPENDS
    "${CMAKE_CURRENT_LIST_DIR}/verify-cdc.cmake")