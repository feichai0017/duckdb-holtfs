cmake_minimum_required(VERSION 3.10)

set(TARGET_NAME holtfs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(HOLT_ROOT "${CMAKE_CURRENT_LIST_DIR}/third_party/holt"
    CACHE PATH "Path to a Holt checkout containing crates/holt-ffi")
set(HOLT_FFI_INCLUDE_DIR "${HOLT_ROOT}/crates/holt-ffi/include")
set(HOLT_CARGO_ARGS build -p holt-ffi --release --locked)

if(WIN32)
  set(HOLT_FFI_LIBRARY "${HOLT_ROOT}/target/release/holt_ffi.lib")
elseif(APPLE AND OSX_BUILD_ARCH STREQUAL "x86_64")
  find_program(RUSTUP_EXECUTABLE rustup)
  if(NOT RUSTUP_EXECUTABLE)
    message(FATAL_ERROR "rustup is required to cross-build holt-ffi for x86_64-apple-darwin")
  endif()
  execute_process(
    COMMAND ${RUSTUP_EXECUTABLE} target add x86_64-apple-darwin
    RESULT_VARIABLE HOLT_RUSTUP_RESULT
    OUTPUT_VARIABLE HOLT_RUSTUP_OUTPUT
    ERROR_VARIABLE HOLT_RUSTUP_ERROR)
  if(NOT HOLT_RUSTUP_RESULT EQUAL 0)
    message(FATAL_ERROR "failed to install Rust target x86_64-apple-darwin:\n${HOLT_RUSTUP_OUTPUT}\n${HOLT_RUSTUP_ERROR}")
  endif()
  list(APPEND HOLT_CARGO_ARGS --target x86_64-apple-darwin)
  set(HOLT_FFI_LIBRARY "${HOLT_ROOT}/target/x86_64-apple-darwin/release/libholt_ffi.a")
else()
  set(HOLT_FFI_LIBRARY "${HOLT_ROOT}/target/release/libholt_ffi.a")
endif()

if(NOT EXISTS "${HOLT_FFI_INCLUDE_DIR}/holt_ffi.h")
  message(FATAL_ERROR "HOLT_ROOT must point at a Holt checkout with crates/holt-ffi/include/holt_ffi.h")
endif()

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "cargo is required to build holt-ffi")
endif()
execute_process(
  COMMAND ${CARGO_EXECUTABLE} ${HOLT_CARGO_ARGS}
  WORKING_DIRECTORY "${HOLT_ROOT}"
  RESULT_VARIABLE HOLT_FFI_BUILD_RESULT
  OUTPUT_VARIABLE HOLT_FFI_BUILD_OUTPUT
  ERROR_VARIABLE HOLT_FFI_BUILD_ERROR)

if(NOT HOLT_FFI_BUILD_RESULT EQUAL 0)
  message(FATAL_ERROR "failed to build holt-ffi:\n${HOLT_FFI_BUILD_OUTPUT}\n${HOLT_FFI_BUILD_ERROR}")
endif()

if(NOT EXISTS "${HOLT_FFI_LIBRARY}")
  message(FATAL_ERROR "holt-ffi build did not produce ${HOLT_FFI_LIBRARY}")
endif()

set(EXTENSION_SOURCES src/holtfs_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set_target_properties(${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME}
  PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

target_include_directories(${EXTENSION_NAME} PRIVATE ${HOLT_FFI_INCLUDE_DIR})
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${HOLT_FFI_INCLUDE_DIR})

target_link_libraries(${EXTENSION_NAME} ${HOLT_FFI_LIBRARY})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${HOLT_FFI_LIBRARY})

if(UNIX)
  target_link_libraries(${EXTENSION_NAME} pthread ${CMAKE_DL_LIBS} m)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} pthread ${CMAKE_DL_LIBS} m)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
