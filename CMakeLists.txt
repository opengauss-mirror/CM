cmake_minimum_required(VERSION 3.14.1)
project(CM)

set(OPENCM_PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(DEFAULT_3RD_PATH ${OPENCM_PROJECT_SOURCE_DIR}/binarylibs)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/build/cmake)
include(tools)
include(set_thirdparty_path)
include(build_options)

not_support(ENABLE_MULTIPLE_NODES)
not_support(ENABLE_PRIVATEGAUSS)
not_support(ENABLE_HOTPATCH)

include(build_config)

add_subdirectory(src)
