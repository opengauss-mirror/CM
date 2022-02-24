
set(CMAKE_CXX_STANDARD 11)

# flags that used for all modules
set(CM_COMMON_FLAGS "-std=c++11")
set(G_LIB_VERSION 1)

# cmake opts
option(ENABLE_UT "enable ut(ON/OFF)" OFF)
option(ENABLE_MEMCHECK "enable memory check" OFF)
option(ENABLE_GCOV "distribute or centralize" OFF)

include(${PROJECT_SOURCE_DIR}/build/cmake/feature_options.cmake)

set(BUILD_MODE Debug)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" OR "${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(BUILD_MODE Debug)
    set(CMAKE_BUILD_TYPE Debug)
    set(OPTIMIZE_LEVEL -O0 -g)
elseif (${CMAKE_BUILD_TYPE} STREQUAL "Release")
    set(BUILD_MODE Release)
    set(ENABLE_MEMCHECK OFF)
    set(ENABLE_UT OFF)
    set(OPTIMIZE_LEVEL -O2 -g3)
else ()
    message(FATAL_ERROR "unsupported CMAKE_BUILD_TYPE = " ${CMAKE_BUILD_TYPE})
endif ()

if (ENABLE_MEMCHECK AND ENABLE_UT)
    message(FATAL_ERROR "unsupported ENABLE_MEMCHECK and ENABLE_UT both true!")
endif ()

if (ENABLE_MEMCHECK)
    set(BUILD_MODE Memcheck)
    message("ENABLE_MEMCHECK is on!")
endif ()

execute_process(COMMAND uname -p OUTPUT_VARIABLE BUILD_TUPLE OUTPUT_STRIP_TRAILING_WHITESPACE)

if (${BUILD_TUPLE} STREQUAL "x86_64")
    set(OS_OPTIONS -msse4.2 -mcx16)
    add_definitions(-DUSE_SSE42_CRC32C_WITH_RUNTIME_CHECK)
elseif (${BUILD_TUPLE} STREQUAL "aarch64")
    set(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK OFF)
    if (ENABLE_MULTIPLE_NODES AND (${CMAKE_BUILD_TYPE} STREQUAL "Release"))
        set(OS_OPTIONS -march=armv8-a+crc+lse)
    else ()
        set(OS_OPTIONS -march=armv8-a+crc)
    endif ()
endif ()

if (${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS} STREQUAL OFF_OFF)
    set(ENABLE_HOTPATCH OFF)
endif ()

set(HOTPATCH_PLATFORM_LIST suse11_sp1_x86_64 suse12_sp5_x86_64 euleros2.0_sp8_aarch64 euleros2.0_sp9_aarch64 euleros2.0_sp10_aarch64 euleros2.0_sp2_x86_64 euleros2.0_sp5_x86_64 euleros2.0_sp10_x86_64 kylinv10_sp1_aarch64 kylinv10_sp1_x86_64_intel)
set(HOTPATCH_ARM_LIST euleros2.0_sp8_aarch64 euleros2.0_sp9_aarch64 euleros2.0_sp10_aarch64 kylinv10_sp1_aarch64)
list(FIND HOTPATCH_PLATFORM_LIST "${PLAT_FORM_NAME}" RET_HOTPATCH)
list(FIND HOTPATCH_ARM_LIST "${PLAT_FORM_NAME}" RET_ARM_HOTPATCH)
if (ENABLE_HOTPATCH AND (${RET_HOTPATCH} STREQUAL -1))
    message(WARNING "Current OS(${PLAT_FORM_NAME}) is not in os list, don't support ENABLE_HOTPATCH!!, supported plantform list is ${HOTPATCH_PLATFORM_LIST}")
    set(ENABLE_HOTPATCH OFF)
endif ()

if (ENABLE_UT)
    set(BUILD_MODE Ut)
    message("ENABLE_UT is on!, we use llt lib, and build debug pkg.")
    set(LIB_MODE llt)
    add_definitions(-D ENABLE_UT)
endif ()

# set install path if not pointed
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR}/${BUILD_MODE})
    message("We set CMAKE_INSTALL_PREFIX default path to [${CMAKE_INSTALL_PREFIX}]")
endif ()

message("We will install target to ${CMAKE_INSTALL_PREFIX}, build mode: <${CMAKE_BUILD_TYPE}>.")

set(SECURE_OPTIONS -fno-common -fstack-protector-strong)
set(SECURE_LINK_OPTS -Wl,-z,noexecstack -Wl,-z,relro,-z,now)
set(PROTECT_OPTIONS -fwrapv -g -std=c++11 ${OPTIMIZE_LEVEL})
set(WARNING_OPTIONS -Wall -Wendif-labels -Werror -Wformat-security)
set(OPTIMIZE_OPTIONS -pipe -fno-aggressive-loop-optimizations -fno-expensive-optimizations -fno-omit-frame-pointer
        -fno-strict-aliasing -freg-struct-return)
set(CHECK_OPTIONS -Wmissing-format-attribute -Wno-attributes -Wno-unused-but-set-variable
        -Wno-write-strings -Wpointer-arith)

if (NOT ENABLE_LIBPQ)
    message("ENABLE_LIBPQ is on, we only support this in single node mode without alarm.")
    set(ENABLE_MULTIPLE_NODES OFF)
    set(ENABLE_ALARM OFF)
endif ()

if (ENABLE_MULTIPLE_NODES)
    message("ENABLE_MULTIPLE_NODES is on!")
    add_definitions(-D ENABLE_MULTIPLE_NODES)
    set(DIST_PATH ${PROJECT_SOURCE_DIR}/distribute)
endif ()

if (ENABLE_PRIVATEGAUSS)
    message("ENABLE_PRIVATEGAUSS is on!")
    add_definitions(-D ENABLE_PRIVATEGAUSS)
    set(DIST_PATH ${PROJECT_SOURCE_DIR}/distribute)
endif()

if (ENABLE_LIBPQ)
    message("ENABLE_LIBPQ is on!")
    add_definitions(-D ENABLE_LIBPQ)
endif()

set(GCC_VERSION $ENV{GCC_VERSION})
if ("x${GCC_VERSION}" STREQUAL "x")
    set(GCC_VERSION "7.3.0")
endif ()

if (ENABLE_MEMCHECK)
    message("add memcheck dependencies.")
    set(MEMCHECK_HOME ${3RD_DEPENDENCY_ROOT}/memcheck/debug)
    set(MEMCHECK_LIB_PATH ${MEMCHECK_HOME}/gcc${GCC_VERSION}/lib)

    set(MEMCHECK_FLAGS -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer)
    set(MEMCHECK_LIBS libasan.a rt dl)
    set(MEMCHECK_LINK_DIRECTORIES ${MEMCHECK_LIB_PATH})
    list(REMOVE_ITEM SECURE_OPTIONS -fstack-protector)

    add_compile_options(${MEMCHECK_FLAGS})
endif ()

if (ENABLE_GCOV)
    message("add coverage dependencies.")
    set(GCOV_FLAGS -fprofile-arcs -ftest-coverage)
    set(GCOV_LIBS gcov)

    link_libraries(${GCOV_LIBS})
    add_compile_options(${GCOV_FLAGS})
    add_definitions(-D ENABLE_GCOV)
endif ()

if (ENABLE_HOTPATCH)
    if (NOT ${RET_ARM_HOTPATCH} EQUAL -1)
        set(HOTPATCH_ATOMIC_LDS -Wl,-T${3RD_HOTPATCH_TOOL}/atomic.lds)
    endif ()
endif ()

if (ENABLE_ETCD)
    list(APPEND 3RD_LIB_PATH ${ETCD_DIRECTORY_LIB})
endif ()

set(G_BIN_EXT_LIBS ${MEMCHECK_LIBS})

set(G_COMPILE_OPTIONS ${OS_OPTIONS} ${CM_COMMON_FLAGS} ${OPTIMIZE_LEVEL} ${SECURE_OPTIONS} ${PROTECT_OPTIONS}
        ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS})

set(G_LINK_OPTIONS ${CM_COMMON_FLAGS} ${SECURE_LINK_OPTS})
# secure opt
set(CMAKE_SKIP_RPATH TRUE)

add_compile_options(${G_COMPILE_OPTIONS})
add_link_options(${G_LINK_OPTIONS})

link_directories(${3RD_LIB_PATH})

set(PROJECT_INC_BASE ${OPENCM_PROJECT_SOURCE_DIR}/src/include)
set(COMM_INC
        ${PROJECT_INC_BASE}
        ${PROJECT_INC_BASE}/cm
        ${PROJECT_INC_BASE}/cm/cm_adapter
        ${PROJECT_INC_BASE}/cm/cm_server
        ${PROJECT_INC_BASE}/cm/cm_agent
        ${PROJECT_INC_BASE}/cm/cm_ctl
        ${SSL_DIRECTORY_INC}
        ${SECURE_DIRECTORY_INC}
        ${CMAKE_BINARY_DIR}
        )

# libpq must need krb5
if (ENABLE_KRB OR ENABLE_LIBPQ)
    set(KRB_LIBS gssapi_krb5_gauss krb5_gauss k5crypto_gauss com_err_gauss krb5support_gauss)
    link_directories(${KRB_HOME}/lib)
    list(APPEND COMM_INC ${KRB_HOME}/include)

    if (ENABLE_KRB)
        add_definitions(-D KRB5)
    endif ()
endif ()

if (ENABLE_LIBPQ)
    list(APPEND COMM_INC
            ${PROJECT_INC_BASE}/cm/cm_agent/clients/libpq)
endif ()
list(APPEND COMM_INC
        ${PROJECT_INC_BASE}/opengauss
        ${PROJECT_INC_BASE}/opengauss/cm
        ${PROJECT_INC_BASE}/opengauss/alarm
        ${PROJECT_INC_BASE}/opengauss/common/config
        )

include_directories(${COMM_INC})
