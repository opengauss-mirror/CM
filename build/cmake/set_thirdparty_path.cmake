# to avoid the different of binarylibs and library
set(CJSON_DIR "cjson")
set(ZLIB_DIR "zlib1.2.11")
set(LIB_MODE_COMM comm)
set(LIB_MODE ${LIB_MODE_COMM})

# if use ${PROJECT_SOURCE_DIR}/library, we don't need ${PLAT_FORM_NAME}
# otherwise if use $ENV{THIRD_BIN_PATH} or 3RD_PATH, we need ${PLAT_FORM_NAME}
set(3RD_PATH $ENV{THIRD_BIN_PATH})
if (("x${3RD_PATH}" STREQUAL "x" OR (NOT ENABLE_LIBPQ)) AND (EXISTS "${PROJECT_SOURCE_DIR}/library"))
    set(3RD_PATH ${PROJECT_SOURCE_DIR}/library)
    set(LIB_MODE "")
    set(LIB_MODE_COMM "")

    set(3RD_DEPENDENCY_ROOT ${3RD_PATH})
    set(3RD_PLATFORM_ROOT ${3RD_PATH})
    set(3RD_BUILDTOOLS_ROOT ${3RD_PATH})
    set(CJSON_DIR "cJSON")
    set(ZLIB_DIR "zlib")
else ()
    if ("x${3RD_PATH}" STREQUAL "x")
        set(3RD_PATH ${DEFAULT_3RD_PATH})
    endif ()

    set(3RD_DEPENDENCY_ROOT ${3RD_PATH}/kernel/dependency)
    set(3RD_PLATFORM_ROOT ${3RD_PATH}/kernel/platform)
    set(3RD_BUILDTOOLS_ROOT ${3RD_PATH}/kernel/buildtools)
endif ()

message("Using 3rd_library path is [${3RD_PATH}]")

# SET(GCC_HOME ${3RD_BUILDTOOLS_ROOT}/gcc7.3/gcc)
set(PGPORT_HOME ${PROJECT_SOURCE_DIR}/common_lib/port)
set(LIB_DCC_HOME ${PROJECT_SOURCE_DIR}/common_lib/dcc)

set(SECURE_HOME ${3RD_PLATFORM_ROOT}/Huawei_Secure_C/${LIB_MODE_COMM}) # whether needed?
set(KRB_HOME ${3RD_DEPENDENCY_ROOT}/kerberos/${LIB_MODE})
set(LIB_PSUTIL_HOME ${3RD_DEPENDENCY_ROOT}/psutil)
# hotpatch
set(3RD_HOTPATCH_HOME ${3RD_PLATFORM_ROOT}/hotpatch)

set(DCC_DIRECTORY_INC ${LIB_DCC_HOME}/include)
set(DCC_DIRECTORY_LIB ${LIB_DCC_HOME}/lib)
set(GSTOR_DIRECTORY_LIB ${LIB_GSTOR_HOME}/lib)
set(CBB_DIRECTORY_INC ${PROJECT_SOURCE_DIR}/common_lib/cbb/include)
set(CBB_DIRECTORY_LIB ${PROJECT_SOURCE_DIR}/common_lib/cbb/lib)

set(3RD_HOTPATCH_INC ${3RD_HOTPATCH_HOME}/include)
set(3RD_HOTPATCH_LIB ${3RD_HOTPATCH_HOME}/lib)
set(3RD_HOTPATCH_TOOL ${3RD_HOTPATCH_HOME}/tool)
set(3RD_HOTPATCH_CONFIG ${3RD_HOTPATCH_HOME}/config)
set(SECURE_DIRECTORY_INC ${SECURE_HOME}/include)
set(SECURE_DIRECTORY_LIB ${SECURE_HOME}/lib)
set(CJSON_DIRECTORY_INC ${3RD_DEPENDENCY_ROOT}/${CJSON_DIR}/${LIB_MODE}/include)
set(CJSON_DIRECTORY_LIB ${3RD_DEPENDENCY_ROOT}/${CJSON_DIR}/${LIB_MODE}/lib)
# if (ENABLE_MULTIPLE_NODES)
set(CGROUP_DIRECTORY_INC ${3RD_DEPENDENCY_ROOT}/libcgroup/${LIB_MODE}/include)
set(CGROUP_DIRECTORY_LIB ${3RD_DEPENDENCY_ROOT}/libcgroup/${LIB_MODE}/lib)
# endif ()
set(SSL_DIRECTORY_INC ${3RD_DEPENDENCY_ROOT}/openssl/${LIB_MODE}/include)
set(SSL_DIRECTORY_LIB ${3RD_DEPENDENCY_ROOT}/openssl/${LIB_MODE}/lib)
set(SSL_DIRECTORY_BIN ${3RD_DEPENDENCY_ROOT}/openssl/${LIB_MODE}/bin)
set(ZLIB_DIRECTORY_INC ${3RD_DEPENDENCY_ROOT}/${ZLIB_DIR}/${LIB_MODE}/include)
set(ZLIB_DIRECTORY_LIB ${3RD_DEPENDENCY_ROOT}/${ZLIB_DIR}/${LIB_MODE}/lib)
set(LZ4_DIRECTORY_LIB /usr/lib64)
set(ETCD_DIRECTORY_INC /usr/include)
set(ETCD_DIRECTORY_LIB /usr/lib64)
set(ETCD_DIRECTORY_BIN /usr/bin)
set(ZSTD_DIRECTORY_LIB ${3RD_DEPENDENCY_ROOT}/zstd/lib)

set(DCC_ALL_LIBS ${DCC_DIRECTORY_LIB} ${SSL_DIRECTORY_LIB} ${ZSTD_DIRECTORY_LIB} ${LZ4_DIRECTORY_LIB})
set(3RD_LIB_PATH ${CJSON_DIRECTORY_LIB} ${CGROUP_DIRECTORY_LIB} ${SSL_DIRECTORY_LIB}
        ${DCC_ALL_LIBS} ${PGPORT_HOME} ${SECURE_DIRECTORY_LIB} ${CBB_DIRECTORY_LIB})
# some objs
set(DCC_LIBS dcc gstor dcf cjson zstd lz4 ssl crypto)
set(ETCD_LIBS clientv3)