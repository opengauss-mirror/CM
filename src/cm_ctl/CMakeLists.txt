# cm_ctl bin
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} TGT_cmctl_SRC)
if (ENABLE_MULTIPLE_NODES)
    aux_source_directory(${DIST_PATH}/cm_ctl TGT_cmctlD_SRC)
    list(APPEND TGT_cmctl_SRC ${TGT_cmctlD_SRC})
endif ()

list(APPEND TGT_cmctl_SRC ${OPENCM_PROJECT_SOURCE_DIR}/src/lib/pq/port/cipher.cpp)

set(CTL_LIB_PATHS
        ${PGPORT_HOME}
        ${CJSON_DIRECTORY_LIB}
        ${CGROUP_DIRECTORY_LIB}
        ${DCC_ALL_LIBS}
        ${ETCD_DIRECTORY_LIB}
        )

set(CMC_LINK_LIBS cmpq_static cmclient_static ${CM_ADPT_LIBS} config_static pthread cjson alarmclient_static
        securec hotpatchclient_static cmcommon_static cmprotocol_static m ${G_BIN_EXT_LIBS})
if (ENABLE_MULTIPLE_NODES)
    list(APPEND CMC_LINK_LIBS cgroup)
endif ()

if (ENABLE_KRB)
    list(APPEND CMC_LINK_LIBS ${KRB_LIBS} gssrpc_gauss)
endif ()

link_directories(${CTL_LIB_PATHS})

if (ENABLE_UT)
    add_shared_tgt(utcmctl TGT_cmctl_SRC "")
    target_link_libraries(utcmctl ${CMC_LINK_LIBS})
    install(TARGETS utcmctl LIBRARY DESTINATION lib)
else ()
    add_exec_tgt(cm_ctl TGT_cmctl_SRC "")
    target_link_libraries(cm_ctl PRIVATE ${CMC_LINK_LIBS})
    install(TARGETS cm_ctl RUNTIME DESTINATION bin)
endif ()