aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} TGT_client_SRC)

set(TGT_client_INC ${PROJECT_INC_BASE}/cm/cm_client)
set(TGT_LINK_LIBS pthread cmcommon_static securec)

if (ENABLE_UT)
    add_shared_tgt(utcmclient TGT_client_SRC TGT_client_INC)
    target_link_libraries(utcmclient ${TGT_LINK_LIBS})

    install(TARGETS utcmclient LIBRARY DESTINATION lib)
else ()
    add_shared_tgt(client TGT_client_SRC TGT_client_INC)
    add_static_tgt(client TGT_client_SRC TGT_client_INC)
    target_link_libraries(client PRIVATE ${TGT_LINK_LIBS})
endif ()
