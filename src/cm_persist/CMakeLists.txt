# cm_persist tools, it should be independent
set(TGT_cmpersist_INC 
    ${PROJECT_INC_BASE}/cm/cm_persist 
    ${SECURE_DIRECTORY_INC})

include_directories(${TGT_cmpersist_INC})
link_directories(${SECURE_DIRECTORY_LIB})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} TGT_cmpersist_SRC)
set(CMP_LINK_LIBS securec ${G_BIN_EXT_LIBS})
if (ENABLE_MEMCHECK)
    list(APPEND CMP_LINK_LIBS pthread)
endif ()
add_exec_tgt(cm_persist TGT_cmpersist_SRC "")
target_link_libraries(cm_persist PRIVATE ${CMP_LINK_LIBS})
install(TARGETS cm_persist RUNTIME DESTINATION bin)
