EXECUTE_PROCESS(
        COMMAND bash -c "git rev-parse HEAD | cut -b 1-8"
        OUTPUT_VARIABLE COMMIT_ID
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
EXECUTE_PROCESS(
        COMMAND bash -c "date \"+%Y-%m-%d %H:%M:%S\""
        OUTPUT_VARIABLE COMPILE_TIME
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
EXECUTE_PROCESS(
        COMMAND bash -c "dos2unix ${PROJECT_SOURCE_DIR}/build/cm.ver && source ${PROJECT_SOURCE_DIR}/build/cm.ver && echo \"\${PRODUCT} ${CMAKE_PROJECT_NAME} \${VERSION}\""
        OUTPUT_VARIABLE PRO_INFO
        OUTPUT_STRIP_TRAILING_WHITESPACE
)

SET(CM_VERSION_STR "(${CMAKE_PROJECT_NAME} build ${COMMIT_ID}) compiled at ${COMPILE_TIME} ${BUILD_MODE}")

CONFIGURE_FILE(
        "${OPENCM_PROJECT_SOURCE_DIR}/build/cmake/config.h.in"
        "${CMAKE_BINARY_DIR}/config.h"
)
