aux_source_directory (. SRC_LIST)

add_executable (sgw ${SRC_LIST})
add_compile_options(-g -Wall -Wextra -Wno-unused-function -Wno-unused-parameter -fPIC)
target_link_libraries(sgw pthread crypto)
set (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)