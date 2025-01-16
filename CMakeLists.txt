cmake_minimum_required(VERSION 3.7)

set( CMAKE_CXX_STANDARD 20 )
set( CMAKE_POSITION_INDEPENDENT_CODE true )

find_package(Protobuf REQUIRED)
message(Protobuf_INCLUDE_DIRS="${Protobuf_INCLUDE_DIRS}")
message(Protobuf_LIBRARIES="${Protobuf_LIBRARIES}")
message(Protobuf_PROTOC_LIBRARIES="${Protobuf_PROTOC_LIBRARIES}")
message(Protobuf_LITE_LIBRARIES="${Protobuf_LITE_LIBRARIES}")

find_package(ZLIB REQUIRED)
# find_package(LIBGD REQUIRED)
find_package(Threads)

add_subdirectory(abseil-cpp)
include_directories("${CMAKE_SOURCE_DIR}")
include_directories("${CMAKE_SOURCE_DIR}/src")
include_directories("${CMAKE_SOURCE_DIR}/abseil-cpp")
include_directories(${CMAKE_BINARY_DIR})

message(CMAKE_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
message(CMAKE_BINARY_DIR="${CMAKE_BINARY_DIR}")

add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64)

# print file paths
# add_compile_options(-H)
add_compile_options(-O3)
add_compile_options(-Wall)
# add_compile_options(-Wno-unused-but-set-variable)
# add_compile_options(-Wno-unused-variable)
add_compile_options(-DLOGURU_WITH_STREAMS=1)
add_compile_options(-march=native)

# Debugging information
# add_compile_options(-g)

# add_compile_options(-D_GLIBCXX_CONCEPT_CHECKS -D_GLIBCXX_DEBUG -D_GLIBCXX_DEBUG_PEDANTIC)

add_library(loguru_lib src/logging/loguru.cpp)
add_library(build_graph_lib src/graph/build_graph.cpp)
target_link_libraries(build_graph_lib PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::btree absl::flat_hash_map absl::strings absl::str_format loguru_lib)

add_executable(build_graph_main src/bin/build_graph_main.cpp)
target_include_directories(build_graph_main SYSTEM PRIVATE ${ZLIB_INCLUDE_DIR})
target_link_libraries(build_graph_main PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::btree absl::flat_hash_map absl::strings absl::str_format loguru_lib absl::time build_graph_lib)

add_executable(osmgrep src/bin/osmgrep.cpp)
target_include_directories(osmgrep SYSTEM PRIVATE ${ZLIB_INCLUDE_DIR})
target_link_libraries(osmgrep PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::strings absl::str_format absl::flat_hash_map loguru_lib absl::time)

add_executable(test src/test/test.cpp)
target_include_directories(test SYSTEM PRIVATE)
target_link_libraries(test PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::strings absl::flat_hash_map  absl::str_format loguru_lib absl::time build_graph_lib)

add_executable(test_consume_way src/test/test_consume_way.cpp)
target_include_directories(test_consume_way SYSTEM PRIVATE)
target_link_libraries(test_consume_way PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::strings absl::flat_hash_map  absl::str_format loguru_lib absl::time build_graph_lib)

add_executable(test_louvain src/test/test_louvain.cpp)
target_include_directories(test_louvain SYSTEM PRIVATE)
target_link_libraries(test_louvain PRIVATE absl::strings absl::str_format loguru_lib absl::time)

add_executable(test_routing src/test/test_routing.cpp)
target_include_directories(test_routing SYSTEM PRIVATE)
target_link_libraries(test_routing PRIVATE absl::strings absl::flat_hash_map absl::str_format loguru_lib absl::time)

add_executable(tile_server src/bin/tile_server.cpp)
target_include_directories(tile_server SYSTEM PRIVATE)
target_link_libraries(tile_server PRIVATE libgd.a libpng.a ZLIB::ZLIB freetype fontconfig absl::strings absl::str_format loguru_lib absl::time)

add_executable(extract_admin src/bin/extract_admin.cpp)
target_include_directories(extract_admin SYSTEM PRIVATE ${ZLIB_INCLUDE_DIR})
target_link_libraries(extract_admin PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::strings absl::str_format loguru_lib absl::time)

add_executable(poi_traffic src/bin/poi_traffic.cpp)
target_include_directories(poi_traffic SYSTEM PRIVATE ${ZLIB_INCLUDE_DIR})
target_link_libraries(poi_traffic PRIVATE osmpbf ZLIB::ZLIB protobuf::libprotobuf absl::strings absl::str_format loguru_lib absl::time build_graph_lib)

