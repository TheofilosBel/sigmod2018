cmake_minimum_required (VERSION 2.6)
project (Sig18)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
#set(CMAKE_C_COMPILER /usr/local/bin/gcc)
#set(CMAKE_CXX_COMPILER /usr/bin/g++-6)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_} -fuse-ld=gold -Ofast -march=native -lpthread  -msse4.2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_}  -fuse-ld=gold -Ofast -march=native -lpthread")


include_directories(include JobScheduler)

set(JS_SOURCE_FILES JobScheduler/filter_job.cpp JobScheduler/create_job.cpp JobScheduler/checksum_job.cpp JobScheduler/main_job.cpp JobScheduler/js_master.h JobScheduler/queryFill_job.cpp  JobScheduler/create_job_64.cpp JobScheduler/checksum_job_64.cpp)
set(RJ_SOURCE_FILES parallel_radix_join.cpp parallel_radix_join_64.cpp cpu_mapping.cpp generator.cpp)

add_library(database QueryPlan.cpp Relation.cpp Filter.cpp Parser.cpp Utils.cpp)
target_include_directories(database PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

OPTION(FORCE_TESTS "Build tests, regardless of build type." ON)
if (CMAKE_BUILD_TYPE MATCHES "[Dd][Ee][Bb][Uu][Gg]" OR FORCE_TESTS)
    add_subdirectory(test)
endif()

set(SOURCE_FILES QueryPlan.cpp Filter.cpp Relation.cpp Parser.cpp Utils.cpp Creates.cpp CheckSums.cpp)
add_executable(Joiner Joiner.cpp ${SOURCE_FILES} ${JS_SOURCE_FILES} ${RJ_SOURCE_FILES})
target_link_libraries(Joiner database)


# Interactive command line tool to translate our query format to SQL queries
set(SOURCE_FILE_FOR_Query2SQL Relation.cpp Parser.cpp Utils.cpp)
add_executable(Query2SQL Query2SQL.cpp ${SOURCE_FILE_FOR_Query2SQL})

# Test harness
add_executable(harness harness.cpp)

ADD_CUSTOM_TARGET(link_target ALL
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/workloads
  ${CMAKE_CURRENT_BINARY_DIR}/workloads)
