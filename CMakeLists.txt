cmake_minimum_required(VERSION 2.8.8)

project(leveldb-blocks CXX)

enable_testing()

# additional modules and their overrides (i.e FindGTest)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})

include(CTest)
include(ProcessorCount)

find_package(GTest REQUIRED)
find_package(LevelDB REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")

include_directories(include)
include_directories(${LevelDB_INCLUDE_DIRS})

set(ctest_args --output-on-failure)

ProcessorCount(N)
if (NOT N EQUAL 0)
    set(ctest_args ${ctest_args} -j${N})
    set(CTEST_BUILD_FLAGS -j${N})
    set(ctest_test_args ${ctest_test_args} PARALLEL_LEVEL ${N})
endif()

add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} ${ctest_args})

add_subdirectory(test)
