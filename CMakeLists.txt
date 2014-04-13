cmake_minimum_required(VERSION 2.8.8)

project(leveldb-blocks CXX)

enable_testing()

# additional modules and their overrides (i.e FindGTest)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})
set(FAST_CHECK TRUE CACHE BOOL "Run tests in a faster manner")

include(CTest)
include(ProcessorCount)

find_package(GTest REQUIRED)
find_package(LevelDB REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

include_directories(include)
include_directories(${LevelDB_INCLUDE_DIRS})

install(DIRECTORY include/ DESTINATION include)

set(ctest_args --output-on-failure)

add_custom_target(depend-check)
add_custom_target(check-disabled)

if (BUILD_TESTING)
    add_subdirectory(test)

    ProcessorCount(n)
    if (NOT n EQUAL 0)
        set(ctest_args ${ctest_args} -j${n})
    endif()
endif()

add_custom_target(check
    COMMAND ${CMAKE_CTEST_COMMAND} ${ctest_args}
    DEPENDS depend-check
    )
