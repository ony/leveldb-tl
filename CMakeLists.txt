cmake_minimum_required(VERSION 2.8.8)

project(leveldb-tl CXX)

enable_testing()

# additional modules and their overrides (i.e FindGTest)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})

option(FAST_CHECK "Run tests in a faster manner" ON)
option(BUILD_DOCUMENTATION "Build the documentation (Doxygen)." OFF)

include(CTest)
include(ProcessorCount)

find_package(GTest REQUIRED)
find_package(LevelDB REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

include_directories(include)
include_directories(${LevelDB_INCLUDE_DIRS})

install(DIRECTORY include/ DESTINATION include)

add_executable(mksandwich mksandwich.cpp)
target_link_libraries(mksandwich ${GTEST_BOTH_LIBRARIES} ${LevelDB_LIBRARIES})

set(ctest_args --output-on-failure)

add_custom_target(depend-check)
add_custom_target(check-disabled)

# testing
if(BUILD_TESTING)
    add_subdirectory(test)

    ProcessorCount(n)
    if(NOT n EQUAL 0)
        set(ctest_args ${ctest_args} -j${n})
    endif()
endif()

add_custom_target(check
    COMMAND ${CMAKE_CTEST_COMMAND} ${ctest_args}
    DEPENDS depend-check
    )

# doxygen
if(BUILD_DOCUMENTATION)
    find_package(Doxygen REQUIRED)

    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
    add_custom_target(doc ALL
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen" VERBATIM
        )
    install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html DESTINATION share/doc/${CMAKE_PROJECT_NAME})
endif()

include(FeatureSummary)
feature_summary(WHAT ALL)
