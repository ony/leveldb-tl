# @@@LICENSE
#
# Copyright (c) 2014 Nikolay Orliuk <virkony@gmail.com>
# Copyright (c) 2014 LG Electronics, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# LICENSE@@@

cmake_minimum_required(VERSION 2.8.8)

project(leveldb-tl CXX)

enable_testing()

# additional modules and their overrides (i.e FindGTest)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules" ${CMAKE_MODULE_PATH})

option(FAST_CHECK "Run tests in a faster manner" ON)
option(BUILD_DOCUMENTATION "Build the documentation (Doxygen)." OFF)
option(BUILD_MKSANDWICH "Build sandwich converting tool" ON)

include(CTest)
include(ProcessorCount)

find_package(LevelDB REQUIRED)
if (BUILD_TESTING)
    find_package(GTest REQUIRED)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wconversion -Werror")

include_directories(include)
include_directories(${LevelDB_INCLUDE_DIRS})

install(DIRECTORY include/ DESTINATION include)

if (BUILD_MKSANDWICH)
    add_executable(mksandwich mksandwich.cpp)
    target_link_libraries(mksandwich ${GTEST_BOTH_LIBRARIES} ${LevelDB_LIBRARIES})
endif()

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
