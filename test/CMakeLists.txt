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

include(CheckCXXCompilerFlag)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# CHECK_CXX_COMPILER_FLAG actually check preprocessor flag...
set(SAFE_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
CHECK_CXX_COMPILER_FLAG( -DDummy FLAG_SANITIZE_ADDRESS)
set(CMAKE_REQUIRED_FLAGS ${SAFE_CMAKE_REQUIRED_FLAGS})

if(FLAG_SANITIZE_ADDRESS)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
endif()

set(TESTS
    simple
    test_cover
    test_txn
    test_whiteout
    test_sandwich
    test_corners
    )

foreach(test ${TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} ${GTEST_BOTH_LIBRARIES} ${LevelDB_LIBRARIES})
    if(FAST_CHECK)
        add_test(${test} ${test})
    else()
        GTEST_ADD_TESTS(${test} "" AUTO)
    endif()
    add_custom_target(check-disabled-${test}
        COMMAND ${test} --gtest_also_run_disabled_tests --gtest_filter=*.DISABLED_*
        DEPENDS ${test}
        )
    add_dependencies(check-disabled check-disabled-${test})
endforeach()

# a bit of magic to automatically reload generated list of tests
if(NOT FAST_CHECK)
    add_custom_command(
        OUTPUT .refresh_tests
        COMMAND ${CMAKE_MAKE_PROGRAM} rebuild_cache
        COMMAND touch .refresh_tests
        DEPENDS ${TESTS}.cpp
        )

    add_custom_target(refresh_tests DEPENDS .refresh_tests)
    add_dependencies(depend-check refresh_tests ${TESTS})
endif()

add_dependencies(depend-check ${TESTS})

# also lets check symbols duplications by linking all tests together
add_executable(all-tests ${TESTS}.cpp)
target_link_libraries(all-tests ${GTEST_BOTH_LIBRARIES} ${LevelDB_LIBRARIES})
