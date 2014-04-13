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
