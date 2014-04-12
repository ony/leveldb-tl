set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

set(TESTS
    simple
    test_cover
    test_txn
    test_whiteout
    )

foreach(test ${TESTS})
    add_executable(${test} ${test}.cpp)
    target_link_libraries(${test} ${GTEST_BOTH_LIBRARIES} ${LevelDB_LIBRARIES})
    GTEST_ADD_TESTS(${test} "" AUTO)
endforeach()

# a bit of magic to automatically reload generated list of tests
add_custom_command(
    OUTPUT .refresh_tests
    COMMAND ${CMAKE_MAKE_PROGRAM} rebuild_cache
    COMMAND touch .refresh_tests
    DEPENDS ${TESTS}.cpp
    )

add_custom_target(refresh_tests ALL DEPENDS .refresh_tests)

add_dependencies(check refresh_tests ${TESTS})
