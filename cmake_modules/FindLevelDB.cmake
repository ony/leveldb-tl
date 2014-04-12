# provides:
#   LevelDB_FOUND
#   LevelDB_INCLUDE_DIRS
#   LevelDB_LIBRARIES

find_path(LevelDB_INCLUDE_DIR
    NAMES leveldb/db.h
    )

find_library(LevelDB_LIBRARY
    NAMES leveldb
    )

mark_as_advanced(LevelDB_INCLUDE_DIR LevelDB_LIBRARY)

include(${CMAKE_ROOT}/Modules/FindPackageHandleStandardArgs.cmake)

find_package_handle_standard_args(LevelDB
    REQUIRED_VARS LevelDB_INCLUDE_DIR LevelDB_LIBRARY
    )

set(LevelDB_FOUND ${LEVELDB_FOUND})

if(LevelDB_FOUND)
    set(LevelDB_INCLUDE_DIRS ${LevelDB_INCLUDE_DIR})
    set(LevelDB_LIBRARIES ${LevelDB_LIBRARY})
endif()
