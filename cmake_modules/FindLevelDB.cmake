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

find_library(Snappy_LIBRARY
    NAMES snappy
    )

mark_as_advanced(LevelDB_INCLUDE_DIR LevelDB_LIBRARY Snappy_LIBRARY)

include(${CMAKE_ROOT}/Modules/FindPackageHandleStandardArgs.cmake)

find_package_handle_standard_args(LevelDB
    REQUIRED_VARS LevelDB_INCLUDE_DIR LevelDB_LIBRARY
    )

set(LevelDB_FOUND ${LEVELDB_FOUND})

if(LevelDB_FOUND)
    set(LevelDB_INCLUDE_DIRS ${LevelDB_INCLUDE_DIR})
    set(LevelDB_LIBRARIES ${LevelDB_LIBRARY})
    if(NOT Snappy_LIBRARY STREQUAL "Snappy_LIBRARY-NOTFOUND")
        set(LevelDB_LIBRARIES ${LevelDB_LIBRARIES} ${Snappy_LIBRARY})
    endif()
endif()
