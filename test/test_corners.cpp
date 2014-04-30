// @@@LICENSE
//
//      Copyright (c) 2014 Nikolay Orliuk <virkony@gmail.com>
//      Copyright (c) 2014 LG Electronics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// LICENSE@@@

#include "leveldb/memory_db.hpp"
#include "leveldb/whiteout_db.hpp"
#include "leveldb/txn_db.hpp"
#include "leveldb/walker.hpp"

#include <gtest/gtest.h>

#include "util.hpp"

using namespace std;
using ::testing::PrintToString;

TEST(TestTxnIterator, memory_walk_deleted)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    auto w = leveldb::walker(mem);
    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    EXPECT_OK( mem.Delete("a") );
    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );
}

TEST(TestTxnIterator, whiteout_walk_deleted)
{
    leveldb::WhiteoutDB mem { "a", "b", "c" };

    auto w = leveldb::walker(mem);
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( mem.Delete("a") );
    EXPECT_FALSE( mem.Check("a") );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
}

TEST(TestTxnIterator, txn_walk_deleted_delete)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("a") );
    EXPECT_STATUS( NotFound, txn.Get("a", v) );
    EXPECT_OK( mem.Get("a", v) );

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "1", w.value() );

    EXPECT_OK( txn.Put("a", "4") );

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() ) << "Still have key " << PrintToString(w.key());
}

TEST(TestTxnIterator, txn_walk_deleted_next_delete)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("b") );
    EXPECT_STATUS( NotFound, txn.Get("b", v) );
    EXPECT_OK( mem.Get("b", v) );

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    EXPECT_OK( txn.Put("b", "4") );

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "4", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() ) << "Still have key " << PrintToString(w.key());
}

TEST(TestTxnIterator, txn_walk_deleted_next_dummy_delete)
{
    leveldb::MemoryDB mem {
        { "a", "2" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("b") );
    EXPECT_STATUS( NotFound, txn.Get("b", v) );
    EXPECT_STATUS( NotFound, mem.Get("b", v) );

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    EXPECT_OK( txn.Put("b", "4") );

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "4", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() ) << "Still have key " << PrintToString(w.key());
}


TEST(TestTxnIterator, txn_walk_deleted)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("a") );
    EXPECT_STATUS( NotFound, txn.Get("a", v) );
    EXPECT_OK( mem.Get("a", v) );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
}

TEST(TestTxnIterator, txn_walk_deleted_put)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    EXPECT_OK( txn.Put("a", "2") );
    EXPECT_OK( txn.Get("a", v) );
    EXPECT_STATUS( NotFound, mem.Get("a", v) );
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("a") );
    EXPECT_STATUS( NotFound, txn.Get("a", v) );
    EXPECT_STATUS( NotFound, mem.Get("a", v) );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
}

TEST(TestTxnIterator, txn_walk_deleted_next_put)
{
    leveldb::MemoryDB mem {
        { "a", "2" },
        { "c", "3" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    EXPECT_OK( txn.Put("b", "1") );
    EXPECT_OK( txn.Get("b", v) );
    EXPECT_STATUS( NotFound, mem.Get("b", v) );
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Delete("b") );
    EXPECT_STATUS( NotFound, txn.Get("b", v) );
    EXPECT_STATUS( NotFound, mem.Get("b", v) );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
}

// insert right between walkers over database and transaction
TEST(TestTxnIterator, txn_insert_next)
{
    leveldb::MemoryDB mem {
        { "a", "2" },
        { "d", "4" },
    };

    auto txn = leveldb::transaction(mem);

    std::string v;

    auto w = leveldb::walker(txn);
    EXPECT_OK( txn.Put("c", "3") );
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    EXPECT_OK( txn.Put("b", "1") );

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "d", w.key() );
}
