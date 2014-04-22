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

TEST(TestTxnIterator, DISABLED_txn_walk_deleted)
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
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
}

// insert right between walkers over database and transaction
TEST(TestTxnIterator, DISABLED_txn_insert_next)
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
