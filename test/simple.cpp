#include "leveldb/bottom_db.hpp"
#include "leveldb/walker.hpp"
#include "leveldb/merge_walker.hpp"
#include "leveldb/memory_db.hpp"

#include <gtest/gtest.h>

#include "util.hpp"

using namespace std;

TEST(Simple, memoryDB)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };
    EXPECT_EQ( 3, mem.size() );

    string v;
    EXPECT_OK( mem.Get("b", v) );
    EXPECT_EQ( "1", v );

    auto w = leveldb::walker(mem);
    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "a", w.key() );

    leveldb::AnyDB &db = mem;

    auto w3 = leveldb::walker(db);
    auto w2 = std::move(w3);
    w2.SeekToFirst();
    ASSERT_TRUE( w2.Valid() );
    EXPECT_OK( w2.status() );
    EXPECT_EQ( "a", w2.key() );

}

TEST(Simple, walkMemoryDB)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    auto w = leveldb::walker(mem);

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "2", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() );

    w.SeekToFirst();
    w.Prev();
    EXPECT_FALSE( w.Valid() );

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() );
}

TEST(Simple, walkWhiteout)
{
    leveldb::Whiteout wh { "b", "a", "c" };

    auto w = leveldb::walker(wh);
    auto x = w;

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );

    w.Next();
    EXPECT_FALSE( w.Valid() );

    w.SeekToFirst();
    w.Prev();
    EXPECT_FALSE( w.Valid() );

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );

    w.Next();
    EXPECT_FALSE( w.Valid() );
}

TEST(Simple, walkSubtract)
{
    leveldb::MemoryDB mem {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    leveldb::Whiteout wh { "b" };

    auto w = leveldb::subtract(mem, wh);

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );

    w.Next();
    EXPECT_FALSE( w.Valid() );

    w.SeekToFirst();
    w.Prev();
    EXPECT_FALSE( w.Valid() );

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );

    w.Next();
    EXPECT_FALSE( w.Valid() );
}

TEST(Simple, DISABLED_dummy)
{
    leveldb::BottomDB db;
    db.options.create_if_missing = true;
    ASSERT_OK( db.Open("/tmp/test.ldb") );

    EXPECT_OK( db.Put("a", "1") );
    EXPECT_OK( db.Put("b", "2") );
    EXPECT_OK( db.Put("c", "4") );


    leveldb::Whiteout wh { "b" };
    leveldb::MemoryDB mem { { "d", "5" } };

    auto w = leveldb::subtract(db, wh);

    w.SeekToFirst();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
}
