#include "leveldb/bottom_db.hpp"
#include "leveldb/walker.hpp"
#include "leveldb/cover_walker.hpp"
#include "leveldb/memory_db.hpp"
#include "leveldb/txn_db.hpp"
#include "leveldb/sandwich_db.hpp"

#include <gtest/gtest.h>

#include "util.hpp"

using namespace std;
using ::testing::PrintToString;
using leveldb::walker;
using leveldb::subtract;

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
    EXPECT_EQ( "2", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() );
    EXPECT_FAIL( w.status() );

    leveldb::AnyDB &db = mem;

    auto w3 = leveldb::walker(db);
    auto w2 = std::move(w3);
    w2.SeekToFirst();
    ASSERT_TRUE( w2.Valid() );
    EXPECT_OK( w2.status() );
    EXPECT_EQ( "a", w2.key() );

    w2.Prev();
    EXPECT_FALSE( w2.Valid() );
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

    auto w = walker(subtract(mem, wh));

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

TEST(Simple, walkCover)
{
    leveldb::MemoryDB mem1 {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    leveldb::MemoryDB mem2 {
        { "b", "4" },
        { "d", "5" },
    };

    auto w = walker(cover(mem1, mem2));

    w.SeekToFirst();
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

    w.Prev();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "4", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "d", w.key() );
    EXPECT_EQ( "5", w.value() );

    w.Prev();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

}

TEST(Simple, transaction)
{
    leveldb::MemoryDB db {
        { "b", "1" },
        { "a", "2" },
        { "c", "3" },
    };

    using TxnType = leveldb::TxnDB<leveldb::MemoryDB>;
    TxnType txn(db);
    string v;

    ASSERT_OK( txn.Get("a", v) );
    EXPECT_EQ( "2", v );

    ASSERT_OK( txn.Get("b", v) );
    EXPECT_EQ( "1", v );

    ASSERT_OK( txn.Put("a", "4") );
    ASSERT_OK( txn.Get("a", v) );
    EXPECT_EQ( "4", v );
    ASSERT_OK( db.Get("a", v) );
    EXPECT_EQ( "2", v );

    ASSERT_OK( txn.Delete("b") );
    EXPECT_FAIL( txn.Get("b", v) );
    ASSERT_OK( db.Get("b", v) );
    EXPECT_EQ( "1", v );

    // check iterator
    TxnType::IteratorType i(txn);

    i.SeekToFirst();
    ASSERT_TRUE( i.Valid() );
    EXPECT_EQ( "a", i.key() );
    EXPECT_EQ( "4", i.value() );

    i.Next();
    ASSERT_TRUE( i.Valid() );
    EXPECT_EQ( "c", i.key() );
    EXPECT_EQ( "3", i.value() );

    i.Next();
    EXPECT_FALSE( i.Valid() );

    // flush changes to DB
    ASSERT_OK( txn.commit() );

    ASSERT_OK( db.Get("a", v) );
    EXPECT_EQ( "4", v );
    ASSERT_OK( txn.Get("a", v) );
    EXPECT_EQ( "4", v );

    EXPECT_FAIL( db.Get("b", v) );
    EXPECT_FAIL( txn.Get("b", v) );

    // ensure that old values from Txn doesn't shadow underlying databse
    ASSERT_OK( db.Put("a", "5") );
    ASSERT_OK( txn.Get("a", v) );
    EXPECT_EQ( "5", v );
}

TEST(Simple, sequence)
{
    leveldb::MemoryDB db;
    short x;

    vector<short> e;


    leveldb::Sequence<short> a(db, "x");
    ASSERT_OK( a.Next(x) );
    EXPECT_EQ( 0, x );
    ASSERT_OK( a.Next(x) );
    EXPECT_EQ( 1, x );

    string v;

    leveldb::Sequence<short> b(db, "x");
    db.Get("x", v);
    ASSERT_OK( b.Next(x) );
    EXPECT_EQ( 2, x ) << "In db " << PrintToString(v);
    db.Get("x", v);
    ASSERT_OK( b.Next(x) );
    EXPECT_EQ( 3, x ) << "In db " << PrintToString(v);

    leveldb::Sequence<short> c(db, "x");
    ASSERT_OK( c.Next(x) );
    EXPECT_EQ( 4, x );
    ASSERT_OK( c.Next(x) );
    EXPECT_EQ( 5, x );
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

    auto w = walker(subtract(db, wh));

    w.SeekToFirst();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
}
