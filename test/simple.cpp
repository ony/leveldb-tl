#include "leveldb/sandwich_db.hpp"
#include "leveldb/bottom_db.hpp"
#include "leveldb/walker.hpp"
#include "leveldb/cover_walker.hpp"
#include "leveldb/memory_db.hpp"
#include "leveldb/txn_db.hpp"
#include "leveldb/ref_db.hpp"

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
    leveldb::WhiteoutDB wh { "b", "a", "c" };

    auto w = leveldb::walker(wh);
    auto x = w; // copy
    (void) x;

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

    leveldb::WhiteoutDB wh { "b" };

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
    TxnType::Walker i(txn);

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

    leveldb::Sequence<unsigned short> a(db, "x");
    ASSERT_OK( a.Next(x) );
    EXPECT_EQ( 0, x );
    ASSERT_OK( a.Next(x) );
    EXPECT_EQ( 1, x );
    EXPECT_OK( a.Sync() );

    string v;

    leveldb::Sequence<unsigned short> z(std::move(a));
    leveldb::Sequence<unsigned short> b(db, "x");
    EXPECT_OK( db.Get("x", v) );
    ASSERT_OK( b.Next(x) );
    EXPECT_EQ( 2, x ) << "In db " << PrintToString(v);
    db.Get("x", v);
    ASSERT_OK( b.Next(x) );
    EXPECT_EQ( 3, x ) << "In db " << PrintToString(v);
    EXPECT_OK( b.Sync() );

    leveldb::Sequence<short> c(db, "x");
    ASSERT_OK( c.Next(x) );
    EXPECT_EQ( 4, x );
    ASSERT_OK( c.Next(x) );
    EXPECT_EQ( 5, x );
    EXPECT_OK( c.Sync() );
}

TEST(Simple, host_order)
{
    leveldb::host_order<unsigned short> x { leveldb::Slice("\x42\x43", 2) };
    EXPECT_EQ( "\x42\x43", leveldb::Slice(x) );
    x.next_net();
    EXPECT_EQ( "\x42\x44", leveldb::Slice(x) );
}

TEST(Simple, sandwich)
{
    leveldb::SandwichDB<leveldb::MemoryDB> sdb;
    auto txn = sdb.ref<leveldb::TxnDB>();

    auto a = txn.use("alpha");
    ASSERT_TRUE( a.Valid() );
    txn.use("gamma").Put("x", "z");
    auto b = txn.use("beta");
    ASSERT_TRUE( b.Valid() );
    EXPECT_OK( txn->commit() );

    string v;

    EXPECT_FAIL( a.Get("a", v) );
    EXPECT_FAIL( b.Get("a", v) );
    EXPECT_OK( a.Put("a", "1") );
    EXPECT_OK( a.Put("b", "3") );
    EXPECT_OK( b.Put("b", "2") );

    ASSERT_OK( a.Get("a", v) );
    EXPECT_EQ( "1", v );
    EXPECT_FAIL( b.Get("a", v) );

    auto c = txn.use("alpha");
    ASSERT_OK( c.Get("a", v) );
    EXPECT_EQ( "1", v );

    EXPECT_OK( sdb.Sync() );
    EXPECT_OK( txn.Sync() );

    auto w = walker(a);

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    ASSERT_FALSE( w.Valid() ) << "Walker still points to " << PrintToString(w.key());
    EXPECT_FAIL( w.status() );

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "3", w.value() );

    EXPECT_OK( a.Delete("a") );
    w.Prev();
    EXPECT_FALSE( w.Valid() );
    EXPECT_FAIL( w.status() );
}

TEST(Simple, big_sandwich)
{
    leveldb::SandwichDB<leveldb::MemoryDB> sdb;

    auto a = sdb.use("alpha");

    // make it bigger by stuffing in between layers
    for (size_t i = 1; i < 0x0200; ++i)
    {
        (void) sdb.use(to_string(i));
    }

    ASSERT_TRUE( a.Valid() );
    sdb.use("gamma").Put("x", "z");
    auto b = sdb.use("beta");
    ASSERT_TRUE( b.Valid() );

    EXPECT_OK( sdb.Sync() );

    string v;

    EXPECT_FAIL( a.Get("a", v) );
    EXPECT_FAIL( b.Get("a", v) );
    EXPECT_OK( a.Put("a", "1") );
    EXPECT_OK( a.Put("b", "3") );
    EXPECT_OK( b.Put("b", "2") );

    ASSERT_OK( a.Get("a", v) );
    EXPECT_EQ( "1", v );
    EXPECT_FAIL( b.Get("a", v) );

    auto c = sdb.use("alpha");
    ASSERT_OK( c.Get("a", v) );
    EXPECT_EQ( "1", v );

    auto w = walker(a);

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "1", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    ASSERT_FALSE( w.Valid() ) << "Walker still points to " << PrintToString(w.key());
    EXPECT_FAIL( w.status() );

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "3", w.value() );

    EXPECT_OK( a.Delete("a") );
    w.Prev();
    EXPECT_FALSE( w.Valid() );
    EXPECT_FAIL( w.status() );
}

TEST(Simple, ref)
{
    leveldb::SandwichDB<leveldb::MemoryDB> sdb0;
    auto sdb = sdb0.ref();
    auto txn = sdb.ref<leveldb::TxnDB>();

    auto a = sdb.use("x");
    auto b = a.ref(txn);

    ASSERT_TRUE( a.Valid() );
    ASSERT_TRUE( b.Valid() );

    string v;

    EXPECT_OK( a.Put("a", "0") );
    EXPECT_OK( b.Put("a", "1") );
    EXPECT_OK( a.Put("b", "2") );
    EXPECT_OK( a.Put("c", "3") );
    EXPECT_OK( b.Put("d", "4") );

    EXPECT_OK( a.Get("a", v) );
    EXPECT_EQ( "0", v );
    EXPECT_OK( b.Get("a", v) );
    EXPECT_EQ( "1", v );

    EXPECT_OK( a.Get("b", v) );
    EXPECT_EQ( "2", v );
    EXPECT_OK( b.Get("b", v) );
    EXPECT_EQ( "2", v );

    EXPECT_OK( a.Get("c", v) );
    EXPECT_EQ( "3", v );
    EXPECT_OK( b.Get("c", v) );
    EXPECT_EQ( "3", v );

    EXPECT_FAIL( a.Get("d", v) );
    EXPECT_OK( b.Get("d", v) );
    EXPECT_EQ( "4", v );

    auto w = leveldb::walker(a);

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "a", w.key() );
    EXPECT_EQ( "0", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "b", w.key() );
    EXPECT_EQ( "2", w.value() );

    w.Next();
    ASSERT_TRUE( w.Valid() );
    EXPECT_OK( w.status() );
    EXPECT_EQ( "c", w.key() );
    EXPECT_EQ( "3", w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() );
    EXPECT_FAIL( w.status() );
}

TEST(Simple, DISABLED_dummy)
{
    leveldb::SandwichDB<leveldb::BottomDB> sdb;
    sdb->options.create_if_missing = true;
    ASSERT_OK( sdb->Open("/tmp/test.ldb") );

    auto txn = sdb.ref<leveldb::TxnDB>();

    auto alpha = sdb.use("alpha");
    auto db = alpha.ref(txn);

    EXPECT_OK( db.Put("a", "1") );
    EXPECT_OK( db.Put("b", "2") );
    EXPECT_OK( db.Put("c", "4") );


    leveldb::WhiteoutDB wh { "b" };
    leveldb::MemoryDB mem { { "d", "5" } };

    auto w = walker(subtract(db, wh));

    w.SeekToFirst();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "a", w.key() );

    w.Next();
    EXPECT_TRUE( w.Valid() );
    EXPECT_EQ( "c", w.key() );
}
