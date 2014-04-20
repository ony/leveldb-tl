#include "leveldb/txn_db.hpp"
#include "leveldb/memory_db.hpp"

#include <gtest/gtest.h>

#include "util.hpp"

using namespace std;

class TestTxn : public ::testing::TestWithParam<string>
{
protected:
    leveldb::MemoryDB db;
    using TxnType = leveldb::TxnDB<leveldb::MemoryDB>;
    TxnType txn = transaction(db);
    TxnType::Walker w = walker(txn);
    vector<pair<string,string>> e; // expected key/val
private:
    void SetUp()
    {
        string k = "a";
        string v = "0";
        // fill database and expected view
        for (char c : GetParam())
        {
            switch (c)
            {
            case '<': db.Put(k, v); break;
            case '>': db.Put(k, v); break;
            case '-': db.Put(k, v); ++v[0]; txn.Put(k, v); break;
            case 'x': db.Put(k, v); ++v[0]; txn.Delete(k); break;
            case 'X': txn.Delete(k); break;
            }
            if (c != 'x' && c != 'X') e.emplace_back(k,v);

            ++k[0]; ++v[0];
        }
    }
};

namespace {
    template <size_t n>
    const vector<string> &genCases()
    {
        static vector<string> ys;
        if (!ys.empty()) return ys;

        for (auto x : genCases<n-1>())
        {
            for (char c : { '<', '>', '-', 'x', 'X' })
                ys.push_back(x + c);
        }

        return ys;
    }

    template<>
    const vector<string> &genCases<0>()
    {
        static const vector<string> ys { string{} };
        return ys;
    }
}

INSTANTIATE_TEST_CASE_P(Comb0, TestTxn, ::testing::ValuesIn(genCases<0>()));
INSTANTIATE_TEST_CASE_P(Comb1, TestTxn, ::testing::ValuesIn(genCases<1>()));
INSTANTIATE_TEST_CASE_P(Comb2, TestTxn, ::testing::ValuesIn(genCases<2>()));
INSTANTIATE_TEST_CASE_P(Comb3, TestTxn, ::testing::ValuesIn(genCases<3>()));
INSTANTIATE_TEST_CASE_P(Comb8, TestTxn, ::testing::ValuesIn(genCases<5>()));

TEST_P(TestTxn, forward)
{
    w.SeekToFirst();

    for (const auto &p : e)
    {
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );

        w.Next();
    }

    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
}

TEST_P(TestTxn, backward)
{
    w.SeekToLast();

    for (auto i = e.rbegin(); i != e.rend(); ++i)
    {
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( i->first, w.key() );
        EXPECT_EQ( i->second, w.value() );

        w.Prev();
    }

    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString() << "\n";
}

TEST_P(TestTxn, seek_first_prev)
{
    if (e.empty()) return;

    w.SeekToFirst();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( e.front().first, w.key() );
    EXPECT_EQ( e.front().second, w.value() );

    w.Prev();
    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString() << "\n";
}

TEST_P(TestTxn, seek_last_next)
{
    if (e.empty()) return;

    w.SeekToLast();
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( e.back().first, w.key() );
    EXPECT_EQ( e.back().second, w.value() );

    w.Next();
    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString() << "\n";
}

TEST_P(TestTxn, sowtooth_forward)
{
    if (e.size() < 2) return;

    auto i = e.begin();
    w.SeekToFirst();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( i->first, w.key() );
    EXPECT_EQ( i->second, w.value() );

    ++i; w.Next();

    for (;i != e.end(); i+=2, w.Next(), w.Next())
    {
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( i->first, w.key() );
        EXPECT_EQ( i->second, w.value() );

        --i; w.Prev();
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( i->first, w.key() );
        EXPECT_EQ( i->second, w.value() );
    }

    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString() << "\n";
}

TEST_P(TestTxn, sowtooth_backward)
{
    if (e.size() < 2) return;

    auto i = e.rbegin();
    w.SeekToLast();

    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( i->first, w.key() );
    EXPECT_EQ( i->second, w.value() );

    ++i; w.Prev();

    for (;i != e.rend(); i+=2, w.Prev(), w.Prev())
    {
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( i->first, w.key() );
        EXPECT_EQ( i->second, w.value() );

        --i; w.Next();
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( i->first, w.key() );
        EXPECT_EQ( i->second, w.value() );
    }

    EXPECT_FALSE( w.Valid() )
        << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString() << "\n";
}

TEST_P(TestTxn, seek_for_first)
{
    if (e.empty()) return;

    const auto &p = e.front();

    w.Seek(p.first);
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( p.first, w.key() );
    EXPECT_EQ( p.second, w.value() );
}

TEST_P(TestTxn, seek_for_last)
{
    if (e.empty()) return;

    const auto &p = e.back();

    w.Seek(p.first);
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( p.first, w.key() );
    EXPECT_EQ( p.second, w.value() );
}

TEST_P(TestTxn, seek_for_third)
{
    if (e.size() < 3) return;

    const auto &p = e[2];

    w.Seek(p.first);
    ASSERT_TRUE( w.Valid() );
    EXPECT_EQ( p.first, w.key() );
    EXPECT_EQ( p.second, w.value() );
}

TEST_P(TestTxn, seek_for_fuzzy_fourth)
{
    if (e.size() < 3) return;

    w.Seek(e[2].first + "1");

    if (e.size() == 3)
    {
        EXPECT_FALSE( w.Valid() );
    }
    else
    {
        const auto &p = e[3];
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );
    }
}

TEST_P(TestTxn, seek_for_fuzzy_first)
{
    w.Seek("0"); // something before first ("a")

    if (e.empty())
    {
        EXPECT_FALSE( w.Valid() );
    }
    else
    {
        const auto &p = e[0];
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );
    }
}

TEST_P(TestTxn, seek_for_fuzzy_max)
{
    w.Seek("zzz"); // something after last entry

    EXPECT_FALSE( w.Valid() );
}
