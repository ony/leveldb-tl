#include "leveldb/txn_db.hpp"
#include "leveldb/sandwich_db.hpp"
#include "leveldb/memory_db.hpp"

#include <gtest/gtest.h>

#include "util.hpp"

using namespace std;
using namespace leveldb;
using ::testing::PrintToString;

class TestSandwich : public ::testing::TestWithParam<string>
{
protected:
    MemoryDB db;

    // SandwichDB<TxnDB<MemoryDB>> sdb { db };
    SandwichDB<MemoryDB> sdb { db };

    using Expectation = vector<pair<string,string>>;
    vector<Expectation> es; // expected key/val for each part

private:
    void SetUp()
    {
        string k = "a";
        string v = "0";
        // fill database and expected view
        
        for (char c : GetParam())
        {
            size_t i;
            string name = "a";
            switch (c)
            {
            case 'a' ... 'z':
                i = c - 'a';
                if (i >= es.size()) es.resize(i+1);
                es[i].emplace_back(k, v);
                name[0] = c;
                sdb.use(name).Put(k, v);
                break;
            }

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
            for (char c = 'a'; c <= 'f'; ++c)
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

INSTANTIATE_TEST_CASE_P(Comb0, TestSandwich, ::testing::ValuesIn(genCases<0>()));
INSTANTIATE_TEST_CASE_P(Comb1, TestSandwich, ::testing::ValuesIn(genCases<1>()));
INSTANTIATE_TEST_CASE_P(Comb2, TestSandwich, ::testing::ValuesIn(genCases<2>()));
INSTANTIATE_TEST_CASE_P(Comb3, TestSandwich, ::testing::ValuesIn(genCases<3>()));
INSTANTIATE_TEST_CASE_P(Comb8, TestSandwich, ::testing::ValuesIn(genCases<5>()));

TEST_P(TestSandwich, forward)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

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
}

TEST_P(TestSandwich, backward)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        w.SeekToLast();

        for (auto i = e.rbegin(); i != e.rend(); ++i)
        {
            ASSERT_TRUE( w.Valid() );
            EXPECT_EQ( i->first, w.key() );
            EXPECT_EQ( i->second, w.value() );

            w.Prev();
        }

        EXPECT_FALSE( w.Valid() )
            << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
    }
}

TEST_P(TestSandwich, seek_first_prev)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.empty()) continue;

        w.SeekToFirst();
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( e.front().first, w.key() );
        EXPECT_EQ( e.front().second, w.value() );

        w.Prev();
        EXPECT_FALSE( w.Valid() )
            << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
    }
}


TEST_P(TestSandwich, seek_last_next)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.empty()) continue;

        w.SeekToLast();
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( e.back().first, w.key() );
        EXPECT_EQ( e.back().second, w.value() );

        w.Next();
        EXPECT_FALSE( w.Valid() )
            << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
    }
}

TEST_P(TestSandwich, sowtooth_forward)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.size() < 2) continue;

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
            << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
    }
}

TEST_P(TestSandwich, sowtooth_backward)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.size() < 2) continue;

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
            << "Walker still points to " << w.key().ToString() << "=" << w.value().ToString();
    }
}

TEST_P(TestSandwich, seek_for_first)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.empty()) continue;

        const auto &p = e.front();

        w.Seek(p.first);
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );
    }
}

TEST_P(TestSandwich, seek_for_last)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.empty()) continue;

        const auto &p = e.back();

        w.Seek(p.first);
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );
    }
}

TEST_P(TestSandwich, seek_for_third)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.size() < 3) continue;

        const auto &p = e[2];

        w.Seek(p.first);
        ASSERT_TRUE( w.Valid() );
        EXPECT_EQ( p.first, w.key() );
        EXPECT_EQ( p.second, w.value() );
    }
}


TEST_P(TestSandwich, seek_for_fuzzy_fourth)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        if (e.size() < 3) continue;

        // "c" might be deleted, build from e[2]
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
}

TEST_P(TestSandwich, seek_for_fuzzy_first)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

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
}

TEST_P(TestSandwich, seek_for_fuzzy_max)
{
    string n = "a";
    for (const auto &e : es)
    {
        SCOPED_TRACE("Sandwich part: " + n);

        auto t = sdb.use(n);
        auto w = walker(t);

        ++n[0];

        w.Seek("zzz"); // something after last entry

        EXPECT_FALSE( w.Valid() );
    }
}
