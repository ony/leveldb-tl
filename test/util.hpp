#pragma once

#include <leveldb/slice.h>

#include <gtest/gtest.h>

namespace leveldb {
    void PrintTo(const Slice &s, ::std::ostream *os)
    { *os << ::testing::PrintToString(s.ToString()); }
}

::testing::AssertionResult ok(leveldb::Status s)
{
    if (s.ok()) return ::testing::AssertionSuccess() << s.ToString();
    else return ::testing::AssertionFailure() << s.ToString();
}

#define ASSERT_OK(E) ASSERT_TRUE(ok(E))
#define EXPECT_OK(E) EXPECT_TRUE(ok(E))

#define ASSERT_FAIL(E) ASSERT_FALSE(ok(E))
#define EXPECT_FAIL(E) EXPECT_FALSE(ok(E))
