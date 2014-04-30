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

#pragma once

#include <leveldb/slice.h>

#include <gtest/gtest.h>

namespace leveldb {
    inline void PrintTo(const Slice &s, ::std::ostream *os)
    { *os << ::testing::PrintToString(s.ToString()); }
}

inline ::testing::AssertionResult check(
    const leveldb::Status &s,
    std::function<bool(const leveldb::Status &)> pred = [](const leveldb::Status &s) { return s.ok(); }
)
{
    if (pred(s)) return ::testing::AssertionSuccess() << s.ToString();
    else return ::testing::AssertionFailure() << s.ToString();
}

constexpr bool hostIsNetwork()
{ return *reinterpret_cast<const short*>("\x01\x02") == 0x0102; }

#define ASSERT_OK(E) ASSERT_TRUE(check(E))
#define EXPECT_OK(E) EXPECT_TRUE(check(E))

#define ASSERT_FAIL(E) ASSERT_FALSE(check(E))
#define EXPECT_FAIL(E) EXPECT_FALSE(check(E))

#define ASSERT_STATUS(M, E) ASSERT_TRUE(check(E, [](const leveldb::Status &s) { return s.Is ## M(); }))
#define EXPECT_STATUS(M, E) EXPECT_TRUE(check(E, [](const leveldb::Status &s) { return s.Is ## M(); }))
