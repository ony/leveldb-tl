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

#include <memory>

#include <leveldb/db.h>

#include <leveldb/any_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    struct BottomDB final : std::unique_ptr<DB>, AnyDB
    {
        BottomDB() = default;
        BottomDB(BottomDB &&) = default;

        template <typename... Args>
        BottomDB(Args &&... args) :
            unique_ptr(std::forward<Args>(args)...)
        {}

        ~BottomDB() noexcept override = default;

        BottomDB &operator=(BottomDB &&) = default;

        WriteOptions writeOptions;
        ReadOptions readOptions;
        Options options;

        Status Get(const Slice &key, std::string &value) noexcept override
        { return (*this)->Get(readOptions, key, &value); }
        Status Put(const Slice &key, const Slice &value) noexcept override
        { return (*this)->Put(writeOptions, key, value); }
        Status Delete(const Slice &key) noexcept override
        { return (*this)->Delete(writeOptions, key); }

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return std::unique_ptr<Iterator>((*this)->NewIterator(readOptions)); }

        Status Open(const std::string &name)
        {
            DB *raw_db;
            Status s = DB::Open(options, name, &raw_db);
            if (s.ok()) reset(raw_db);
            else reset();
            return s;
        }

        Status Write(WriteBatch &updates)
        { return (*this)->Write(writeOptions, &updates); }
    };
}
