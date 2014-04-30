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

#include <string>
#include <memory>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

namespace leveldb
{
    class AnyDB
    {
    public:
        virtual ~AnyDB() noexcept = default;

        virtual Status Get(const Slice &key, std::string &value) noexcept = 0;
        virtual Status Put(const Slice &key, const Slice &value) noexcept = 0;
        virtual Status Delete(const Slice &key) noexcept = 0;

        virtual std::unique_ptr<Iterator> NewIterator() noexcept = 0;

        struct Walker;

    protected: // some common logic but implicitly disabled

        // general implementation (expect specialization)
        Status Write(WriteBatch &updates)
        {
            struct UpdateHandler : WriteBatch::Handler
            {
                AnyDB &db;
                UpdateHandler(AnyDB &origin) : db(origin) {}

                void Put(const Slice& key, const Slice& value) override
                { db.Put(key, value); }

                void Delete(const Slice& key) override
                { db.Delete(key); }

            } handler { *this };
            return updates.Iterate(&handler);
        }
    };

    /// Default implementation of iterator type for generic AnyDB.
    /// It's recommended to override in AnyDB implementation with a more
    /// specific and thus faster variant.
    struct AnyDB::Walker : std::unique_ptr<Iterator>
    {
        Walker() : unique_ptr() {}
        explicit Walker(Iterator *it) : unique_ptr(it) {}
        Walker(std::unique_ptr<Iterator> &&it) : unique_ptr(std::move(it)) {}
        Walker(AnyDB &db) : unique_ptr(db.NewIterator()) {}

        using unique_ptr::operator*;
        using unique_ptr::operator->;

        bool Valid() const { return (*this)->Valid(); }

        void SeekToFirst() { (*this)->SeekToFirst(); }
        void SeekToLast() { (*this)->SeekToLast(); }
        void Seek(const Slice &target) { (*this)->Seek(target); }

        void Next() { (*this)->Next(); }
        void Prev() { (*this)->Prev(); }

        Slice key() const { return (*this)->key(); }
        Slice value() const { return (*this)->value(); }
        Status status() const { return (*this)->status(); }
    };

    template <typename T>
    class AsIterator final : public Iterator
    {
        T impl;
    public:
        AsIterator(T &&origin) : impl(std::forward<T>(origin))
        {}

        bool Valid() const override { return impl.Valid(); }
        void SeekToFirst() override { impl.SeekToFirst(); }
        void SeekToLast() override { impl.SeekToLast(); }

        void Seek(const Slice &target) override
        { impl.Seek(target); }

        void Next() override { impl.Next(); }
        void Prev() override { impl.Prev(); }

        Slice key() const override { return impl.key(); }
        Slice value() const override { return impl.value(); }

        Status status() const override { return impl.status(); }
    };

    template <typename T>
    constexpr std::unique_ptr<AsIterator<T>> asIterator(T &&origin)
    { return std::unique_ptr<AsIterator<T>>(new AsIterator<T>(std::forward<T>(origin))); }

    template <typename T, typename... Args>
    constexpr std::unique_ptr<AsIterator<T>> asIterator(Args &&... args)
    { return std::unique_ptr<AsIterator<T>>(new AsIterator<T>(std::forward<Args>(args)...)); }
}
