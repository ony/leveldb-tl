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

#include <set>
#include <leveldb/walker.hpp>

namespace leveldb
{
    class WhiteoutDB : protected std::set<std::string>
    {
        size_t rev = 0;
    public:
        WhiteoutDB() = default;

        template <typename... Args>
        WhiteoutDB(Args &&... args) :
            set(std::forward<Args>(args)...)
        {}

        WhiteoutDB(std::initializer_list<std::string> items) :
            set(items)
        {}

        using set::begin;
        using set::end;
        using set::empty;

        bool Check(const Slice &key)
        { return find(key.ToString()) != end(); }

        bool Insert(std::string &&key)
        {
            return insert(std::forward<std::string>(key)).second;
        }

        bool Insert(const Slice &key)
        { return Insert(key.ToString()); }

        template <typename... Args>
        Status Delete(Args &&... args)
        {
            if (erase(std::forward<Args>(args)...) > 0) ++rev;
            return Status::OK();
        }

        Status Delete(const Slice &key)
        { return Delete(key.ToString()); }

        Status Delete()
        {
            if (!empty()) ++rev;
            clear();
            return Status::OK();
        }

        class Walker
        {
            WhiteoutDB &rows;
            WhiteoutDB::iterator impl;

            size_t rev;
            std::string savepoint;

            // re-sync with container (delete epoch)
            bool Sync()
            {
                if (rev != rows.rev)
                {
                    rev = rows.rev;
                    // need to re-align on access if applicable
                    if (Valid())
                    { SeekImpl(savepoint); }
                    return true;
                }
                return false;
            }

            void Synced()
            {
                rev = rows.rev;
                // remember our key for further re-align if applicable
                if (Valid())
                { savepoint = *impl; }
            }

            void SeekImpl(const std::string &target)
            { impl = rows.lower_bound(target); }

        public:
            Walker(WhiteoutDB &origin) : rows(origin), rev(origin.rev)
            {}

            bool Valid() const { return rev == rows.rev && impl != rows.end(); }

            void SeekToFirst() { impl = rows.begin(); Synced(); }
            void SeekToLast() { impl = rows.end(); if (impl != rows.begin()) --impl; Synced(); }
            void Seek(const Slice &target) { SeekImpl(target.ToString()); Synced(); }

            void Next()
            {
                if (Sync()) return; // already moved to next record - nothing to do
                ++impl;
                Synced();
            }

            void Prev()
            {
                (void) Sync();
                // regardles if we point to next record after ghost or current
                // is still valid we should move backward one step
                if (impl == rows.begin()) impl = rows.end();
                else --impl;
                Synced();
            }

            Slice key() const { return *impl; }

            Status status() const { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };
    };
}
