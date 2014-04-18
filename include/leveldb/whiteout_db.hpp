#pragma once

#include <set>
#include <leveldb/walker.hpp>

namespace leveldb
{
    class WhiteoutDB : protected std::set<std::string>
    {
        size_t rev = 0;
    public:
        template <typename... Args>
        WhiteoutDB(Args &&... args) : set(std::forward<Args>(args)...)
        {}
        WhiteoutDB(std::initializer_list<std::string> items) : set(items)
        {}

        using set::begin;
        using set::end;

        bool Check(const Slice &key)
        { return find(key.ToString()) != end(); }

        Status Insert(const Slice &key)
        {
            (void) insert(key.ToString());
            return Status::OK();
        }

        Status Delete(const Slice &key)
        {
            if (erase(key.ToString()) > 0) ++rev;
            return Status::OK();
        }

        Status Delete()
        {
            if (!empty()) ++rev;
            clear();
            return Status::OK();
        }

        class IteratorType
        {
            WhiteoutDB &rows;
            mutable WhiteoutDB::iterator impl;

            mutable size_t rev;
            mutable std::string savepoint;

            // re-sync with container (delete epoch)
            void Sync(bool setup = false) const
            {
                if (setup)
                {
                    rev = rows.rev;
                    // remember our key for further re-align if applicable
                    if (impl != rows.end())
                    { savepoint = *impl; }
                }
                else if (rev != rows.rev)
                {
                    rev = rows.rev;
                    // need to re-align on access if applicable
                    if (impl != rows.end())
                    { SeekImpl(savepoint); }
                }
            }

            void SeekImpl(const std::string &target) const
            { impl = rows.lower_bound(target); }

        public:
            IteratorType(WhiteoutDB &origin) : rows(origin), rev(origin.rev)
            {}

            bool Valid() const { Sync(); return impl != rows.end(); Sync(true); }

            void SeekToFirst() { impl = rows.begin(); Sync(true); }
            void SeekToLast() { impl = rows.end(); if (impl != rows.begin()) --impl; Sync(true); }
            void Seek(const Slice &target) { SeekImpl(target.ToString()); Sync(true); }

            void Next() { Sync(); ++impl; Sync(true); }
            void Prev() {
                Sync();
                if (impl == rows.begin()) impl = rows.end();
                else --impl;
                Sync(true);
            }

            Slice key() const { Sync(); return *impl; }

            Status status() const { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };
    };
}
