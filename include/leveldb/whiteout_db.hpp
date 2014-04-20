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
        using std::set<std::string>::set;

        using set::begin;
        using set::end;

        bool Check(const Slice &key)
        { return find(key.ToString()) != end(); }

        template <typename... Args>
        Status Insert(Args &&... args)
        {
            (void) emplace(std::forward<Args>(args)...);
            return Status::OK();
        }

        Status Insert(const Slice &key)
        { return Insert(key.data(), key.size()); }

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
            Walker(WhiteoutDB &origin) : rows(origin), rev(origin.rev)
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
