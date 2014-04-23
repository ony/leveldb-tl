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

            bool Valid() const { return impl != rows.end(); }

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
