#pragma once

#include <map>
#include <string>

#include <leveldb/write_batch.h>

#include <leveldb/any_db.hpp>

namespace leveldb
{
    class MemoryDB final : private std::map<std::string, std::string>, public AnyDB
    {
        size_t rev = 0;
    public:
        using map::map;

        ~MemoryDB() noexcept override = default;

        using map::size;
        using map::begin;
        using map::end;

        Status Get(const Slice &key, std::string &value) noexcept override
        {
            auto it = find(key.ToString());
            if (it == end()) return Status::NotFound("key not found", key);
            value = it->second;
            return Status::OK();
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            auto v = value.ToString();
            auto r = emplace(key.ToString(), v);
            if (!r.second) r.first->second = v; // overwrite
            return Status::OK();
        }

        Status Delete(const Slice &key) noexcept override
        {
            if (erase(key.ToString()) > 0) ++rev;
            return Status::OK();
        }

        void Delete()
        {
            if (empty()) return;
            ++rev;
            clear();
        }

        class Walker
        {
            MemoryDB *rows;
            MemoryDB::iterator impl; // may change after container change

            // in case of deletion in container
            size_t rev;
            std::string savepoint;

            // re-sync with container if needed
            bool Sync()
            {
                if (rev != rows->rev)
                {
                    rev = rows->rev;
                    // need to re-align on access if applicable
                    if (Valid())
                    { SeekImpl(savepoint); }
                    return true;
                }
                return false;
            }

            void Synced()
            {
                rev = rows->rev;
                // remember our key for further re-align if applicable
                if (Valid())
                { savepoint = impl->first; }
            }

            void SeekImpl(const std::string &target)
            { impl = rows->lower_bound(target); }

        public:
            Walker(MemoryDB &origin) :
                rows(&origin),
                rev(origin.rev)
            {}

            /// Check that current entry is valid.
            /// You should validate this object before any other operation of
            /// accessing data or relative movement.
            ///
            /// \note Validity of this iterator may change with container
            ///       changing
            /// \note In case if iterator points to ghost record we should step
            ///       away from it. That's the only case when Valid shouldn't
            ///       be called and any movement must be performed before.
            bool Valid() const { return impl != rows->end(); }

            void SeekToFirst() { impl = rows->begin(); Synced(); }

            void SeekToLast()
            {
                impl = rows->end();
                if (impl != rows->begin()) --impl;
                Synced();
            }

            void Seek(const Slice &target) { SeekImpl(target.ToString()); Synced(); }

            void Next()
            {
                if (Sync()) return; // already pointing to next record
                ++impl;
                Synced();
            }
            void Prev() {
                (void) Sync();
                // no matter if Sync() automatically moved us forward or not we
                // should move backward to get previous record
                if (impl == rows->begin()) impl = rows->end();
                else --impl;
                Synced();
            }

            Slice key() const { return impl->first; }
            Slice value() const { return impl->second; }

            Status status() const { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(Walker(*this)); }

        using AnyDB::Write;
    };
}

