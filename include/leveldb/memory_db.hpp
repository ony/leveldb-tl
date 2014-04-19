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

        class IteratorType
        {
            MemoryDB *rows;
            mutable MemoryDB::iterator impl; // may change after container change

            // in case of deletion in container
            mutable size_t rev;
            mutable std::string savepoint;

            // re-sync with container
            void Sync(bool setup = false) const
            {
                if (setup)
                {
                    rev = rows->rev;
                    // remember our key for further re-align if applicable
                    if (impl != rows->end())
                    { savepoint = impl->first; }
                }
                else if (rev != rows->rev)
                {
                    rev = rows->rev;
                    // need to re-align on access if applicable
                    if (impl != rows->end())
                    { SeekImpl(savepoint); }
                }
            }

            void SeekImpl(const std::string &target) const
            { impl = rows->lower_bound(target); }

        public:
            IteratorType(MemoryDB &origin) :
                rows(&origin),
                rev(origin.rev)
            {}

            /// Check that current entry is valid.
            /// You should validate this object before any other operation.
            ///
            /// \note Validity of this iterator may change with container
            ///       changing
            bool Valid() const { Sync(); return impl != rows->end(); }

            void SeekToFirst() { impl = rows->begin(); Sync(true); }

            void SeekToLast()
            {
                impl = rows->end();
                if (impl != rows->begin()) --impl;
                Sync(true);
            }

            void Seek(const Slice &target) { SeekImpl(target.ToString()); Sync(true); }

            void Next() { Sync(); ++impl; Sync(true); }
            void Prev() {
                Sync();
                if (impl == rows->begin()) impl = rows->end();
                else --impl;
                Sync(true);
            }

            Slice key() const { Sync(); return impl->first; }
            Slice value() const { Sync(); return impl->second; }

            Status status() const
            { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(IteratorType(*this)); }

        using AnyDB::Write;
    };
}

