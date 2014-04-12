#pragma once

#include <map>
#include <string>

#include <leveldb/write_batch.h>

#include <leveldb/any_db.hpp>

namespace leveldb
{
    class MemoryDB final : public std::map<std::string, std::string>, public AnyDB
    {
    public:
        using std::map<std::string, std::string>::map;

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
            (void) erase(key.ToString());
            return Status::OK();
        }

        class IteratorType final
        {
            friend class MemoryDB;

            MemoryDB *rows;
            MemoryDB::iterator impl;

        public:
            IteratorType(MemoryDB &origin) : rows(&origin)
            {}

            bool Valid() const { return impl != rows->end(); }

            void SeekToFirst() { impl = rows->begin(); }

            void SeekToLast()
            { impl = rows->end(); if (impl != rows->begin()) --impl; }

            void Seek(const Slice &target)
            { impl = rows->lower_bound(target.ToString()); }

            void Next() { ++impl; }
            void Prev() {
                if (impl == rows->begin()) impl = rows->end();
                else --impl;
            }

            Slice key() const { return impl->first; }
            Slice value() const { return impl->second; }

            Status status() const
            { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(IteratorType(*this)); }

        Status Write(WriteBatch &updates)
        {
            struct UpdateHandler : WriteBatch::Handler
            {
                MemoryDB *db;

                void Put(const Slice& key, const Slice& value) override
                { db->Put(key, value); }
                void Delete(const Slice& key) override
                { db->Delete(key); }
            } handler;
            handler.db = this;
            return updates.Iterate(&handler);
        }
    };
}

