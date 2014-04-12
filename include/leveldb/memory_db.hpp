#pragma once

#include <map>
#include <string>

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
            (void) emplace(key.ToString(), value.ToString());
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
            void Prev() { --impl; }

            Slice key() const { return impl->first; }
            Slice value() const { return impl->second; }

            Status status() const
            { return Valid() ? Status::OK() : Status::NotFound("invalid iterator"); }
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(IteratorType(*this)); }
    };
}

