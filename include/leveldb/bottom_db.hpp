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
        using std::unique_ptr<DB>::unique_ptr;

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

    // handle BottomDB as an AnyDB
    template<>
    struct Walker<BottomDB> : Walker<AnyDB>
    { using Walker<AnyDB>::Walker; };
}
