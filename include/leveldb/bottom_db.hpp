#pragma once

#include <memory>

#include <leveldb/db.h>

#include <leveldb/any_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    class BottomDB final : public AnyDB
    {
        std::unique_ptr<DB> db;
    public:
        ~BottomDB() override = default;

        WriteOptions writeOptions;
        ReadOptions readOptions;
        Options options;

        const DB *operator*() const { return db.get(); }
        const DB *operator->() const { return db.get(); }
        DB *operator*() { return db.get(); }
        DB *operator->() { return db.get(); }

        Status Get(const Slice &key, std::string &value) noexcept override
        { return db->Get(readOptions, key, &value); }
        Status Put(const Slice &key, const Slice &value) noexcept override
        { return db->Put(writeOptions, key, value); }
        Status Delete(const Slice &key) noexcept override
        { return db->Delete(writeOptions, key); }

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return std::unique_ptr<Iterator>(db->NewIterator(readOptions)); }

        Status Open(const std::string &name)
        {
            DB *raw_db;
            Status s = DB::Open(options, name, &raw_db);
            if (s.ok()) db.reset(raw_db);
            else db.reset();
            return s;
        }

        Status Write(WriteBatch &updates)
        { return db->Write(writeOptions, &updates); }
    };

    // handle BottomDB as an AnyDB
    template<>
    struct Walker<BottomDB> : Walker<AnyDB>
    { using Walker<AnyDB>::Walker; };
}
