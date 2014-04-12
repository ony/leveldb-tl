#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/memory_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    template<typename Base>
    class TxnDB final : public AnyDB
    {
        Base &base;
        MemoryDB overlay;
        Whiteout whiteout;

    public:
        TxnDB(Base &origin) : base(origin)
        {}

        Status Get(const Slice &key, std::string &value) noexcept override
        {
            if (whiteout.find(key.ToString()) != whiteout.end())
            { return Status::NotFound("Deleted in transaction", key); }
            auto s = overlay.Get(key, value);
            if (s.ok()) return s;
            return base.Get(key, value);
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            whiteout.erase(key.ToString());
            return overlay.Put(key, value);
        }

        Status Delete(const Slice &key) noexcept override
        {
            whiteout.emplace(key.data(), key.size());
            return overlay.Delete(key);
        }

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return nullptr; } // TODO: implement

        Status commit()
        {
            WriteBatch batch;
            for (auto k : whiteout) batch.Delete(k);
            for (auto kv : overlay) batch.Put(kv.first, kv.second);
            Status s = base.Write(batch);
            if (s.ok())
            {
                overlay.clear();
                whiteout.clear();
            }
            return s;
        }
    };
}
