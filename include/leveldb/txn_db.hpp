#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/memory_db.hpp>
#include <leveldb/cover_walker.hpp>

namespace leveldb
{
    template<typename Base>
    class TxnDB final : public AnyDB
    {
        Base &base;
        MemoryDB overlay;
        Whiteout whiteout;

        using Collection = Cover<Subtract<Base>, MemoryDB>;

    public:
        TxnDB(Base &origin) : base(origin)
        {}

        ~TxnDB() noexcept override = default;

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
            whiteout.insert(key.ToString());
            return overlay.Delete(key);
        }

        class IteratorType final : public Walker<Collection>
        {
        public:
            IteratorType(TxnDB<Base> &txn) :
                Walker<Collection>({{txn.base, txn.whiteout}, txn.overlay})
            {}
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(IteratorType(*this)); }

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

        using AnyDB::Write;
    };

    template <typename Base>
    constexpr TxnDB<Base> transaction(Base &base)
    { return base; }
}
