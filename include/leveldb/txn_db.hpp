#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/memory_db.hpp>
#include <leveldb/whiteout_db.hpp>
#include <leveldb/cover_walker.hpp>

namespace leveldb
{
    // note that Base object should outlive transaction
    template<typename Base = AnyDB>
    class TxnDB final : public AnyDB
    {
        Base &base;
        MemoryDB overlay;
        WhiteoutDB whiteout;

        using Collection = Cover<Subtract<Base>, MemoryDB>;

    public:
        TxnDB(Base &origin) : base(origin)
        {}

        ~TxnDB() noexcept override = default;

        Status Get(const Slice &key, std::string &value) noexcept override
        {
            if (whiteout.Check(key))
            { return Status::NotFound("Deleted in transaction", key); }
            auto s = overlay.Get(key, value);
            if (s.ok()) return s;
            return base.Get(key, value);
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            (void) whiteout.Delete(key);
            return overlay.Put(key, value);
        }

        Status Delete(const Slice &key) noexcept override
        {
            whiteout.Insert(key);
            return overlay.Delete(key);
        }

        class IteratorType : public Walker<Collection>
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
                overlay.Delete();
                whiteout.Delete();
            }
            return s;
        }

        void reset()
        {
            overlay.Delete();
            whiteout.Delete();
        }

        using AnyDB::Write;
    };

    template <typename Base>
    constexpr TxnDB<Base> transaction(Base &base)
    { return base; }
}
