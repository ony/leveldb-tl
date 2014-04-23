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
        size_t rev = 0;

        using Collection = Cover<Subtract<Base>, MemoryDB>;

    public:
        TxnDB(Base &origin) : base(origin)
        {}

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
            ++rev;
            (void) whiteout.Delete(key);
            return overlay.Put(key, value);
        }

        Status Delete(const Slice &key) noexcept override
        {
            ++rev;
            whiteout.Insert(key);
            return overlay.Delete(key);
        }

        class Walker
        {
            mutable typename Collection::Walker impl;
            mutable size_t rev;
            size_t &revTxn;

            void Sync(bool setup = false) const
            {
                if (setup)
                {
                    rev = revTxn;
                }
                else if (rev != revTxn)
                {
                    rev = revTxn;
                    impl.Seek(impl.key());
                }
            }
        public:
            Walker(TxnDB<Base> &txn) :
                impl({{txn.base, txn.whiteout}, txn.overlay}),
                rev(txn.rev),
                revTxn(txn.rev)
            {}

            bool Valid() const { Sync(); return impl.Valid(); }
            Slice key() const { Sync(); return impl.key(); }
            Slice value() const { Sync(); return impl.value(); }
            Status status() const { Sync(); return impl.status(); }

            void Seek(const Slice &target) { impl.Seek(target); Sync(true); }
            void SeekToFirst() { impl.SeekToFirst(); Sync(true); }
            void SeekToLast() { impl.SeekToLast(); Sync(true); }
            void Next() { Sync(); impl.Next(); Sync(true); }
            void Prev() { Sync(); impl.Prev(); Sync(true); }
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return asIterator(Walker(*this)); }

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
