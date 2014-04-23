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
            typename Collection::Walker impl;
            size_t rev;
            size_t &revTxn;
            std::string savepoint;

            /// Sync walkers with collection.
            /// \return true if jumped one record forward
            bool Sync()
            {
                if (rev != revTxn)
                {
                    rev = revTxn;
                    if (Valid())
                    {
                        impl.Seek(savepoint);
                        assert( impl.key().compare(savepoint) >= 0 );
                        return impl.key().compare(savepoint) > 0;
                    }

                }
                return false;
            }

            void Synced()
            {
                rev = revTxn;
                if (Valid()) savepoint = impl.key().ToString();
            }
        public:
            Walker(TxnDB<Base> &txn) :
                impl({{txn.base, txn.whiteout}, txn.overlay}),
                rev(txn.rev),
                revTxn(txn.rev)
            {}

            bool Valid() const { return impl.Valid(); }
            Slice key() const { return impl.key(); }
            Slice value() const { return impl.value(); }
            Status status() const { return impl.status(); }

            void Seek(const Slice &target) { impl.Seek(target); Synced(); }
            void SeekToFirst() { impl.SeekToFirst(); Synced(); }
            void SeekToLast() { impl.SeekToLast(); Synced(); }
            void Next() { if (!Sync()) { impl.Next(); Synced(); } }
            void Prev() { Sync(); impl.Prev(); Synced(); }
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
