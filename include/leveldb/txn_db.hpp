// @@@LICENSE
//
//      Copyright (c) 2014 Nikolay Orliuk <virkony@gmail.com>
//      Copyright (c) 2014 LG Electronics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// LICENSE@@@

#pragma once

#include <set>

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
    public:
        class Walker;
    private:
        Base &base;
        MemoryDB overlay;
        WhiteoutDB whiteout;
        std::set<Walker *> walkers;

        using Collection = Cover<Subtract<Base>, MemoryDB>;

    public:
        TxnDB(Base &origin) : base(origin)
        {}

        TxnDB(TxnDB &&origin) :
            base(origin.base),
            overlay(std::move(origin.overlay)),
            whiteout(std::move(origin.whiteout)),
            walkers(std::move(origin.walkers))
        { for (auto walker : walkers) walker->parentChanged(this); }

        TxnDB(const TxnDB &origin) :
            base(origin.base),
            overlay(origin.overlay),
            whiteout(origin.whiteout)
        {}

        ~TxnDB() noexcept override = default;

        TxnDB &operator=(const TxnDB &) = delete;
        TxnDB &operator=(TxnDB &&) = delete;

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
            Status s = overlay.Put(key, value);
            if (s.ok())
            {
                for (auto walker : walkers)
                { walker->overlayPut(key); }
            }
            return s;
        }

        Status Delete(const Slice &key) noexcept override
        {
            if (!whiteout.Insert(key)) return Status::OK(); // already deleted?
            for (auto walker : walkers) walker->overlayDelete(key);
            return overlay.Delete(key);
        }

        class Walker : public Collection::Walker
        {
            friend TxnDB;
            TxnDB *txn;
            typedef typename Collection::Walker Impl;

            void parentChanged(TxnDB *parent = nullptr)
            { txn = parent; }

        public:
            Walker(TxnDB<Base> &origin) :
                Impl({{origin.base, origin.whiteout}, origin.overlay}),
                txn(&origin)
            { txn->walkers.insert(this); }

            ~Walker()
            { if (txn) txn->walkers.erase(this); }

            Walker(Walker &&origin) :
                Impl(std::move(origin)),
                txn(origin.txn)
            {
                txn->walkers.erase(&origin);
                txn->walkers.insert(this);
                origin.txn = nullptr;
            }

            Walker(const Walker &origin) :
                Impl(origin),
                txn(origin.txn)
            { txn->walkers.insert(this); }

            Walker &operator=(const Walker &) = delete;
            Walker &operator=(Walker &&) = delete;
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
    { return { base }; }
}
