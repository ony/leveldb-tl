#pragma once

#include <string>
#include <memory>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

namespace leveldb
{
    class AnyDB
    {
    public:
        virtual ~AnyDB() noexcept = default;

        virtual Status Get(const Slice &key, std::string &value) noexcept = 0;
        virtual Status Put(const Slice &key, const Slice &value) noexcept = 0;
        virtual Status Delete(const Slice &key) noexcept = 0;

        virtual std::unique_ptr<Iterator> NewIterator() noexcept = 0;

    protected: // some common logic but implicitly disabled

        // general implementation (expect specialization)
        Status Write(WriteBatch &updates)
        {
            struct UpdateHandler : WriteBatch::Handler
            {
                AnyDB &db;
                UpdateHandler(AnyDB &origin) : db(origin) {}

                void Put(const Slice& key, const Slice& value) override
                { db.Put(key, value); }

                void Delete(const Slice& key) override
                { db.Delete(key); }

            } handler { *this };
            return updates.Iterate(&handler);
        }
    };

    template <typename T>
    class AsIterator final : public Iterator
    {
        T impl;
    public:
        AsIterator(T &&origin) : impl(std::forward<T>(origin))
        {}

        bool Valid() const override { return impl.Valid(); }
        void SeekToFirst() override { impl.SeekToFirst(); }
        void SeekToLast() override { impl.SeekToLast(); }

        void Seek(const Slice &target) override
        { impl.Seek(target); }

        void Next() override { impl.Next(); }
        void Prev() override { impl.Prev(); }

        Slice key() const override { return impl.key(); }
        Slice value() const override { return impl.value(); }

        Status status() const override { return impl.status(); }
    };

    template <typename T>
    constexpr std::unique_ptr<AsIterator<T>> asIterator(T &&origin)
    { return std::unique_ptr<AsIterator<T>>(new AsIterator<T>(std::forward<T>(origin))); }

    template <typename T, typename... Args>
    constexpr std::unique_ptr<AsIterator<T>> asIterator(Args &&... args)
    { return std::unique_ptr<AsIterator<T>>(new AsIterator<T>(std::forward<Args>(args)...)); }
}
