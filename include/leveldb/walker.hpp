#pragma once

#include <map>
#include <memory>

#include <leveldb/iterator.h>

#include <leveldb/any_db.hpp>

namespace leveldb
{
    template <typename T>
    class Walker
    {
        typename T::IteratorType impl;

    public:
        Walker(T &db) : impl(db)
        {}

        Iterator* operator->() { return &impl; }

        bool Valid() const { return impl.Valid(); }

        void SeekToFirst() { impl.SeekToFirst(); }
        void SeekToLast() { impl.SeekToLast(); }
        void Seek(const Slice &target) { impl.Seek(target); }

        void Next() { impl.Next(); }
        void Prev() { impl.Prev(); }

        Slice key() const { return impl.key(); }
        Slice value() const { return impl.value(); }
        Status status() const { return impl.status(); }
    };

    template <typename T>
    constexpr Walker<T> walker(T &collection)
    { return Walker<T>(collection); }

    template <typename T>
    constexpr Walker<T> walker(const T &collection)
    { return Walker<T>(collection); }

    template <>
    class Walker<AnyDB>
    {
        std::unique_ptr<Iterator> impl;
    public:
        Walker(AnyDB &db) : impl(db.NewIterator())
        {}

        Iterator &operator*() { return *impl; }
        Iterator* operator->() { return impl.get(); }

        bool Valid() const { return impl->Valid(); }

        void SeekToFirst() { impl->SeekToFirst(); }
        void SeekToLast() { impl->SeekToLast(); }
        void Seek(const Slice &target) { impl->Seek(target); }

        void Next() { impl->Next(); }
        void Prev() { impl->Prev(); }

        Slice key() const { return impl->key(); }
        Slice value() const { return impl->value(); }
        Status status() const { return impl->status(); }
    };

}
