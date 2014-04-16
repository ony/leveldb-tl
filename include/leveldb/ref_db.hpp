#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    // Reference to some existing database under terms that Impl object
    // outlives this reference.
    // Usaful for putting multiple different stuff on top of some existing
    // i.e. to have both transactional sandwich and non-transactional over same
    // bottom
    template <typename Impl = AnyDB>
    class RefDB final : public AnyDB
    {
        Impl &impl;
    public:
        RefDB(Impl &origin) : impl(origin) {}

        Status Get(const Slice &key, std::string &value) noexcept override
        { return impl.Get(key, value); }
        Status Put(const Slice &key, const Slice &value) noexcept override
        { return impl.Put(key, value); }
        Status Delete(const Slice &key) noexcept override
        { return impl.Delete(key); }

        struct IteratorType : Walker<Impl>
        {
            IteratorType(RefDB<Impl> &origin) :
                Walker<Impl>(origin.impl)
            {}
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return impl.NewIterator(); }

        Status Write(WriteBatch &updates)
        { return impl.Write(updates); }
    };

    // AnyDB have no specific iterator.
    // So we'll re-use leveldb original one
    template <>
    class RefDB<AnyDB>::IteratorType
    {
        std::unique_ptr<Iterator> impl;
    public:
        IteratorType(RefDB<AnyDB> &origin) :
            impl(origin.NewIterator())
        {}

        bool Valid() const { return impl->Valid(); }
        Slice key() const { return impl->key(); }
        Slice value() const { return impl->value(); }
        Status status() const { return impl->status(); }

        void SeekToFirst() { return impl->SeekToFirst(); }
        void SeekToLast() { return impl->SeekToLast(); }
        void Seek(const Slice &target) { return impl->Seek(target); }

        void Next() { return impl->Next(); }
        void Prev() { return impl->Prev(); }
    };

}
