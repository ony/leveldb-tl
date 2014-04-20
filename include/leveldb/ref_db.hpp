#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    // Reference to some existing database under terms that Impl object
    // outlives this reference.
    // Useful for putting multiple different stuff on top of some existing
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

        struct Walker : Impl::Walker
        {
            Walker(RefDB<Impl> &origin) :
                Impl::Walker(origin.impl)
            {}
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return impl.NewIterator(); }

        Status Write(WriteBatch &updates)
        { return impl.Write(updates); }
    };
}
