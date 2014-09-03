#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/ref_db.hpp>
#include <leveldb/walker.hpp>
#include <leveldb/sequence.hpp>

namespace leveldb
{
    template <typename Base, typename Prefix = unsigned short>
    class SandwichDB final
    {
    public:
        class Part;

    private:
        Base base;
        Part meta { *this };
        Sequence<Prefix> seq { meta, Slice() };

    public:
        SandwichDB(SandwichDB<Base,Prefix> &&) = default;

        template <typename... Args>
        SandwichDB(Args &&... args) : base(std::forward<Args>(args)...)
        {}

        Base &operator*() { return base; }
        Base *operator->() { return &base; }

        /// Create a same sandwich but from ref to this database.
        //
        /// \typeparam T refers to embeded database type that can be
        /// constructed out of reference to current one.
        ///
        /// Usually used to create transaction/refs
        template <typename T = RefDB<Base>>
        SandwichDB<T, Prefix> ref()
        { return base; }
        template <template<typename> class T>
        SandwichDB<T<Base>, Prefix> ref()
        { return base; }

        using Cookie = host_order<Prefix>;

        /// Obtain part of sandwich
        Part use(Cookie cookie)
        { return {*this, cookie}; }

        /// Cook a cookie for use
        ///
        /// \param result  ref to memory cell that receives cookie if status is
        ///                success
        ///
        /// \note this call should be a pretty rare because it actually does
        ///       lookup for an entry in database
        Status cook(const Slice &name, Cookie &cookie)
        {
            Status s;
            assert( !name.empty() );

            std::string v;
            s = meta.Get(name, v);
            if (s.ok())
            {
                if (Cookie::corrupted(v))
                { return Status::Corruption("Invalid sandwich mapping entry"); }
                cookie = v;
                return s;
            }
            else if (s.IsNotFound())
            {
                Cookie nextCookie;
                s = seq.Next(nextCookie);
                if (s.ok() && nextCookie == 0) s = seq.Next(nextCookie); // skip meta part
                if (s.ok()) s = meta.Put(name, nextCookie);
                if (s.ok()) cookie = nextCookie;
                return s;
            }
            else
            { return s; }
        }

        /// Synchronize meta-data back to underground layer.
        /// This should be done before destroying object
        Status Sync()
        { return seq.Sync(); }

        /// Just an easy interface around cookie()
        Part use(const Slice &name)
        {
            Cookie cookie {};
            if (cook(name, cookie).ok())
            { return use(cookie); }
            else
            { return {}; }
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB<Base, Prefix>::Part final : public AnyDB
    {
        SandwichDB *sandwich;
        host_order<Prefix> prefix;

        friend class SandwichDB<Base, Prefix>;

        Part(SandwichDB &origin, Prefix prefix = 0) :
            sandwich(&origin), prefix(prefix)
        {}
    public:
        Part() : sandwich(nullptr) {}

        /// Create an associated part in other sandwich stack with same parts
        /// structure.
        ///
        /// Usually used to ref part for transaction/refs backed sandwich
        template <template <typename> class T>
        typename SandwichDB<T<Base>, Prefix>::Part ref(SandwichDB<T<Base>, Prefix> &origin)
        { return origin.use(prefix); }

        bool Valid() const { return sandwich; }
        SandwichDB::Cookie Cookie() const { return prefix; }

        Status Get(const Slice &key, std::string &value) noexcept override
        {
            assert( Valid() );
            const size_t buf_size = prefix.size() + key.size();
            char buf[buf_size];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + sizeof(prefix), key.data(), key.size());
            return sandwich->base.Get(Slice(buf, buf_size), value);
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            assert( Valid() );
            const size_t buf_size = prefix.size() + key.size();
            char buf[buf_size];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + prefix.size(), key.data(), key.size());
            return sandwich->base.Put(Slice(buf, buf_size), value);
        }
        Status Delete(const Slice &key) noexcept override
        {
            assert( Valid() );
            const size_t buf_size = prefix.size() + key.size();
            char buf[prefix.size() + key.size()];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + prefix.size(), key.data(), key.size());
            return sandwich->base.Delete(Slice(buf, buf_size));
        }

        class Walker;

        std::unique_ptr<Iterator> NewIterator() noexcept override
        {
            assert( Valid() );
            return asIterator(Walker(*this));
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB<Base, Prefix>::Part::Walker
    {
        host_order<Prefix> prefix;
        typename Base::Walker impl;

    public:
        Walker(SandwichDB<Base, Prefix>::Part &origin) :
            prefix{ origin.prefix }, impl{ origin.sandwich->base }
        {}

        bool Valid() const { return impl.Valid() && impl.key().starts_with(prefix); }
        Slice key() const
        {
            Slice k = impl.key();
            k.remove_prefix(prefix.size());
            return k;
        }
        Slice value() const { return impl.value(); }
        Status status() const
        {
            Status s = impl.status();
            if (s.ok() && !Valid()) s = Status::NotFound("Out of sandwich slice");
            return s;
        }

        void SeekToFirst() { impl.Seek(prefix); }
        void SeekToLast()
        {
            auto p = prefix;
            if (p == p.max())
            {
                impl.SeekToLast();
            }
            else
            {
                p.next_net();;
                impl.Seek(p);
                if (impl.Valid())
                { impl.Prev(); }
                else
                { impl.SeekToLast(); }
            }
        }
        void Next() { impl.Next(); }
        void Prev() { impl.Prev(); }

        void Seek(const Slice &target)
        {
            const size_t buf_size = prefix.size() + target.size();
            char buf[buf_size];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + sizeof(prefix), target.data(), target.size());
            impl.Seek(Slice(buf, buf_size));
        }
    };
}
