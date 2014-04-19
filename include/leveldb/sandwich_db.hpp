#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/ref_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    template <typename T>
    class host_order
    {
        union {
            T value;
            char octets[sizeof(T)];
        };

    public:
        host_order() = default;
        host_order(T x) : value(x) {}
        host_order(const Slice &x) { *this = x; }

        host_order &operator=(T x)
        { value = x; return *this; }
        host_order &operator=(const Slice &s)
        {
            assert( !corrupted(s) );
            memcpy(octets, s.data(), size());
            return *this;
        }

        operator T() const { return value; }
        operator Slice() const { return Slice(data(), size()); }

        host_order<T> &operator++()
        { ++value; return *this; }
        host_order<T> &operator--()
        { --value; return *this; }

        // arithmetic in network order
        host_order<T> &next_net()
        {
            for (char *p = octets + sizeof(octets)-1; p >= octets; --p)
            {
                if (++(*p) != 0) break;
            }
            return *this;
        }

        static constexpr size_t size()
        { return sizeof(T); }
        constexpr const char *data() const
        { return octets; }
        static bool corrupted(const Slice &s)
        { return s.size() != size(); }
    };

    template <typename T, bool sync = true>
    class Sequence final
    {
        static constexpr T invalid = (T)-1;

        AnyDB &base;
        Slice key;
        host_order<T> next { invalid };

    public:
        // name and origin should outlive this object
        Sequence(AnyDB &base, const Slice &key) :
            base(base), key(key)
        {}

        ~Sequence()
        { Save(); }

        Status Next(host_order<T> &value)
        {
            Status s;
            if (next == invalid)
            {
                s = Load();
                if (!s.ok()) return s;
            }
            value = next;
            return Next();
        }
        Status Next(T &value)
        {
            Status s;
            if (next == invalid)
            {
                s = Load();
                if (!s.ok()) return s;
            }
            value = next;
            return Next();
        }
    private:
        Status Load()
        {
            std::string v;
            Status s = base.Get(key, v);
            if (s.IsNotFound())
            {
                next = 0;
                if (sync) s = Save();
            }
            else if (s.ok())
            {
                if (next.corrupted(v))
                { return Status::Corruption("Invalid sequence entry (value size mismatch)"); }

                next = v;
            }
            return s;
        }

        Status Next() // just skip to next
        {
            ++next;
            if (sync)
            {
                Status s = Save();
                if (!s.ok()) // rollback
                {
                    --next;
                    return s;
                }
            }
            return Status::OK();
        }
    public:
        Status Save()
        { return base.Put(key, next); }
    };

    template <typename Base, typename Prefix = unsigned short>
    class SandwichDB final
    {
    public:
        class Part;
        using IteratorType = typename Part::IteratorType;

    private:
        Base base;
        Part meta { *this };
        Sequence<Prefix> seq { meta, Slice() };

    public:
        SandwichDB(SandwichDB<Base,Prefix> &&) = default;

        template <typename... Args>
        SandwichDB(Args &&... args) : base(std::forward<Args>(args)...)
        {}

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
            char buf[prefix.size() + key.size()];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + sizeof(prefix), key.data(), key.size());
            return sandwich->base.Get(Slice(buf, sizeof(buf)), value);
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            assert( Valid() );
            char buf[prefix.size() + key.size()];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + prefix.size(), key.data(), key.size());
            return sandwich->base.Put(Slice(buf, sizeof(buf)), value);
        }
        Status Delete(const Slice &key) noexcept override
        {
            assert( Valid() );
            char buf[prefix.size() + key.size()];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + prefix.size(), key.data(), key.size());
            return sandwich->base.Delete(Slice(buf, sizeof(buf)));
        }

        class IteratorType;

        std::unique_ptr<Iterator> NewIterator() noexcept override
        {
            assert( Valid() );
            return asIterator(IteratorType(*this));
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB<Base, Prefix>::Part::IteratorType
    {
        host_order<Prefix> prefix;
        Walker<Base> impl;

    public:
        IteratorType(SandwichDB<Base, Prefix>::Part &origin) :
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
            p.next_net();;
            impl.Seek(p);
            if (impl.Valid())
            { impl.Prev(); }
            else
            { impl.SeekToLast(); }
        }
        void Next() { impl.Next(); }
        void Prev() { impl.Prev(); }

        void Seek(const Slice &target)
        {
            char buf[prefix.size() + target.size()];
            (void) memcpy(buf, prefix.data(), prefix.size());
            (void) memcpy(buf + sizeof(prefix), target.data(), target.size());
            impl.Seek(Slice(buf, sizeof(buf)));
        }
    };
}
