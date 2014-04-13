#pragma once

#include <leveldb/any_db.hpp>

namespace leveldb
{
    template <typename T>
    class network_order
    {
        unsigned char octets[sizeof(T)];
    public:
        network_order() = default;

        network_order(const Slice &x)
        {
            assert( x.size() == sizeof(octets) );
            memcpy(octets, x.data(), sizeof(octets));
        }

        network_order(T x)
        {
            for (unsigned char *p = octets + sizeof(octets)-1; p >= octets; --p)
            {
                *p = x;
                x >>= 8;
            }
        }

        operator T() const
        {
            T x = 0;
            for (unsigned char c : octets)
            { x = (x << 8) | c; }
            return x;
        }

        operator Slice() const
        { return Slice(data(), size()); }

        // arithmetic in network order
        network_order<T> &operator++()
        {
            for (unsigned char *p = octets + sizeof(octets)-1; p >= octets; --p)
            {
                if (++(*p) != 0) break;
            }
            return *this;
        }

        constexpr size_t size() const
        { return sizeof(octets); }
        constexpr const char *data() const
        { return reinterpret_cast<const char *>(octets); }
    };

    template <typename T, bool sync = true>
    class Sequence final
    {
        AnyDB &base;
        Slice key;
        T next = (T)-1;

    public:
        // name and origin should outlive this object
        Sequence(AnyDB &base, const Slice &key) :
            base(base), key(key)
        {}

        ~Sequence()
        { Save(); }

        Status Next(T &value)
        {
            Status s;
            if (next == (T)-1)
            {
                s = Load();
                if (!s.ok()) return s;
            }
            value = next++;
            if (sync)
            {
                s = Save();
                if (!s.ok()) // rollback
                {
                    --next;
                    return s;
                }
            }
            return Status::OK();
        }
    private:
        // we use network order
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
                if (v.size() != sizeof(T))
                { return Status::Corruption("Invalid sequence entry (value size mismatch)"); }

                next = network_order<T> { v };
            }
            return s;
        }
    public:
        Status Save()
        { return base.Put(key, network_order<T> { next }); }
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
        template <typename... Args>
        SandwichDB(Args &&... args) : base(std::forward<Args>(args)...)
        {}

        Base *operator->() { return &base; }

        // Note: this should be a pretty rare call
        Part use(const Slice &name)
        {
            Status s;
            assert( !name.empty() );

            Prefix p;

            std::string v;
            s = meta.Get(name, v);
            if (s.ok())
            {
                if (v.size() != sizeof(Prefix)) // corrupted?
                { return Part(); }
                p = network_order<Prefix>{ v };
            }
            else if (s.IsNotFound())
            {
                s = seq.Next(p);
                if (s.ok() && p == 0) s = seq.Next(p); // skip meta part
                if (s.ok())
                { s = meta.Put(name, network_order<Prefix>{ p }); }
            }
            if (s.ok()) return Part(*this, p);
            else return Part();
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB<Base, Prefix>::Part final : public AnyDB
    {
        SandwichDB *sandwich;
        network_order<Prefix> prefix;

        friend class SandwichDB<Base, Prefix>;

        Part(SandwichDB &origin, Prefix prefix = 0) :
            sandwich(&origin), prefix(prefix)
        {}
    public:
        Part() : sandwich(nullptr) {}

        bool Valid() const { return sandwich; }

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
        network_order<Prefix> prefix;
        typename Base::IteratorType impl;

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
            ++p;
            impl.Seek(p);
            impl.Prev();
        }
        void Next() { impl.Next(); }
        void Prev() { impl.Prev(); }

        void Seek(const Slice &target)
        {
            char buf[prefix.size() + target.size()];
            (void) memcpy(buf, prefix.data(), sizeof(prefix));
            (void) memcpy(buf + sizeof(prefix), target.data(), target.size());
            impl.Seek(Slice(buf, sizeof(buf)));
        }
    };
}
