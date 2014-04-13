#pragma once

#include <leveldb/any_db.hpp>

namespace leveldb
{
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

                next = 0;
                for (auto c : v)
                {
                    next = (next << 8) + c;
                }
            }
            return s;
        }
    public:
        Status Save()
        {
            char v[sizeof(T)];
            T x = next;

            for (char *p = v + sizeof(v)-1; p >= v; --p)
            {
                *p = x;
                x >>= 8;
            }
            return base.Put(key, Slice(v, sizeof(v)));
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB final
    {
    public:
        class Part;

    private:
        Base base;
        Part meta { *this };
        Sequence<Prefix> seq { meta, Slice() };

    public:
        template <typename... Args>
        SandwichDB(Args... args) : base(args...)
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
                memcpy(&p, v.data(), sizeof(Prefix));
            }
            else if (s.IsNotFound())
            {
                s = seq.Next(p);
                if (s.ok() && p == 0) s = seq.Next(p); // skip meta part
                if (s.ok())
                { s = meta.Put(name, Slice(reinterpret_cast<const char*>(&p), sizeof(p))); }
            }
            if (s.ok()) return Part(*this, p);
            else return Part();
        }
    };

    template <typename Base, typename Prefix = short>
    class SandwichDB<Base, Prefix>::Part final : public AnyDB
    {

        SandwichDB *sandwich;
        Prefix prefix;

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
            char buf[sizeof(prefix) + key.size()];
            (void) memcpy(buf, &prefix, sizeof(prefix));
            (void) memcpy(buf + sizeof(prefix), key.data(), key.size());
            return sandwich->base.Get(Slice(buf, sizeof(buf)), value);
        }

        Status Put(const Slice &key, const Slice &value) noexcept override
        {
            assert( Valid() );
            char buf[sizeof(prefix) + key.size()];
            (void) memcpy(buf, &prefix, sizeof(prefix));
            (void) memcpy(buf + sizeof(prefix), key.data(), key.size());
            return sandwich->base.Put(Slice(buf, sizeof(buf)), value);
        }
        Status Delete(const Slice &key) noexcept override
        {
            assert( Valid() );
            char buf[sizeof(prefix) + key.size()];
            (void) memcpy(buf, &prefix, sizeof(prefix));
            (void) memcpy(buf + sizeof(prefix), key.data(), key.size());
            return sandwich->base.Delete(Slice(buf, sizeof(buf)));
        }
        std::unique_ptr<Iterator> NewIterator() noexcept override
        {
            assert( Valid() );
            return nullptr;
        }
        Status Write(WriteBatch &updates)
        {
            assert( Valid() );
            return Status::NotSupported("TODO: WriteBatch");
        }
    };
}
