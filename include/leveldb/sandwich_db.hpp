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
}
