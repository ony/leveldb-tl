#pragma once

#include <limits>

#include <leveldb/any_db.hpp>

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

        // lets make value variant as a primary handler for comparison
        template <typename T1>
        bool operator==(T1 &&other) const
        { return value == std::forward<T1>(other); }

        template <typename T1>
        bool operator!=(T1 &&other) const
        { return value != std::forward<T1>(other); }

        host_order<T> &operator++()
        { ++value; return *this; }
        host_order<T> &operator--()
        { --value; return *this; }

        static constexpr host_order<T> max()
        { return std::numeric_limits<T>::max(); }

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

    template <typename T, T PAGE_SIZE = 10>
    class Sequence
    {
        AnyDB &base;
        Slice key;
        T next = 0;
        host_order<T> allocated = next;

    public:
        /// Construct from specific entry of AnyDB.
        /// \param base refers to underlying database
        /// \param key identifies entry for storing sequence state
        /// \note name and origin should outlive this object and any recipient
        ///       of move
        Sequence(AnyDB &base, const Slice &key) :
            base(base), key(key)
        {}

        /// dtor just to ensure that Sync() was called.
        /// \note object shouldn't be destroyed earlier than underlying
        ///       database
        /// \note that it is strongly recommended to call Sync() directly
        ///       before destroying object to control result of operation
        ~Sequence()
        {
            // assert(next == allocated);
            (void) Sync();
        }

        Sequence(const Sequence<T> &) = delete;

        Sequence(Sequence<T> &&sequence) :
            base(sequence.base),
            key(sequence.key),
            next(sequence.next),
            allocated(sequence.allocated)
        {
            // we'll take care about calling Sync()
            sequence.allocated = sequence.next;
        }

        /// Request next value in sequence.
        /// \typeparam T1 specifies output type compatible with T
        template <typename T1>
        Status Next(T1 &value)
        {
            assert( allocated == 0 || next <= allocated );

            if (allocated == 0) // noting allocated yet
            {
                Status s = AllocPage();
                if (!s.ok()) return s;
            }

            if (next == allocated)
            {
                value = next++;
                (void) AllocPage(); // pre-allocate next page for next request
            }
            else
            {
                value = next++;
            }
            return Status::OK();
        }

    private:
        Status AllocPage()
        {
            std::string v;
            Status s = base.Get(key, v);
            if (s.ok())
            {
                if (allocated.corrupted(v))
                { return Status::Corruption("Invalid sequence entry (value size mismatch)"); }

                if (allocated == 0) // initial (need to load prev value)
                {
                    allocated = v;
                    if (allocated == 0) return Status::NotFound("sequence overflow");
                    next = allocated;
                }
                else if (allocated != host_order<T>{v})
                { return Status::Corruption("Concurrent sequence entry change (value mismatch)"); }
            }
            else if (s.IsNotFound())
            {
                if (allocated != 0) // not an initial state?
                { return Status::Corruption("Concurrent sequence entry change (missing value)"); }
            }
            else // other errors
            {
                return s;
            }

            // to avoid overflow we'll use:
            // min(max, x + p) ~ min(max - p, x) + p
            const host_order<T> nextAllocated = T(std::min(T(allocated.max() - PAGE_SIZE), T(allocated)) + PAGE_SIZE);
            if (nextAllocated == allocated) // overflow
            {
                (void) base.Put(key, host_order<T>{0}); // mark as overflow
                allocated = 0;
                return Status::NotFound("meet sequence overflow");
            }
            s = base.Put(key, nextAllocated);
            if (s.ok()) allocated = nextAllocated;
            return s;
        }

    public:
        /// Sync current sequence back to underlying storage.
        /// This operation should be done before releasing object.
        Status Sync()
        {
            assert(next <= allocated);

            if (next < allocated)
            {
                std::string v;
                Status s = base.Get(key, v);

                if (s.IsNotFound())
                { return Status::Corruption("Concurrent sequence entry change (missing value)"); }

                if (allocated.corrupted(v))
                { return Status::Corruption("Invalid sequence entry (value size mismatch)"); }

                if (allocated != host_order<T>{v})
                { return Status::Corruption("Concurrent sequence entry change (value mismatch)"); }

                const host_order<T> nextAllocated = next;

                s = base.Put(key, nextAllocated);
                if (!s.ok()) return s;

                allocated = nextAllocated;
            }

            return Status::OK();
        }
    };
}
