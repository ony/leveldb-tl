#pragma once

#include <leveldb/walker.hpp>

namespace leveldb
{
    enum class Order : int { LT = -1, EQ = 0, GT = 1 };

    constexpr Order compare(int x) // compare with zero
    { return (x < 0) ? Order::LT : (x > 0) ? Order::GT : Order::EQ; }

    Order compare(const Slice &a, const Slice &b)
    { return compare(a.compare(b)); }

    template <typename Base>
    struct Subtract
    {
        Base &base;
        Whiteout &whiteout; // subset of base
    };

    template <typename Base>
    class Walker<Subtract<Base>>
    {
        Walker<Base> w_base;
        Walker<Whiteout> w_whiteout;

        void SkipNext()
        {
            if (!w_whiteout.Valid())
            { w_whiteout.Seek(key()); }
            if (!w_whiteout.Valid())
            { return; }

            for (;;)
            {
                switch (compare(key(), w_whiteout.key()))
                {
                case Order::LT:
                    return;
                case Order::GT:
                    w_whiteout.Next();
                    if (!w_whiteout.Valid()) return;
                    break;
                case Order::EQ:
                    w_base.Next();
                    w_whiteout.Next();
                    if (!w_whiteout.Valid() || !Valid()) return;
                    break;
                }
            }
        }

        void SkipPrev()
        {
            if (!w_whiteout.Valid())
            { w_whiteout.Seek(key()); }
            if (!w_whiteout.Valid())
            { return; }

            for (;;)
            {
                switch (compare(key(), w_whiteout.key()))
                {
                case Order::GT:
                    return;
                case Order::LT:
                    w_whiteout.Prev();
                    if (!w_whiteout.Valid()) return;
                    break;
                case Order::EQ:
                    w_base.Prev();
                    w_whiteout.Prev();
                    if (!w_whiteout.Valid() || !Valid()) return;
                    break;
                }
            }
        }

    public:
        Walker(Subtract<Base> op) :
            w_base(op.base),
            w_whiteout(op.whiteout)
        {}

        bool Valid() const { return w_base.Valid(); }
        Slice key() const { return w_base.key(); }
        Slice value() const { return w_base.value(); }

        void SeekToFirst()
        {
            w_base.SeekToFirst();
            if (!Valid()) return;

            w_whiteout.SeekToFirst();
            SkipNext();
        }

        void SeekToLast()
        {
            w_base.SeekToLast();
            if (!Valid()) return;

            w_whiteout.SeekToLast();
            SkipPrev();
        }

        void Next()
        {
            w_base.Next();
            if (Valid()) SkipNext();
        }

        void Prev()
        {
            w_base.Prev();
            if (Valid()) SkipPrev();
        }
    };

    Walker<Subtract<AnyDB>> subtract(AnyDB &base, Whiteout &whiteout)
    { return Walker<Subtract<AnyDB>>({base, whiteout}); }

    template <typename Base>
    Walker<Subtract<Base>> subtract(Base &base, Whiteout &whiteout)
    { return Walker<Subtract<Base>>({base, whiteout}); }
}
