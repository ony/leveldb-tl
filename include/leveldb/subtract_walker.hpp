#pragma once

#include <leveldb/walker.hpp>
#include <leveldb/whiteout_db.hpp>

namespace leveldb
{
    enum class Order : int { LT = -1, EQ = 0, GT = 1 };

    constexpr Order compare(int x) // compare with zero
    { return (x < 0) ? Order::LT : (x > 0) ? Order::GT : Order::EQ; }

    inline Order compare(const Slice &a, const Slice &b)
    { return compare(a.compare(b)); }

    /// Traits of source for walking
    template <typename T>
    struct WalkSource
    { typedef T &Embed; };

    /// Source for walking over data with whiteouts
    template <typename Base>
    struct Subtract
    {
        typename WalkSource<Base>::Embed base;
        typename WalkSource<WhiteoutDB>::Embed whiteout; // subset of base

        class Walker;
    };

    template <typename Base>
    class Subtract<Base>::Walker
    {
        typename Base::Walker w_base;
        typename WhiteoutDB::Walker w_whiteout;

        void SkipNext()
        {
            if (!w_whiteout.Valid())
            {
                w_whiteout.Seek(key());
                if (!w_whiteout.Valid())
                { return; }
            }

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
            {
                w_whiteout.Seek(key());
                if (!w_whiteout.Valid())
                { return; }
            }

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
        Status status() const { return w_base.status(); }

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

        void Seek(const Slice &target)
        {
            w_base.Seek(target);
            if (!Valid()) return;

            w_whiteout.Seek(target);
            SkipNext();
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

    template <typename T>
    struct WalkSource<Subtract<T>>
    { typedef Subtract<T> Embed; };

    template <typename Base>
    constexpr Subtract<Base> subtract(Base &base, WhiteoutDB &whiteout)
    { return {base, whiteout}; }
}
