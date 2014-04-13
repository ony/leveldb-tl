#pragma once

#include <leveldb/subtract_walker.hpp>

namespace leveldb
{
    template <typename Base, typename Overlay>
    struct Cover
    {
        Base &base;
        Overlay &overlay;
    };

    template <typename Base, typename Overlay>
    struct Cover<Subtract<Base>, Overlay>
    {
        Subtract<Base> base;
        Overlay &overlay;
    };

    template <typename Base, typename Overlay>
    class Walker<Cover<Base, Overlay>>
    {
        Walker<Base> i;
        Walker<Overlay> j;

        enum { Both, FwdLeft, FwdRight, RevLeft, RevRight } state;

        bool useOverlay() const
        {
            switch (state)
            {
            case FwdRight:
            case RevRight:
            case Both:
                return true;
            case FwdLeft:
            case RevLeft:
                return false;
            default:
                abort(); // unreachable
            }
        }

        void Activate(bool fwd = true)
        {
            if (!j.Valid())
            {
                state = fwd ? FwdLeft : RevLeft;
                return;
            }
            if (!i.Valid())
            {
                state = fwd ? FwdRight : RevRight;
                return;
            }
            switch (compare(i.key(), j.key()))
            {
            case Order::EQ:
                state = Both;
                break;
            case Order::LT:
                state = fwd ? FwdLeft : RevRight;
                break;
            case Order::GT:
                state = fwd ? FwdRight : RevLeft;
                break;
            }
        }

    public:
        Walker(Cover<Base, Overlay> op) :
            i(op.base),
            j(op.overlay)
        {}

        bool Valid() const { return useOverlay() ? j.Valid() : i.Valid(); }
        Slice key() const { return useOverlay() ? j.key() : i.key(); }
        Slice value() const { return useOverlay() ? j.value() : i.value(); }
        Status status() const { return useOverlay() ? j.status() : i.status(); }

        void Seek(const Slice &target)
        {
            i.Seek(target);
            j.Seek(target);
            Activate();
        }

        void SeekToFirst()
        {
            i.SeekToFirst();
            j.SeekToFirst();
            Activate();
        }

        void SeekToLast()
        {
            i.SeekToLast();
            j.SeekToLast();
            Activate(false);
        }

        void Next()
        {
            switch (state)
            {
            case FwdLeft: i.Next(); break;
            case FwdRight: j.Next(); break;
            case Both: i.Next(); j.Next(); break;

            // special cases (reversing)
            case RevLeft:
                if (!j.Valid())
                {
                    j.Seek(i.key());
                    if (!j.Valid())
                    { i.Next(); state = FwdLeft; return; }
                }
                if (i.key().compare(j.key()) >= 0) j.Next();
                i.Next();
                break;

            case RevRight:
                if (!i.Valid())
                {
                    i.Seek(j.key());
                    if (!i.Valid())
                    { j.Next(); state = FwdRight; return; }
                }
                if (i.key().compare(j.key()) <= 0) i.Next();
                j.Next();
                break;
            }
            Activate();
        }

        void Prev()
        {
            switch (state)
            {
            case RevLeft: i.Prev(); break;
            case RevRight: j.Prev(); break;
            case Both: i.Prev(); j.Prev(); break;

            // special cases (reversing)
            case FwdLeft:
                if (!j.Valid())
                {
                    j.Seek(i.key());
                    if (!j.Valid())
                    { i.Prev(); j.SeekToLast(); break; }
                }
                i.Prev();
                j.Prev();
                break;

            case FwdRight:
                if (!i.Valid())
                {
                    i.Seek(j.key());
                    if (!i.Valid())
                    { j.Prev(); i.SeekToLast(); break; }
                }
                i.Prev();
                j.Prev();
                break;
            }
            Activate(false);
        }
    };

    template <typename Base, typename Overlay>
    constexpr Cover<Base, Overlay> cover(Base &base, Overlay &overlay)
    { return Cover<Base, Overlay>({base, overlay}); }
}
