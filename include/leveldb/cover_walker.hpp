#pragma once

#include <leveldb/subtract_walker.hpp>

namespace leveldb
{
    template <typename Base, typename Overlay>
    struct Cover
    {
        typename WalkSource<Base>::Embed base;
        typename WalkSource<Overlay>::Embed overlay;

        class Walker;
    };

    template <typename Base, typename Overlay>
    class Cover<Base, Overlay>::Walker
    {
        typename Base::Walker i;
        typename Overlay::Walker j;

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
        {
            // ensure that both iterators are initialized so notification won't
            // hurt us
            SeekToFirst();
        }

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

    protected:
        void overlayPut(const Slice &key)
        {
            if (!Valid()) return; // do not bother
            switch (state)
            {
            case FwdLeft:
                // check range
                switch (compare(i.key(), key))
                {
                case Order::EQ:
                    j.Seek(key);
                    state = Both;
                    return;
                case Order::GT: return;
                case Order::LT: break;

                }
                if (j.Valid() && j.key().compare(key) < 0) return;
                // ok this is scase when key inserted between current base and
                // next cover
                j.Seek(key);
                break;
            case RevLeft:
                switch (compare(i.key(), key))
                {
                case Order::EQ:
                    j.Seek(key);
                    state = Both;
                    return;
                case Order::LT: return;
                case Order::GT: break;
                }
                if (j.Valid() && j.key().compare(key) > 0) return;
                j.Seek(key); // exact match so no need to do Prev()
                break;
            case Both:
            case RevRight:
            case FwdRight:
                // our current value is from overaly
                // no need to sync hidden
                break;
            }
        }

        void overlayDelete(const Slice &key)
        {
            if (!Valid()) return;
            switch (state)
            {
            case FwdLeft:
                // check if we already pointing to this record
                if (!j.Valid() || j.key().compare(key) != 0) return;
                j.Seek(key); // we know that this record exists
                j.Next();
                break;
            case RevLeft:
                if (!j.Valid() || j.key().compare(key) != 0) return;
                j.Seek(key);
                j.Prev();
                break;
            case Both:
            case RevRight:
            case FwdRight:
                // even if we point to record that is about to be deleted we
                // shouldn't care since client is responsible for moving away
                // from ghost records
                break;
            }
        }
    };

    template <typename Base, typename Overlay>
    constexpr Cover<Base, Overlay> cover(Base &base, Overlay &overlay)
    { return Cover<Base, Overlay>({base, overlay}); }
}
