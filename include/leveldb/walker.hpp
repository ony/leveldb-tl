#pragma once

#include <map>
#include <memory>

#include <leveldb/iterator.h>

#include <leveldb/any_db.hpp>

namespace leveldb
{
    template <typename T>
    constexpr typename T::Walker walker(T &collection)
    { return { collection }; }

    template <typename T>
    constexpr typename T::Walker walker(const T &collection)
    { return { collection }; }
}
