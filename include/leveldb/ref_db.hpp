// @@@LICENSE
//
//      Copyright (c) 2014 Nikolay Orliuk <virkony@gmail.com>
//      Copyright (c) 2014 LG Electronics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// LICENSE@@@

#pragma once

#include <leveldb/any_db.hpp>
#include <leveldb/walker.hpp>

namespace leveldb
{
    // Reference to some existing database under terms that Impl object
    // outlives this reference.
    // Useful for putting multiple different stuff on top of some existing
    // i.e. to have both transactional sandwich and non-transactional over same
    // bottom
    template <typename Impl = AnyDB>
    class RefDB final : public AnyDB
    {
        Impl &impl;
    public:
        RefDB(Impl &origin) : impl(origin) {}

        Status Get(const Slice &key, std::string &value) noexcept override
        { return impl.Get(key, value); }
        Status Put(const Slice &key, const Slice &value) noexcept override
        { return impl.Put(key, value); }
        Status Delete(const Slice &key) noexcept override
        { return impl.Delete(key); }

        struct Walker : Impl::Walker
        {
            Walker(RefDB<Impl> &origin) :
                Impl::Walker(origin.impl)
            {}
        };

        std::unique_ptr<Iterator> NewIterator() noexcept override
        { return impl.NewIterator(); }

        Status Write(WriteBatch &updates)
        { return impl.Write(updates); }
    };
}
