// @@@LICENSE
//
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

#include <iostream>
#include <deque>
#include <string>

#include "leveldb/sandwich_db.hpp"
#include "leveldb/bottom_db.hpp"

using namespace std;

void usage()
{
	cerr << "Usage: combine [options...] <DEST> <PART...>" << endl
	     << "Options:" << endl
	     << "    -s<SUFFIX>  Append <SUFFIX> to each part name during open" << endl;
}

int main(int argc, char *argv[])
{
	string suffix = "";
	deque<string> parts;
	string dest;

	for (int n = 1; n < argc; ++n)
	{
		if (argv[n][0] == '-')
		{
			switch (argv[n][1])
			{
			case 's':
				suffix = argv[n] + 2;
				break;

			default:
				cerr << "Wrong argument " << argv[n] << endl;
				usage();
				return 1;
			}

		}
		else if (dest.empty())
		{
			dest = argv[n];
		}
		else
		{
			parts.push_back(argv[n]);
		}
	}

	if (dest.empty())
	{
		cerr << "At least destination database should be provided" << endl;
		usage();
		return 1;
	}

	leveldb::SandwichDB<leveldb::BottomDB> cdb;
	cdb->options.create_if_missing = true;
	cdb->options.error_if_exists = true;
	auto s = cdb->Open(dest.c_str());

	if (!s.ok())
	{
		cerr << "Failed to open destination database " << dest  << ": " << s.ToString() << endl;
		return 2;
	}

	for (const auto &part : parts)
	{
		cerr << "Processing part " << part << endl;
		leveldb::BottomDB db;
		s = db.Open((part + suffix).c_str());

		if (!s.ok())
		{
			cerr << "Failed to open destination database " << dest  << ": " << s.ToString() << endl;
			return 2;
		}

		auto pdb = cdb.use(part);
		decltype(db)::Walker it { db };

		for (it.SeekToFirst(); it.Valid(); it.Next())
		{
			pdb.Put(it.key(), it.value());
		}
	}

	cerr << "Done" << endl;

	return 0;
}
