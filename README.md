LevelDB Template Library
========================

This project is intended for using in programs like browsers, small dbms like
http://github.com/openwebos/db8 .

Features
--------
- abstract AnyDB
- in-memory AnyDB
- transactions layer
- sandwich layer (multiple AnyDB in one)
- reference layer to embed ref. to existing AnyDB

See tests/simple.cpp for samples of usage

Goals to stick with
-------------------
- extreme fusion between layers to give a chance for compiler optimizer
- minimize using of ref/ptr that leads outside of this library

Notes for development
---------------------
- tests should be run under valgrind also unless built with libasan (sanitizer
  from gcc 4.8)

Credits
-------
Inspired by https://github.com/feniksa/ldblayer

# Copyright and License Information

Copyright (c) 2014 Nikolay Orliuk <virkony@gmail.com>

Copyright (c) 2014 LG Electronics, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use content of this library including all source code files and
documentation files except in compliance with the License. You may obtain
a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
