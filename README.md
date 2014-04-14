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

Credits
-------
Inspired by https://github.com/feniksa/ldblayer

Copyright (C) 2014  Nikolay Orliuk <virkony@gmail.com>

All parts are subject for LGPL-2.1 license shipped with this sources.

Act of distributing any derived work from this project as a whole or as its
parts fall under terms of LGPL-2.1 and subject for spreading open source.
Software that uses this library is out of scope of this license.
