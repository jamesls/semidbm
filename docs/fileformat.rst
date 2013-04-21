======================
File Format of DB file
======================

:author: James Saryerwinnie
:status: Draft
:target-version: 0.4.0
:date: April 15, 2013

Abstract
========

This document proposes a new file format for semidbm.  This is a backwards
incompatible change.

Motivation
==========

When python3 support was added, ``semidbm`` received a significant performance
degredation.  This was mainly due to the str vs. bytes differentiation, and
the fact that semidbm was a text based format.  All of the integer sizes and
checksum information was written as ASCII strings, and as a result, encoding
the string to a byte sequence added additional overhead.

In order to improve performance, ``semidbm`` should adopt a binary format,
specifically the sizes of the keys and values as well as the checksums should
be written as binary values.  This will avoid the need to use string formatting
when writing values.  It will also improve the load time of a db file.


Specification
=============

A semidbm file will consist of a header and a sequence of entries.
All multibyte sequences are writteni network byte order.


Header
======

The semidbm header format consists of:

* 4 byte magic number (``53 45 4d 49``)
* 4 byte version number consisting of 2 byte major version and 2 byte
  minor version (currently (1, 1)).


Entries
=======

After the header, the file contains a sequence of
entries.  Each entry has this format:

* 4 byte key size
* 4 byte value size
* Key contents
* Value content
* 4 byte CRC32 checksum of Key + Value

If a key is deleted it will have a value size of -1 and no value content.
