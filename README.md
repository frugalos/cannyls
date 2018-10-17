cannyls
=======

[![Crates.io: cannyls](https://img.shields.io/crates/v/cannyls.svg)](https://crates.io/crates/cannyls)
[![Documentation](https://docs.rs/cannyls/badge.svg)](https://docs.rs/cannyls)
[![Build Status](https://travis-ci.org/frugalos/cannyls.svg?branch=master)](https://travis-ci.org/frugalos/cannyls)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

CannyLS is an embedded and persistent key-value storage optimized for random-access workload and huge-capacity HDD.

CannyLS mainly has following features:
- A local storage for storing objects that called as ["lump"][lump]:
  - Basically, a lump is a simple key-value entry
  - The distinctive properties are that the key is **fixed length (128 bits)** and suited for storing a relatively large size value (e.g., several MB)
- Provides simple functionalities:
  - Basically, the only operations you need to know are `PUT`, `GET` and `DELETE`
  - But it supports [deadline based I/O scheduling] as an advanced feature
- Optimized for random-access workload on huge-capacity HDD (up to 512 TB):
  - See [Benchmark Results] for more details about performance
- Aiming to provide predictable and stable read/write latency:
  - There are (nearly) strict upper bounds about the number of disk accesses issued when executing operations
    - One disk access when `PUT` and `DELETE`, and two when `PUT`
  - There are no background processings like compaction and stop-the-world GC which may block normal operations for a long time
  - For eliminating overhead and uncertainty, CannyLS has no caching layer:
    - It uses [Direct I/O] for bypassing OS layer caching (e.g., page cache)
    - If you need any caching layer, it is your responsibility to implement it
- Detailed metrics are exposed using [Prometheus]

See [Wiki] for more details about CannyLS.

[lump]: https://github.com/frugalos/cannyls/wiki/Terminology#lump
[Benchmark Results]: https://github.com/frugalos/cannyls/wiki/Benchmark
[Prometheus]: https://prometheus.io/
[deadline based I/O scheduling]: https://github.com/frugalos/cannyls/wiki/I-O-Scheduling-based-on-Request-Deadlines
[Direct I/O]: https://github.com/frugalos/cannyls/wiki/Terminology#ダイレクトio
[Wiki]: https://github.com/frugalos/cannyls/wiki


Documentation
-------------

- [Rustdoc](https://docs.rs/cannyls)
- [Wiki (Japanese only)][Wiki]
