# RoaringBitmap [![github-actions-badge][]][github-actions] [![release-badge][]][cargo] [![docs-badge][]][docs] [![rust-version-badge][]][rust-version]

This is a [Rust][] port of the [Roaring bitmap][] data structure, initially
defined as a [Java library][roaring-java] and described in [_Better bitmap
performance with Roaring bitmaps_][roaring-paper].

## Rust version policy

This crate only supports the current stable version of Rust, patch releases may
use new features at any time.

## Developing

This project uses [Clippy][], [rustfmt][], and denies warnings in CI builds. Available via
`rustup component add clippy rustfmt`.

To ensure your changes will be accepted please check them with:
```
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

In addition, ensure all tests are passing with `cargo test`

### Benchmarking

It is recommended to run the `cargo bench` command.
The [benchmarks directory](./benchmarks) contains a library that is dedicated to benchmarking the 
Roaring library by using a set of [real-world datasets][]. It is also advised to run the benchmarks
on a bare-metal machine, running them on the base branch and then on the contribution PR
branch to better see the changes.

Those benchmarks are designed on top of the Criterion library,
you can read more about it [on the user guide][].

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.

[github-actions-badge]:
https://github.com/RoaringBitmap/roaring-rs/actions/workflows/test.yml/badge.svg
[github-actions]: https://github.com/RoaringBitmap/roaring-rs/actions
[release-badge]: https://img.shields.io/github/release/RoaringBitmap/roaring-rs.svg?style=flat-square
[cargo]: https://crates.io/crates/roaring
[docs-badge]: https://img.shields.io/badge/API-docs-blue.svg?style=flat-square
[docs]: https://docs.rs/roaring
[rust-version-badge]: https://img.shields.io/badge/rust-latest%20stable-blue.svg?style=flat-square
[rust-version]: https://github.com/RoaringBitmap/roaring-rs#rust-version-policy

[Rust]: https://www.rust-lang.org/
[Roaring bitmap]: https://roaringbitmap.org/
[roaring-java]: https://github.com/lemire/RoaringBitmap
[roaring-paper]: https://arxiv.org/pdf/1402.6407v4
[Clippy]: https://github.com/rust-lang/rust-clippy
[rustfmt]: https://github.com/rust-lang/rustfmt

[real-world datasets]: https://github.com/RoaringBitmap/real-roaring-datasets
[on the user guide]: https://bheisler.github.io/criterion.rs/book/user_guide/user_guide.html

## Experimental features

The `simd` feature is in active development. It has not been tested. If you would like to build with `simd` note that
`std::simd` is only available in Rust nightly.
