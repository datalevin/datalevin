# Roaring 0.11.5

`roaring/` contains the published crates.io `roaring` 0.11.5 source, with its
original Apache-2.0 and MIT licenses. The published crate archive has SHA-256
`18bd8a37d17a58532776dcdf6041ce64929adca78e8489d5cacbafe99229d3e1`. The unmodified registry crate is also a
dev dependency, used as an independent wire-format and validation oracle.

`roaring.patch` records the only source changes. To reproduce, extract the
published crate and apply `patch -p1 < ../roaring.patch` from its directory.

- Serialize array and bitmap containers with one checked `write_all` of the
  existing contiguous words on little-endian hosts, using Roaring's existing
  `bytemuck` dependency. Big-endian hosts retain upstream's conversion loop.
- Check sorted, unique array members with a reduction that LLVM can vectorize.
  Invalid arrays still use the original scan and report the first error.

The portable Roaring format, checked deserialization, and public API are
unchanged. This avoids implementing a separate bitmap format in the codec.
The patch adds no unsafe code. Performance was measured on macOS ARM64;
big-endian performance has not been measured. Remove this vendored copy when
an upstream release provides equivalent optimizations.
