# Data Compression in Datalevin (WIP)

In addition to the obvious benefit of reducing storage space, data compression
helps to increase data ingestion and query speed, due to faster key comparison,
better cache locality and reduced workload in general, provided that the
computational overhead of compression and decompression is comparatively low
enough.

## Key Compression

For keys, we use an order preserving compression method, so that range queries
and some predicates can run directly on the compressed data without having to
decompress data first.

A complete dictionary assigns variable-length alphabetic Hu-Tucker codes [1, 2]
to byte pairs and key terminators. The builder uses mergeable priority queues
[3]. Frequency collection starts with a count of one for each symbol so that
unseen input always has a code. The rank sampler uses Floyd sampling without
replacement, with an optional seed for reproducible samples.

The alphabet has 65,793 symbols: 65,536 byte pairs, 256 terminals
for an odd final byte, and one end-of-key terminal. For unsigned bytes `b, c`,
the symbol ranks are:

| Symbol | Rank |
|--------|------|
| End of key | `0` |
| Final unpaired byte `b` | `1 + 257*b` |
| Pair `b, c` | `2 + 257*b + c` |

Encoding writes the codes for full pairs, followed by an end-of-key code for
even-length input or a final-byte code for odd-length input. Only unused bits
of the last byte are padded with zeros. There is no length trailer. Terminals
sort before every continuation of the same prefix; alphabetic, prefix-free
codes therefore preserve unsigned byte-string ordering, including empty keys,
odd-length keys, and trailing zero bytes.

`keycode.bin` stores the `HUTU` header, dictionary version, and arrays of code
lengths and 32-bit codes. Four-bit lookup tables [4] are rebuilt when loading
the dictionary. Each lookup retains every completed symbol, emitting up to
eight bytes, and stops at a key terminator. The two lookup arrays occupy about
12 MiB per dictionary, in addition to the code arrays.

Changing a dictionary requires rebuilding its encoded keys. Never replace a
dictionary underneath existing encoded keys or mix dictionaries in one ordered
stream.

The current experimental binding uses environment-wide `:key-compress :hu`
and `:val-compress :zstd` options with prebuilt dictionary files. Select these
when creating a persistent environment. Its uncompressed metadata records the
compression methods, generation, and dictionary SHA-256s, so reopening requires
no compression options. Missing or changed dictionaries and conflicting explicit
options prevent opening. Changing compression requires a new rebuilt environment;
never replace dictionary files in place. Physical copies carry both dictionaries.
Temporary and in-memory stores do not support these options.

With a standard 511-byte key buffer, serialized raw keys up to **253 bytes**
are guaranteed to fit after encoding, including type headers. Longer raw keys
are accepted only if their actual encoded size fits within 511 bytes; expansion
past that limit aborts the write transaction. Smaller configured key buffers
reduce the guarantee. There is no raw fallback because mixing encodings would
break ordering.

Automatic training and safe dictionary replacement are not yet a supported
lifecycle. Storage work remains, including auditing native Datalog access paths
and validating a complete staged rebuild before activation. See
[the compression plan](../compression-plan.md) for the evaluation and remaining
work.

## Value Compression

The experimental environment value compressor uses a Zstd dictionary stored
in `valcode.bin` and is selected with `:val-compress :zstd`. The binding rejects
non-DUPFIXED list DBIs when this option is enabled: zstd does not preserve their
duplicate ordering. DUPFIXED values always remain raw. This also prevents using
the environment value compressor on Datalog stores, whose EAV index has
variable-width duplicates. Separate per-DBI controls and an ordered duplicate
codec remain future work. General payload compression thresholds
are a separate evaluation from the key dictionary.

## Wire Compression

By default, lz4 compression is used for data sent between client and server. Set
`:compress-message?` option to `false` on the client to disable compression,
e.g. to work with versions of the server prior to `0.10.0`.


## Benchmark

### Compression ratio

### Run time performance

#### Write

#### Query

## References

[1] Zhang, et al. "Order-preserving key compression for in-memory search trees."
SIGMOD 2020.

[2] Hu, Te C., and Alan C. Tucker. "Optimal computer search trees and
variable-length alphabetical codes." SIAM Journal on Applied Mathematics 21.4
(1971): 514-532.

[3] Davis, Sashka, "Hu-Tucker algorithm for building optimal alphabetic binary
search trees" (1998). Master Thesis. Rochester Institute of Technology.

[4] Bergman, Eyal, and Shmuel T. Klein. "Fast decoding of prefix encoded texts."
IEEE Data Compression Conference 2005.
