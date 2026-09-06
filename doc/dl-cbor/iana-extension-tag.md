# Draft IANA CBOR Tag Registration: Datalevin Extension

Status: internal Phase 0 checklist. This request has not been submitted. Do not
submit it until the codec performance gates pass, the extension registry is
complete enough for review, and the project maintainer supplies the contact
information and stable specification URL.

The IANA CBOR tag registry was checked on 2026-09-04. It had last been updated
on 2026-07-20. RFC 8949 assigns tags 24 through 32767 by Specification Required
and larger tags by First Come First Served; there is no general private-use tag
range. Phase 0 uses unassigned tag 17484 (`0x444c`) as a mnemonic stand-in only.
It is not reserved and must never appear in a durable Datalevin artifact.

## RFC 8949 registration fields

- Requested tag: IANA-selected. Prefer an available tag in 24 through 255 if
  expert review accepts the per-value size justification; otherwise select an
  available Specification Required value. Tag 17484 is not required.
- Data item: a non-empty, definite-length CBOR array.
- Semantics (short form): Datalevin platform-neutral extension value; the first
  array item identifies the semantic type and the remaining items are its
  arguments.
- Point of contact: **TBD by the project maintainer**.
- Description of semantics: stable URL for the released version of
  [`v1.md`](v1.md), with the assigned tag and immutable vectors.

## Semantic summary for expert review

The tag content is `[type-id, * arguments]`. A type ID is either a non-negative
integer no larger than `9223372036854775807`, or a globally unique non-empty
text string. Compact integer IDs are assigned by the DL-CBOR specification;
text IDs are the open extension space. Arguments are recursively valid DL-CBOR
values. Unknown extensions are data and never trigger class loading, object
deserialization, or code execution.

The remaining initial integer IDs are list (4) and queue (5). Keyword, symbol,
character, and regex IDs 1, 2, 3, and 6 are retired: those values now use local
byte-string subtypes and do not need an IANA tag assignment. Incompatible
revisions receive a new type ID; an existing ID is never redefined.

The selected collection framing retains lists and queues in this flattened
tagged array. Sets retain their standard tag 258, while vectors and maps use
native CBOR containers. The measured compact collection byte-string alternative
was not selected.

## Checks before submission

- Recheck the live [IANA CBOR tag registry](https://www.iana.org/assignments/cbor-tags).
- Complete the controlled extension-rich throughput, allocation, and small-key
  encoded-size gates against Nippy.
- Validate the remaining extension payloads against the small-key size gate,
  including the effect of a 24-through-255 assignment; collection framing is
  settled as described above.
- Reconfirm the regex contract and initial built-in registry during format
  review.
- Replace the stand-in tag in both implementations and the CDDL.
- Regenerate the draft extension vectors under the assigned number, review the
  bytes, and merge them into the normative append-only corpus.
- Publish a stable public specification URL and fill in the point of contact.
- Obtain the review required by the selected IANA allocation range.
