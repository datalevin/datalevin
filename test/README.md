# CI tests

This directory contains the core smoke suite used by `datalevin.test0`, the pod
checks, and fast unit checks for transaction execution, cache invalidation,
serialization, protocol contexts, and internal API contracts. Run `lein test` for
the CI suite; `lein run` runs the `test0` subset used by release builds.

Keep broad regression matrices, persistent-store lifecycle tests, WAL recovery,
query/prepared-read integration, and multi-client/server scenarios in the sibling
`../dtlvtest/test` project. Its `datalevin.test1` runner covers native-compatible
regressions. Add a namespace to both the static `:require` and `test-namespaces`
when it should run in the native image. Tests requiring an external JVM, Cargo,
or JVM-only tooling belong in its `jvm-only-test-namespaces` list instead.

Shared test fixtures needed by this repository's builds and language binding
checks remain in `test/data`; test-only adapter infrastructure lives in
`test-src` so the sibling checkout can use it too.

When implementation moves to another namespace, update its tests to call the
owning namespace. Do not keep forwarding functions or pass-through stubs in
production just for old tests. Test handler behavior through the active handler,
including failure and cleanup paths, instead of testing a retired helper.
Check Java callers that resolve Clojure vars by name before removing a var that
appears unused to Clojure lint.
