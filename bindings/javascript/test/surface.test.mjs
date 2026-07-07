import assert from "node:assert/strict";
import test from "node:test";

import * as datalevin from "../src/index.js";
import { _BINDINGS } from "../src/interop.js";

test("public surface stays importable without starting the JVM", () => {
  assert.equal(typeof datalevin.abortTransact, "function");
  assert.equal(typeof datalevin.analyze, "function");
  assert.equal(typeof datalevin.apiInfo, "function");
  assert.equal(typeof datalevin.cardinality, "function");
  assert.equal(typeof datalevin.connect, "function");
  assert.equal(typeof datalevin.createUdfRegistry, "function");
  assert.equal(typeof datalevin.datom, "function");
  assert.equal(typeof datalevin.datalogKv, "function");
  assert.equal(typeof datalevin.embeddingAttr, "function");
  assert.equal(typeof datalevin.embeddingOptions, "function");
  assert.equal(typeof datalevin.execJson, "function");
  assert.equal(typeof datalevin.explicitTransactionTimeout, "function");
  assert.equal(typeof datalevin.fillDb, "function");
  assert.equal(typeof datalevin.fulltextAttr, "function");
  assert.equal(typeof datalevin.idocAttr, "function");
  assert.equal(typeof datalevin.idocOptions, "function");
  assert.equal(typeof datalevin.initDb, "function");
  assert.equal(typeof datalevin.interop, "function");
  assert.equal(typeof datalevin.jvmStarted, "function");
  assert.equal(typeof datalevin.keyword, "function");
  assert.equal(typeof datalevin.maxEid, "function");
  assert.equal(typeof datalevin.newClient, "function");
  assert.equal(typeof datalevin.newLlamaEmbedder, "function");
  assert.equal(typeof datalevin.newLlamaGenerator, "function");
  assert.equal(typeof datalevin.newSearchEngine, "function");
  assert.equal(typeof datalevin.newVectorIndex, "function");
  assert.equal(typeof datalevin.openKv, "function");
  assert.equal(typeof datalevin.readEdn, "function");
  assert.equal(typeof datalevin.reIndex, "function");
  assert.equal(typeof datalevin.schemaAttr, "function");
  assert.equal(typeof datalevin.searchIndexWriter, "function");
  assert.equal(typeof datalevin.searchDomain, "function");
  assert.equal(typeof datalevin.searchOptions, "function");
  assert.equal(typeof datalevin.setExplicitTransactionTimeout, "function");
  assert.equal(typeof datalevin.startJvm, "function");
  assert.equal(typeof datalevin.symbol, "function");
  assert.equal(typeof datalevin.transact, "function");
  assert.equal(typeof datalevin.transactAsync, "function");
  assert.equal(typeof datalevin.txDataToSimulatedReport, "function");
  assert.equal(typeof datalevin.txAdd, "function");
  assert.equal(typeof datalevin.txEntity, "function");
  assert.equal(typeof datalevin.txRetract, "function");
  assert.equal(typeof datalevin.txRetractEntity, "function");
  assert.equal(typeof datalevin.udfDescriptor, "function");
  assert.equal(typeof datalevin.vectorAttr, "function");
  assert.equal(typeof datalevin.vectorOptions, "function");
  assert.equal(typeof datalevin.withTransaction, "function");
  assert.equal(typeof datalevin.writeEdn, "function");
  assert.deepEqual(datalevin.schemaAttr({ valueType: ":db.type/string", unique: ":db.unique/identity" }), {
    ":db/valueType": ":db.type/string",
    ":db/unique": ":db.unique/identity"
  });
  assert.deepEqual(datalevin.fulltextAttr({ domains: ["docs"], autoDomain: true }), {
    ":db/valueType": ":db.type/string",
    ":db/fulltext": true,
    ":db.fulltext/domains": ["docs"],
    ":db.fulltext/autoDomain": true
  });
  assert.deepEqual(datalevin.embeddingAttr({ domains: ["docs"], autoDomain: true }), {
    ":db/valueType": ":db.type/string",
    ":db/embedding": true,
    ":db.embedding/domains": ["docs"],
    ":db.embedding/autoDomain": true
  });
  assert.deepEqual(datalevin.vectorAttr({ domains: ["vectors"] }), {
    ":db/valueType": ":db.type/vec",
    ":db.vec/domains": ["vectors"]
  });
  assert.deepEqual(datalevin.idocAttr({
    format: "json",
    domain: "profiles",
    indexedPaths: [":status", [":profile", ":age"]],
    excludedPaths: [":raw"]
  }), {
    ":db/valueType": ":db.type/idoc",
    ":db/idocFormat": ":json",
    ":db/domain": "profiles",
    ":db.idoc/indexedPaths": [":status", [":profile", ":age"]],
    ":db.idoc/excludedPaths": [":raw"]
  });
  assert.deepEqual(datalevin.idocDomain({
    indexedPaths: [":status"],
    excludedPaths: [[":profile", ":raw"]]
  }), {
    ":indexed-paths": [":status"],
    ":excluded-paths": [[":profile", ":raw"]]
  });
  assert.deepEqual(datalevin.searchOptions({
    top: 5,
    limit: 2,
    offset: 4,
    pagingCachePages: 3,
    display: "refs+scores",
    domains: ["docs"]
  }), {
    ":top": 5,
    ":limit": 2,
    ":offset": 4,
    ":paging-cache-pages": 3,
    ":display": ":refs+scores",
    ":domains": ["docs"]
  });
  assert.deepEqual(datalevin.searchDomain({
    domain: "docs",
    indexPosition: true,
    includeText: true,
    indexingMode: "async"
  }), {
    ":domain": "docs",
    ":index-position?": true,
    ":include-text?": true,
    ":indexing-mode": ":async"
  });
  assert.deepEqual(datalevin.vectorOptions({ dimensions: 384, metricType: "cosine" }), {
    ":dimensions": 384,
    ":metric-type": ":cosine"
  });
  assert.deepEqual(datalevin.embeddingOptions({
    provider: "openai-compatible",
    model: "text-embedding-3-small",
    apiKeyEnv: "OPENAI_API_KEY",
    requestDimensions: 1536,
    metricType: "cosine"
  }), {
    ":provider": ":openai-compatible",
    ":model": "text-embedding-3-small",
    ":api-key-env": "OPENAI_API_KEY",
    ":request-dimensions": 1536,
    ":metric-type": ":cosine"
  });
  assert.deepEqual(datalevin.idocOptions({ domains: ["profiles"] }), {
    ":domains": ["profiles"]
  });
  assert.deepEqual(datalevin.txEntity(-1, { ":name": "Ada" }), { ":db/id": -1, ":name": "Ada" });
  assert.deepEqual(datalevin.txAdd(1, "name", "Ada"), [":db/add", 1, ":name", "Ada"]);
  assert.deepEqual(datalevin.txRetract(1, ":name", "Ada"), [":db/retract", 1, ":name", "Ada"]);
  assert.deepEqual(datalevin.txRetractEntity(1), [":db/retractEntity", 1]);
  assert.deepEqual(datalevin.udfDescriptor("math/inc"), {
    ":udf/lang": ":java",
    ":udf/kind": ":query-fn",
    ":udf/id": ":math/inc"
  });
  assert.equal(typeof datalevin.Connection, "function");
  assert.equal(typeof datalevin.Entity, "function");
  assert.equal(typeof datalevin.KV, "function");
  assert.equal(typeof datalevin.KVTransaction, "function");
  assert.equal(typeof datalevin.RawBuffer, "function");
  assert.equal(typeof datalevin.RawKV, "function");
  assert.equal(typeof datalevin.LlamaEmbedder, "function");
  assert.equal(typeof datalevin.LlamaGenerator, "function");
  assert.equal(typeof datalevin.SearchEngine, "function");
  assert.equal(typeof datalevin.UdfRegistry.prototype.analyzerUdf, "function");
  assert.equal(typeof datalevin.UdfRegistry.prototype.queryAnalyzerUdf, "function");
  assert.equal(typeof datalevin.SearchIndexWriter, "function");
  assert.equal(typeof datalevin.VectorIndex, "function");
  assert.equal(typeof datalevin.UdfRegistry, "function");
  assert.equal(typeof datalevin.Client, "function");
  for (const method of [
    "clientId",
    "closeDatabase",
    "createDatabase",
    "disconnect",
    "disconnectClient",
    "disconnected",
    "dropDatabase",
    "haUpdateMembership",
    "listDatabases",
    "listDatabasesInUse",
    "openDatabase",
    "querySystem",
    "replicaStatus",
    "showClients"
  ]) {
    assert.equal(typeof datalevin.Client.prototype[method], "function");
  }
  assert.equal(typeof datalevin.Entity.prototype.touch, "function");
  assert.equal(typeof datalevin.Database.prototype.analyze, "function");
  assert.equal(typeof datalevin.Database.prototype.cardinality, "function");
  assert.equal(typeof datalevin.Connection.prototype.fillDb, "function");
  for (const method of [
    "analyze",
    "cardinality",
    "countDatoms",
    "copy",
    "createSnapshot",
    "datalogKv",
    "datalogIndexCacheLimit",
    "datoms",
    "entityMap",
    "fulltextDatoms",
    "gcTxLogSegments",
    "indexRange",
    "listen",
    "listSnapshots",
    "maxEid",
    "openTxLog",
    "reIndex",
    "rseekDatoms",
    "searchDatoms",
    "seekDatoms",
    "txLogWatermarks",
    "abortTransact",
    "transact",
    "transactAsync",
    "txDataToSimulatedReport",
    "unlisten"
  ]) {
    assert.equal(typeof datalevin.Connection.prototype[method], "function");
  }

  for (const method of [
    "beginTransaction",
    "copy",
    "createSnapshot",
    "delListItems",
    "gcTxLogSegments",
    "getByRank",
    "getEntryByRank",
    "getEnvFlags",
    "getFirst",
    "getFirstN",
    "getList",
    "getRank",
    "inList",
    "keyRangeListCount",
    "keyRange",
    "keyRangeCount",
    "listCount",
    "listRange",
    "listRangeCount",
    "listRangeFilter",
    "listRangeFilterRaw",
    "listRangeFilterCount",
    "listRangeFilterCountRaw",
    "listRangeFirst",
    "listRangeFirstN",
    "listRangeKeep",
    "listRangeKeepRaw",
    "listRangeSome",
    "listRangeSomeRaw",
    "listSnapshots",
    "newSearchEngine",
    "newVectorIndex",
    "openTxLog",
    "putListItems",
    "rangeCount",
    "reIndex",
    "sampleKv",
    "searchIndexWriter",
    "setEnvFlags",
    "stat",
    "sync",
    "transaction",
    "txLogWatermarks",
    "visit",
    "visitKeyRange",
    "visitKeyRangeRaw",
    "visitList",
    "visitListRaw",
    "visitListRange",
    "visitListRangeRaw",
    "visitRaw",
    "withTransaction"
  ]) {
    assert.equal(typeof datalevin.KV.prototype[method], "function");
  }
  for (const method of ["bytes", "rawHandle", "read"]) {
    assert.equal(typeof datalevin.RawBuffer.prototype[method], "function");
  }
  for (const method of ["key", "keyBytes", "rawHandle", "readKey", "readValue", "value", "valueBytes"]) {
    assert.equal(typeof datalevin.RawKV.prototype[method], "function");
  }
  for (const method of ["abort", "active", "close", "commit"]) {
    assert.equal(typeof datalevin.KVTransaction.prototype[method], "function");
  }
  for (const method of [
    "addDoc",
    "clearDocs",
    "docCount",
    "docIndexed",
    "reIndex",
    "removeDoc",
    "search"
  ]) {
    assert.equal(typeof datalevin.SearchEngine.prototype[method], "function");
  }
  assert.equal(typeof datalevin.SearchIndexWriter.prototype.write, "function");
  assert.equal(typeof datalevin.SearchIndexWriter.prototype.commit, "function");
  for (const method of [
    "batchSize",
    "close",
    "closed",
    "contextSize",
    "ctxSize",
    "detokenize",
    "dimensions",
    "embed",
    "embedAll",
    "gpuLayers",
    "modelPath",
    "threads",
    "tokenCount",
    "tokenize",
    "truncateText"
  ]) {
    assert.equal(typeof datalevin.LlamaEmbedder.prototype[method], "function");
  }
  for (const method of [
    "close",
    "closed",
    "contextSize",
    "ctxSize",
    "generate",
    "gpuLayers",
    "modelPath",
    "summarize",
    "threads",
    "tokenCount"
  ]) {
    assert.equal(typeof datalevin.LlamaGenerator.prototype[method], "function");
  }
  for (const method of [
    "addVec",
    "checkpointState",
    "clear",
    "forceCheckpoint",
    "info",
    "reIndex",
    "removeVec",
    "searchVec",
    "vecIndexed"
  ]) {
    assert.equal(typeof datalevin.VectorIndex.prototype[method], "function");
  }
});

test("transact uses the blocking transaction bridge", async () => {
  const original = _BINDINGS.connectionTransact;
  const calls = [];
  _BINDINGS.connectionTransact = async (handle, txData, txMeta = null) => {
    calls.push({ handle, txData, txMeta });
    return { ":tx-data": ["ok"], ":tx-meta": txMeta };
  };

  try {
    const conn = new datalevin.Connection("CONN", { owned: false });
    const txData = [{ ":db/id": -1, ":name": "Ada" }];
    const txMeta = { ":source": "surface" };

    assert.deepEqual(await conn.transact(txData, txMeta), {
      ":tx-data": ["ok"],
      ":tx-meta": txMeta
    });
    assert.deepEqual(await datalevin.transact(conn, txData), {
      ":tx-data": ["ok"],
      ":tx-meta": null
    });
    assert.deepEqual(await datalevin.interop().connectionTransact(conn, txData, txMeta), {
      ":tx-data": ["ok"],
      ":tx-meta": txMeta
    });
    assert.deepEqual(calls, [
      { handle: "CONN", txData, txMeta },
      { handle: "CONN", txData, txMeta: null },
      { handle: "CONN", txData, txMeta }
    ]);
  } finally {
    _BINDINGS.connectionTransact = original;
  }
});

test("abortTransact uses the Datalog abort bridge", async () => {
  const original = _BINDINGS.connectionAbortTransact;
  const calls = [];
  _BINDINGS.connectionAbortTransact = async (handle) => {
    calls.push(handle);
    return null;
  };

  try {
    const conn = new datalevin.Connection("CONN", { owned: false });

    assert.equal(await conn.abortTransact(), null);
    assert.equal(await datalevin.abortTransact(conn), null);
    assert.equal(await datalevin.interop().connectionAbortTransact(conn), null);
    assert.deepEqual(calls, ["CONN", "CONN", "CONN"]);
  } finally {
    _BINDINGS.connectionAbortTransact = original;
  }
});

test("top-level withTransaction explains unsupported Connection targets", async () => {
  const conn = new datalevin.Connection({}, { owned: false });
  const explainsUnsupportedConnectionTransaction = (error) => {
    assert.equal(error instanceof datalevin.DatalevinError, true);
    assert.match(error.message, /Java interface callbacks deadlock/);
    assert.match(error.message, /Use transact\(\)/);
    return true;
  };

  await assert.rejects(
    () => datalevin.withTransaction(conn, async () => null),
    explainsUnsupportedConnectionTransaction
  );
  await assert.rejects(
    () => datalevin.interop().connectionWithTransaction({}, async () => null),
    explainsUnsupportedConnectionTransaction
  );
});
