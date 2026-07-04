export { Client } from "./client.js";
export { Connection } from "./connection.js";
export { Database } from "./database.js";
export { Entity } from "./entity.js";
export {
  DatalevinConfigurationError,
  DatalevinError,
  DatalevinJavaError,
  DatalevinJvmError
} from "./errors.js";
export {
  apiInfo,
  connect,
  datom,
  datalogKv,
  embeddingAttr,
  embeddingOptions,
  execJson,
  explicitTransactionTimeout,
  fillDb,
  fulltextAttr,
  idocAttr,
  idocOptions,
  initDb,
  jvmStarted,
  keyword,
  maxEid,
  newClient,
  newLlamaEmbedder,
  newLlamaGenerator,
  newSearchEngine,
  newVectorIndex,
  openKv,
  readEdn,
  reIndex,
  schemaAttr,
  searchIndexWriter,
  searchDomain,
  searchOptions,
  setExplicitTransactionTimeout,
  startJvm,
  symbol,
  transactAsync,
  txDataToSimulatedReport,
  txAdd,
  txEntity,
  txRetract,
  txRetractEntity,
  vectorAttr,
  vectorOptions,
  withTransaction,
  writeEdn
} from "./interop.js";
export { interop } from "./raw.js";
export { KV, KVTransaction, RawBuffer, RawKV } from "./kv.js";
export { LlamaEmbedder, LlamaGenerator } from "./llm.js";
export { SearchEngine, SearchIndexWriter } from "./search.js";
export { UdfRegistry, createUdfRegistry, udfDescriptor } from "./udf.js";
export { VectorIndex } from "./vector.js";
