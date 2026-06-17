export { Client } from "./client.js";
export { Connection } from "./connection.js";
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
  fillDb,
  fulltextAttr,
  idocAttr,
  idocOptions,
  initDb,
  jvmStarted,
  keyword,
  newClient,
  newSearchEngine,
  newVectorIndex,
  openKv,
  readEdn,
  reIndex,
  schemaAttr,
  searchIndexWriter,
  searchDomain,
  searchOptions,
  startJvm,
  symbol,
  transactAsync,
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
export { KV, KVTransaction } from "./kv.js";
export { SearchEngine, SearchIndexWriter } from "./search.js";
export { UdfRegistry, createUdfRegistry, udfDescriptor } from "./udf.js";
export { VectorIndex } from "./vector.js";
