export { Client } from "./client.js";
export { Connection } from "./connection.js";
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
  openKv,
  readEdn,
  schemaAttr,
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
  writeEdn
} from "./interop.js";
export { interop } from "./raw.js";
export { KV } from "./kv.js";
export { UdfRegistry, createUdfRegistry, udfDescriptor } from "./udf.js";
