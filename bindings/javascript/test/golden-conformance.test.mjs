import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import {
  DatalogSymbol,
  Keyword,
  connect,
  q,
  readEdn,
  tx
} from "../src/index.js";
import { toJs } from "../src/convert.js";
import { classes, resolveClasspath } from "../src/jvm.js";

const MISSING = Symbol("missing");
const QUERY_SECTIONS = new Set([
  "find",
  "with",
  "in",
  "where",
  "having",
  "order-by",
  "offset",
  "limit",
  "timeout",
  "keys",
  "strs",
  "syms"
]);
const ERROR_PATTERNS = [
  [":dl/invalid-value-type", /invalid data|value type|:db\/valueType/i],
  [":dl/unique-constraint", /unique/i],
  [":dl/conflicting-upsert", /conflicting upsert|upsert.*conflict|conflict.*upsert/i]
];

const runtimeAvailable = (() => {
  try {
    resolveClasspath();
    return true;
  } catch {
    return false;
  }
})();

function dtlvtestRoot() {
  const currentFile = fileURLToPath(import.meta.url);
  let root = process.env.DTLVTEST_ROOT
    ? path.resolve(process.env.DTLVTEST_ROOT)
    : path.resolve(path.dirname(currentFile), "../../../..", "dtlvtest");
  if (fs.existsSync(path.join(root, "manifest.edn"))) {
    root = path.dirname(root);
  }
  return fs.existsSync(path.join(root, "spec", "manifest.edn")) ? root : null;
}

function instanceOf(value, type) {
  if (value === null || typeof value !== "object" || typeof value.instanceOf !== "function") {
    return false;
  }
  try {
    return value.instanceOf(type);
  } catch {
    return false;
  }
}

function materialize(collection) {
  if (typeof collection?.toArraySync === "function") {
    try {
      const values = collection.toArraySync();
      return Array.isArray(values) ? values : Array.from(values);
    } catch {
      // Some Clojure collections only expose an iterator through java-bridge.
    }
  }
  const result = [];
  const iterator = collection.iteratorSync();
  while (iterator.hasNextSync()) {
    result.push(iterator.nextSync());
  }
  return result;
}

async function decodeEdn(value, cls) {
  if (instanceOf(value, cls.keywordType)) {
    return q.kw(value.toStringSync());
  }
  if (instanceOf(value, cls.symbolType)) {
    return q.sym(value.toStringSync());
  }
  if (instanceOf(value, cls.mapType) || typeof value?.entrySetSync === "function") {
    const result = new Map();
    for (const entry of materialize(value.entrySetSync())) {
      result.set(
        await decodeEdn(entry.getKeySync(), cls),
        await decodeEdn(entry.getValueSync(), cls)
      );
    }
    return result;
  }
  if (instanceOf(value, cls.setType)) {
    const result = new Set();
    for (const item of materialize(value)) {
      result.add(await decodeEdn(item, cls));
    }
    return result;
  }
  if (
    instanceOf(value, cls.listType)
    || instanceOf(value, cls.collectionType)
    || typeof value?.iteratorSync === "function"
  ) {
    const result = [];
    for (const item of materialize(value)) {
      result.push(await decodeEdn(item, cls));
    }
    return result;
  }
  return toJs(value);
}

async function readEdnFile(filePath, cls) {
  const source = fs.readFileSync(filePath, "utf8");
  return decodeEdn(await readEdn(source), cls);
}

function entries(mapping) {
  return mapping instanceof Map ? mapping.entries() : Object.entries(mapping);
}

function field(mapping, name, defaultValue = MISSING) {
  for (const [key, value] of entries(mapping)) {
    if (key instanceof Keyword && key.name === name) {
      return value;
    }
    if (key === `:${name}`) {
      return value;
    }
  }
  if (defaultValue !== MISSING) {
    return defaultValue;
  }
  throw new TypeError(`Missing golden field :${name}.`);
}

function mapValue(mapping, wantedKey) {
  for (const [key, value] of entries(mapping)) {
    if (key instanceof Keyword && wantedKey instanceof Keyword && key.name === wantedKey.name) {
      return value;
    }
    if (key === wantedKey) {
      return value;
    }
  }
  throw new TypeError(`Missing golden map key ${String(wantedKey)}.`);
}

function tokenName(value) {
  return value instanceof Keyword || value instanceof DatalogSymbol ? value.name : null;
}

function term(value) {
  if (value instanceof DatalogSymbol) {
    if (value.name.startsWith("?")) {
      return q.var(value);
    }
    if (value.name.startsWith("$")) {
      return q.source(value);
    }
    if (value.name === "%") {
      return q.RULES;
    }
    if (value.name === "_") {
      return q.IGNORE;
    }
  }
  return value;
}

function binding(value) {
  if (!Array.isArray(value)) {
    return term(value);
  }
  if (value.length === 2 && tokenName(value[1]) === "...") {
    return q.collectionBinding(term(value[0]));
  }
  if (value.length === 1 && Array.isArray(value[0])) {
    return q.relationBinding(...value[0].map(term));
  }
  return q.tupleBinding(...value.map(term));
}

function findTerm(value) {
  if (Array.isArray(value)) {
    if (value.length === 0 || !(value[0] instanceof DatalogSymbol)) {
      throw new TypeError(`Unsupported golden find expression: ${String(value)}.`);
    }
    return q.aggregate(value[0], ...value.slice(1).map(term));
  }
  return term(value);
}

function findSpec(values) {
  if (values.length === 0) {
    throw new TypeError("A golden query must contain at least one :find value.");
  }
  if (tokenName(values.at(-1)) === ".") {
    const terms = values.slice(0, -1).map(findTerm);
    return terms.length === 1 ? q.scalar(terms[0]) : q.tupleScalar(...terms);
  }
  if (values.length === 1 && Array.isArray(values[0])) {
    const nested = values[0];
    if (nested.length === 2 && tokenName(nested[1]) === "...") {
      return q.collection(findTerm(nested[0]));
    }
    return q.tuple(...nested.map(findTerm));
  }
  return q.relation(...values.map(findTerm));
}

function clause(value) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new TypeError(`Unsupported golden where clause: ${String(value)}.`);
  }
  if (Array.isArray(value[0])) {
    const call = value[0];
    if (call.length === 0 || !(call[0] instanceof DatalogSymbol)) {
      throw new TypeError(`Unsupported golden call clause: ${String(value)}.`);
    }
    const args = call.slice(1).map(term);
    if (value.length === 1) {
      return q.predicate(call[0], ...args);
    }
    if (value.length === 2) {
      return q.bind(call[0], binding(value[1]), ...args);
    }
    throw new TypeError(`Unsupported golden call clause: ${String(value)}.`);
  }

  const terms = [...value];
  let sourceValue = null;
  if (terms[0] instanceof DatalogSymbol && terms[0].name.startsWith("$")) {
    sourceValue = q.source(terms.shift());
  }
  if (
    terms[0] instanceof DatalogSymbol
    && !terms[0].name.startsWith("?")
    && !terms[0].name.startsWith("$")
  ) {
    return sourceValue === null
      ? q.rule(terms[0], ...terms.slice(1).map(term))
      : q.ruleFrom(sourceValue, terms[0], ...terms.slice(1).map(term));
  }
  return sourceValue === null
    ? q.pattern(...terms.map(term))
    : q.patternFrom(sourceValue, ...terms.map(term));
}

function orders(inputValues) {
  const values = inputValues.length === 1 && Array.isArray(inputValues[0])
    ? inputValues[0]
    : inputValues;
  const result = [];
  for (let index = 0; index < values.length;) {
    const value = term(values[index]);
    const direction = index + 1 < values.length ? tokenName(values[index + 1]) : null;
    if (direction === "asc" || direction === "desc") {
      result.push(direction === "desc" ? q.desc(value) : q.asc(value));
      index += 2;
    } else {
      result.push(q.asc(value));
      index += 1;
    }
  }
  return result;
}

function query(form) {
  const sections = new Map();
  let current = null;
  for (const item of form) {
    const name = tokenName(item);
    if (item instanceof Keyword && QUERY_SECTIONS.has(name)) {
      current = name;
      if (!sections.has(name)) {
        sections.set(name, []);
      }
    } else if (current === null) {
      throw new TypeError(`Golden query value appears before a section: ${String(item)}.`);
    } else {
      sections.get(current).push(item);
    }
  }

  const options = {
    find: findSpec(sections.get("find") ?? []),
    where: (sections.get("where") ?? []).map(clause),
    inputs: (sections.get("in") ?? []).map(binding),
    withVars: (sections.get("with") ?? []).map(term),
    having: (sections.get("having") ?? []).map(clause),
    orderBy: orders(sections.get("order-by") ?? [])
  };
  for (const name of ["offset", "limit", "timeout"]) {
    if ((sections.get(name) ?? []).length > 0) {
      options[name] = sections.get(name)[0];
    }
  }
  for (const name of ["keys", "strs", "syms"]) {
    if ((sections.get(name) ?? []).length > 0) {
      options[name] = sections.get(name);
    }
  }
  return q.query(options);
}

function txItem(value) {
  if (value instanceof Map) {
    return tx.entity(value);
  }
  if (!Array.isArray(value) || value.length === 0 || !(value[0] instanceof Keyword)) {
    throw new TypeError(`Unsupported golden transaction item: ${String(value)}.`);
  }
  const op = value[0].name;
  const args = value.slice(1);
  if (op === "db/add") {
    return tx.add(...args);
  }
  if (op === "db/retract") {
    return tx.retract(...args);
  }
  if (op === "db/retractAttribute" || op === "db.fn/retractAttribute") {
    return tx.retractAttribute(...args);
  }
  if (op === "db/retractEntity" || op === "db.fn/retractEntity") {
    return tx.retractEntity(...args);
  }
  if (op === "db/cas" || op === "db.fn/cas") {
    return tx.cas(...args);
  }
  if (op === "db.fn/call") {
    return tx.call(...args);
  }
  if (op === "db/ensure") {
    return tx.ensure(...args);
  }
  return tx.invoke(value[0], ...args);
}

function txData(values) {
  return tx.data(values.map(txItem));
}

function canonical(value) {
  if (value instanceof Keyword || value instanceof DatalogSymbol) {
    return value.toString();
  }
  if (typeof value === "bigint") {
    return value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER
      ? Number(value)
      : `${value}n`;
  }
  if (value instanceof Map) {
    const result = {};
    for (const [key, item] of value) {
      result[String(canonical(key))] = canonical(item);
    }
    return result;
  }
  if (value instanceof Set) {
    return Array.from(value, canonical);
  }
  if (Array.isArray(value)) {
    return value.map(canonical);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, canonical(item)])
    );
  }
  return value;
}

function stableSort(values) {
  return [...values].sort((left, right) => (
    JSON.stringify(left).localeCompare(JSON.stringify(right))
  ));
}

function normalize(value, resultKind) {
  const kind = tokenName(resultKind);
  const result = canonical(value);
  if (kind === "scalar") {
    return result;
  }
  if (kind === "tuple" || kind === "seq-of-scalars" || kind === "seq-of-tuples") {
    return [...result];
  }
  if (kind === "set-of-scalars" || kind === "set-of-tuples") {
    return stableSort(result);
  }
  throw new TypeError(`Unsupported golden result kind: ${String(resultKind)}.`);
}

function errorId(error) {
  const data = error?.data;
  if (data instanceof Map) {
    for (const [key, value] of data) {
      if (key === ":error/id" || key === ":dl/error-id") {
        return String(value);
      }
    }
  }
  const message = String(error?.message ?? error);
  for (const [id, pattern] of ERROR_PATTERNS) {
    if (pattern.test(message)) {
      return id;
    }
  }
  return null;
}

async function activeCases(root, cls) {
  const specRoot = path.join(root, "spec");
  const manifest = await readEdnFile(path.join(specRoot, "manifest.edn"), cls);
  const activeRelease = field(manifest, "active-release");
  const release = await readEdnFile(
    path.join(specRoot, "releases", `${activeRelease}.edn`),
    cls
  );
  const suites = field(manifest, "suites");
  const result = [];
  for (const suiteId of field(release, "required-suites")) {
    const entry = mapValue(suites, suiteId);
    const suite = await readEdnFile(path.join(root, field(entry, "path")), cls);
    const dataset = await readEdnFile(path.join(root, field(suite, "dataset")), cls);
    for (const caseDefinition of field(suite, "cases")) {
      result.push({ activeRelease, suiteId, dataset, caseDefinition });
    }
  }
  return result;
}

async function runCase(dbDir, dataset, caseDefinition) {
  const conn = await connect(dbDir, {
    schema: field(dataset, "schema"),
    opts: field(dataset, "conn-opts", null)
  });
  try {
    const seedTx = field(dataset, "seed-tx", null);
    if (seedTx !== null) {
      await conn.transact(txData(seedTx));
    }

    const operation = tokenName(field(caseDefinition, "op"));
    const matcher = tokenName(field(caseDefinition, "matcher"));
    const caseTx = field(caseDefinition, "tx-data", null);
    if (operation === "query-after-tx" && caseTx !== null) {
      await conn.transact(txData(caseTx));
    }

    if (matcher === "equals") {
      const resultKind = field(caseDefinition, "result-kind");
      const actual = await conn.query(
        query(field(caseDefinition, "query")),
        ...field(caseDefinition, "args", [])
      );
      assert.deepEqual(
        normalize(actual, resultKind),
        normalize(field(caseDefinition, "expect"), resultKind)
      );
      return;
    }

    if (matcher === "error-match") {
      const expectedError = field(caseDefinition, "expect-error");
      let caught = null;
      try {
        await conn.transact(txData(caseTx));
      } catch (error) {
        caught = error;
      }
      assert.notEqual(caught, null, "Expected golden transaction to fail.");
      assert.equal(errorId(caught), field(expectedError, "id").toString());
      const messagePattern = field(expectedError, "message-pattern", null);
      if (messagePattern !== null) {
        assert.match(String(caught.message ?? caught), new RegExp(messagePattern));
      }
      return;
    }

    throw new TypeError(`Unsupported golden matcher: ${matcher}.`);
  } finally {
    await conn.close();
  }
}

const goldenRoot = dtlvtestRoot();

test(
  "JavaScript binding executes the active dtlvtest golden release",
  { skip: !runtimeAvailable || goldenRoot === null, timeout: 60000 },
  async (context) => {
    const cls = await classes();
    const cases = await activeCases(goldenRoot, cls);
    assert.notEqual(cases.length, 0, "The active release must select a golden case.");
    for (const { activeRelease, suiteId, dataset, caseDefinition } of cases) {
      const caseId = field(caseDefinition, "id");
      const label = `${activeRelease}/${suiteId}/${caseId}`;
      await context.test(label, async () => {
        const dbDir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-golden-"));
        await runCase(dbDir, dataset, caseDefinition);
      });
    }
  }
);
