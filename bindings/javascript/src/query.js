import {
  DatalogSymbol,
  Form,
  Keyword,
  RawForm,
  ednList,
  formData,
  immutableSnapshot,
  quote
} from "./form.js";
import { udfReference } from "./udf-value.js";

export function kw(name) {
  return name instanceof Keyword ? name : new Keyword(name);
}

export function sym(name) {
  return name instanceof DatalogSymbol ? name : new DatalogSymbol(name);
}

export function variable(name) {
  if (name instanceof DatalogSymbol) {
    if (!name.name.startsWith("?")) {
      throw new TypeError(`Expected a Datalog variable, got ${name}.`);
    }
    return name;
  }
  const text = String(name);
  return sym(text.startsWith("?") ? text : `?${text}`);
}

export const v = variable;

export function source(name = "$") {
  if (name instanceof DatalogSymbol) {
    if (!name.name.startsWith("$")) {
      throw new TypeError(`Expected a Datalog source, got ${name}.`);
    }
    return name;
  }
  const text = String(name);
  return sym(text.startsWith("$") ? text : `$${text}`);
}

export const DB = sym("$");
export const RULES = sym("%");
export const IGNORE = sym("_");
const ELLIPSIS = sym("...");
const DOT = sym(".");

function asForm(value) {
  return value instanceof Form ? value.toForm() : value;
}

function attribute(value) {
  return typeof value === "string" ? kw(value) : value;
}

function callable(value) {
  return typeof value === "string" ? sym(value) : value;
}

function isVariableSymbol(value) {
  return value instanceof DatalogSymbol && value.name.startsWith("?");
}

function isOrderIndex(value) {
  return (
    (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    || (typeof value === "bigint" && value >= 0n)
  );
}

function plainObject(value) {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

export class Expression extends Form {
  constructor(form) {
    super();
    this.form = immutableSnapshot([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class Clause extends Form {
  constructor(form, kind = "clause") {
    super();
    this.form = immutableSnapshot([...form]);
    this.kind = kind;
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

function clauseOperator(value) {
  return value instanceof Clause ? value.kind : null;
}

function rejectAndClauses(values, context) {
  if (values.some((value) => clauseOperator(value) === "and")) {
    throw new TypeError(
      `q.and() is only valid as a branch of q.or() or q.orJoin(), not ${context}.`
    );
  }
}

export class Binding extends Form {
  constructor(form) {
    super();
    this.form = immutableSnapshot(form);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

class QueryOptions extends Form {
  constructor(entries) {
    super();
    this.data = immutableSnapshot(new Map(entries));
    Object.freeze(this);
  }

  get size() {
    return this.data.size;
  }

  get(key) {
    return this.data.get(key);
  }

  has(key) {
    return this.data.has(key);
  }

  entries() {
    return this.data.entries();
  }

  [Symbol.iterator]() {
    return this.data[Symbol.iterator]();
  }

  toForm() {
    return this.data;
  }

  asData() {
    return formData(this);
  }
}

export class FulltextOptions extends QueryOptions {}
export class VectorSearchOptions extends QueryOptions {}
export class IdocMatchOptions extends QueryOptions {}

const FULLTEXT_DISPLAYS = new Map([
  ["refs", 3],
  ["refs+scores", 4],
  ["texts", 4],
  ["offsets", 4],
  ["texts+offsets", 5]
]);
const VECTOR_DISPLAYS = new Map([
  ["refs", 3],
  ["refs+dists", 4]
]);

function optionName(value) {
  if (value instanceof Keyword) {
    return value.name;
  }
  if (typeof value !== "string") {
    throw new TypeError("Query option keys must be keywords or strings.");
  }
  return kw(value).name;
}

function nonnegativeOption(name, value) {
  const valid = (
    (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    || (typeof value === "bigint" && value >= 0n)
  );
  if (!valid) {
    throw new TypeError(`${name} must be a non-negative integer.`);
  }
  return value;
}

function domainOption(value) {
  if (typeof value === "string" || value === null || !value?.[Symbol.iterator]) {
    throw new TypeError("Search domains must be a sequence of strings.");
  }
  const domains = [...value];
  if (domains.length === 0) {
    throw new TypeError("Search domains must not be empty.");
  }
  if (!domains.every((domain) => typeof domain === "string" && domain.length > 0)) {
    throw new TypeError("Search domains must contain non-empty strings.");
  }
  return domains;
}

function displayOption(value, supported) {
  const display = typeof value === "string" ? kw(value) : value;
  if (!(display instanceof Keyword) || !supported.has(display.name)) {
    const expected = [...supported.keys()].map((name) => `:${name}`).join(", ");
    throw new TypeError(`Unsupported search display; expected one of ${expected}.`);
  }
  return display;
}

function extraOptionEntries(extra) {
  if (extra === null || extra === undefined) {
    return [];
  }
  if (extra instanceof Map) {
    return [...extra.entries()];
  }
  if (plainObject(extra)) {
    return Object.entries(extra);
  }
  throw new TypeError("extra query options must be a Map or plain object.");
}

function queryOptions(
  OptionType,
  values,
  { extra = null, displays = null, nonnegative = new Set() } = {}
) {
  const raw = new Map();
  for (const [name, value] of values) {
    if (value !== null && value !== undefined) {
      raw.set(name, value);
    }
  }
  for (const [key, value] of extraOptionEntries(extra)) {
    raw.set(optionName(key), value);
  }

  const entries = [];
  for (const [name, original] of raw) {
    let value = original;
    if (name === "domains") {
      value = domainOption(value);
    } else if (name === "display" && displays !== null) {
      value = displayOption(value, displays);
    } else if (nonnegative.has(name)) {
      value = nonnegativeOption(name, value);
    }
    entries.push([kw(name), immutableSnapshot(value)]);
  }
  return new OptionType(entries);
}

export function fulltextOptions({
  top = null,
  limit = null,
  offset = null,
  pagingCachePages = null,
  display = null,
  domains = null,
  proximityExpansion = null,
  proximityMaxDist = null,
  docFilter = null,
  extra = null
} = {}) {
  return queryOptions(
    FulltextOptions,
    [
      ["top", top],
      ["limit", limit],
      ["offset", offset],
      ["paging-cache-pages", pagingCachePages],
      ["display", display],
      ["domains", domains],
      ["proximity-expansion", proximityExpansion],
      ["proximity-max-dist", proximityMaxDist],
      ["doc-filter", docFilter]
    ],
    {
      extra,
      displays: FULLTEXT_DISPLAYS,
      nonnegative: new Set(["top", "limit", "offset", "paging-cache-pages"])
    }
  );
}

export function vectorSearchOptions({
  top = null,
  display = null,
  domains = null,
  vecFilter = null,
  extra = null
} = {}) {
  return queryOptions(
    VectorSearchOptions,
    [
      ["top", top],
      ["display", display],
      ["domains", domains],
      ["vec-filter", vecFilter]
    ],
    {
      extra,
      displays: VECTOR_DISPLAYS,
      nonnegative: new Set(["top"])
    }
  );
}

export function idocMatchOptions({ domains = null, extra = null } = {}) {
  return queryOptions(
    IdocMatchOptions,
    [["domains", domains]],
    { extra }
  );
}

export class FindSpec extends Form {
  constructor(form) {
    super();
    this.form = immutableSnapshot([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

function findSpecDetails(find) {
  const form = find.form;
  if (form.length === 2 && form.at(-1) === DOT) {
    return { shape: "scalar", elements: [form[0]] };
  }
  if (form.length > 2 && form.at(-1) === DOT) {
    return { shape: "tuple", elements: form.slice(0, -1) };
  }
  if (form.length === 1 && Array.isArray(form[0])) {
    const nested = form[0];
    if (nested.length === 2 && nested.at(-1) === ELLIPSIS) {
      return { shape: "collection", elements: [nested[0]] };
    }
    return { shape: "tuple", elements: nested };
  }
  return { shape: "relation", elements: form };
}

function findElementVariables(value) {
  if (isVariableSymbol(value)) {
    return [value];
  }
  if (!(value instanceof Expression) || value.form.length === 0) {
    return [];
  }

  const operator = value.form[0];
  const name = operator instanceof DatalogSymbol ? operator.name : null;
  if (name === "pull") {
    const variableIndex = value.form.length === 4 ? 2 : 1;
    const variableValue = value.form[variableIndex];
    return isVariableSymbol(variableValue) ? [variableValue] : [];
  }
  if (["+", "-", "*", "/", "mod", "rem", "quot"].includes(name)) {
    return value.form.slice(1).flatMap(findElementVariables);
  }

  const args = name === "aggregate" ? value.form.slice(2) : value.form.slice(1);
  return args.length > 0 && isVariableSymbol(args.at(-1)) ? [args.at(-1)] : [];
}

function findVariables(find) {
  return new Set(findSpecDetails(find).elements.flatMap(findElementVariables));
}

export class Order {
  constructor(term, direction) {
    this.term = immutableSnapshot(term);
    if (!isVariableSymbol(this.term) && !isOrderIndex(this.term)) {
      throw new TypeError("An order term must be a query variable or non-negative index.");
    }
    if (!(direction instanceof Keyword) || !["asc", "desc"].includes(direction.name)) {
      throw new TypeError("An order direction must be :asc or :desc.");
    }
    this.direction = direction;
    Object.freeze(this);
  }
}

export class PullAttr extends Form {
  constructor(attributeName, options = {}) {
    super();
    if (!plainObject(options)) {
      throw new TypeError("Pull attribute options must be an object.");
    }
    const allowed = new Set(["as", "limit", "default", "xform"]);
    for (const key of Object.keys(options)) {
      if (!allowed.has(key)) {
        throw new TypeError(`Unknown pull attribute option: ${key}.`);
      }
    }

    const entries = [];
    if (Object.hasOwn(options, "as")) {
      entries.push([kw("as"), options.as]);
    }
    if (Object.hasOwn(options, "limit")) {
      entries.push([kw("limit"), pullLimit(options.limit)]);
    }
    if (Object.hasOwn(options, "default")) {
      entries.push([kw("default"), options.default]);
    }
    if (Object.hasOwn(options, "xform")) {
      entries.push([kw("xform"), pullXform(options.xform)]);
    }

    this.attribute = immutableSnapshot(attribute(attributeName));
    this.options = immutableSnapshot(entries);
    Object.freeze(this);
  }

  toForm() {
    const result = [this.attribute];
    for (const [key, value] of this.options) {
      result.push(key, value);
    }
    return immutableSnapshot(result);
  }

  asData() {
    return formData(this);
  }
}

export class PullNested extends Form {
  constructor(attributeSpec, pattern) {
    super();
    this.attribute = immutableSnapshot(pullAttrSpec(attributeSpec));
    this.pattern = immutableSnapshot(pullNestedPattern(pattern));
    Object.freeze(this);
  }

  toForm() {
    return immutableSnapshot(new Map([[this.attribute, this.pattern]]));
  }

  asData() {
    return formData(this);
  }
}

export class PullSelector extends Form {
  constructor(items = []) {
    super();
    this.items = immutableSnapshot(Array.from(items, pullSelectorItem));
    Object.freeze(this);
  }

  get length() {
    return this.items.length;
  }

  at(index) {
    return this.items.at(index);
  }

  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }

  toForm() {
    return this.items;
  }

  asData() {
    return formData(this);
  }
}

export class JoinVars extends Form {
  constructor(free = [], { required = [] } = {}) {
    super();
    if (!Array.isArray(free) || !Array.isArray(required)) {
      throw new TypeError("Required and free join variables must be arrays.");
    }
    this.required = immutableSnapshot([...required]);
    this.free = immutableSnapshot([...free]);
    const variables = [...this.required, ...this.free];
    if (variables.length === 0) {
      throw new TypeError("Join variables must not be empty.");
    }
    if (!variables.every(isVariableSymbol)) {
      throw new TypeError("Join variables must be values created by q.var().");
    }
    if (new Set(variables.map((variableValue) => variableValue.name)).size !== variables.length) {
      throw new TypeError("Join variables must be distinct.");
    }
    Object.freeze(this);
  }

  toForm() {
    return immutableSnapshot(this.required.length > 0
      ? [this.required, ...this.free]
      : [...this.free]);
  }
}

export class RuleBranch extends Form {
  constructor(name, variables, clauses) {
    super();
    this.name = sym(name);
    this.variables = normalizeJoinVars(variables);
    this.clauses = immutableSnapshot([...clauses]);
    if (
      this.name.name.startsWith("?")
      || this.name.name.startsWith("$")
      || this.name.name === "%"
      || this.name.name === "_"
    ) {
      throw new TypeError("A rule name must be a plain Datalog symbol.");
    }
    if (this.clauses.length === 0) {
      throw new TypeError("A rule branch requires at least one clause.");
    }
    rejectAndClauses(this.clauses, "at the top level of a rule branch");
    Object.freeze(this);
  }

  toForm() {
    return immutableSnapshot([
      [this.name, ...this.variables.toForm()],
      ...this.clauses.map(asForm)
    ]);
  }
}

export class RuleSet extends Form {
  constructor(branches) {
    super();
    this.branches = immutableSnapshot([...branches]);
    if (this.branches.length === 0) {
      throw new TypeError("A rule set requires at least one branch.");
    }
    if (!this.branches.every((branch) => branch instanceof RuleBranch)) {
      throw new TypeError("A rule set accepts only q.ruleBranch() values.");
    }

    const arities = new Map();
    for (const branch of this.branches) {
      const arity = [branch.variables.required.length, branch.variables.free.length];
      const previous = arities.get(branch.name.name);
      if (previous !== undefined && (previous[0] !== arity[0] || previous[1] !== arity[1])) {
        throw new TypeError(
          `Rule branches named ${branch.name} must have matching required/free arity.`
        );
      }
      arities.set(branch.name.name, arity);
    }
    Object.freeze(this);
  }

  toForm() {
    return immutableSnapshot(this.branches.map((branch) => branch.toForm()));
  }

  asData() {
    return formData(this);
  }
}

function normalizeReturnMap(find, mode, values) {
  if (typeof values === "string" || values?.[Symbol.iterator] === undefined) {
    throw new TypeError(`${mode} must be an iterable of field names.`);
  }
  const names = Array.from(
    values,
    (name) => typeof name === "string" ? sym(name) : name
  );
  if (names.length === 0) {
    throw new TypeError(`${mode} requires at least one field name.`);
  }
  if (!names.every((name) => name instanceof DatalogSymbol)) {
    throw new TypeError(`${mode} field names must be symbols or strings.`);
  }

  const { shape, elements } = findSpecDetails(find);
  if (shape === "scalar" || shape === "collection") {
    throw new TypeError(`${mode} does not work with a ${shape} find specification.`);
  }
  if (names.length !== elements.length) {
    throw new TypeError(
      `${mode} field count must match the ${elements.length} find elements.`
    );
  }
  return immutableSnapshot([kw(mode), names]);
}

function validateQueryOrder(find, ordering) {
  const variables = findVariables(find);
  const { elements } = findSpecDetails(find);
  const seen = new Set();
  for (const item of ordering) {
    const term = item.term;
    const key = isOrderIndex(term) ? `index:${term}` : `variable:${term.name}`;
    if (seen.has(key)) {
      throw new TypeError("Order terms must be distinct.");
    }
    seen.add(key);
    if (isOrderIndex(term)) {
      if (
        (typeof term === "bigint" && term >= BigInt(elements.length))
        || (typeof term === "number" && term >= elements.length)
      ) {
        throw new TypeError("Order column index is outside the find specification.");
      }
    } else if (!variables.has(term)) {
      throw new TypeError("An order variable must occur in the find specification.");
    }
  }
}

export class Query extends Form {
  constructor({
    find,
    where = [],
    inputs = [],
    with: withVars = [],
    having = [],
    orderBy = [],
    limit = null,
    offset = null,
    timeout = null,
    keys = null,
    strs = null,
    syms = null
  } = {}) {
    super();
    if (find === undefined || find === null) {
      throw new TypeError("query() requires a find specification.");
    }
    this.find = find instanceof FindSpec ? find : relation(...findValues(find));
    this.where = immutableSnapshot([...where]);
    this.inputs = immutableSnapshot([...inputs]);
    this.withVars = immutableSnapshot([...withVars]);
    this.having = immutableSnapshot([...having]);
    this.orderBy = immutableSnapshot(
      orderBy.map((item) => item instanceof Order ? item : asc(item))
    );
    rejectAndClauses(this.where, "at the top level of :where");
    rejectAndClauses(this.having, "at the top level of :having");
    validateQueryOrder(this.find, this.orderBy);
    this.limit = limit;
    this.offset = offset;
    this.timeout = timeout;

    const selected = Object.entries({ keys, strs, syms }).filter(([, names]) => names !== null);
    if (selected.length > 1) {
      throw new TypeError("Only one of keys, strs, or syms may be supplied.");
    }
    if (selected.length === 1) {
      const [mode, names] = selected[0];
      this.returnMap = normalizeReturnMap(this.find, mode, names);
    } else {
      this.returnMap = null;
    }
    Object.freeze(this);
  }

  toForm() {
    const result = [kw("find"), ...this.find.toForm()];
    if (this.returnMap !== null) {
      result.push(this.returnMap[0], ...this.returnMap[1]);
    }
    if (this.withVars.length > 0) {
      result.push(kw("with"), ...this.withVars);
    }
    if (this.inputs.length > 0) {
      result.push(kw("in"), ...this.inputs);
    }
    if (this.where.length > 0) {
      result.push(kw("where"), ...this.where.map(asForm));
    }
    if (this.having.length > 0) {
      result.push(kw("having"), ...this.having.map(asForm));
    }
    if (this.timeout !== null && this.timeout !== undefined) {
      result.push(kw("timeout"), this.timeout);
    }
    if (this.orderBy.length > 0) {
      const ordering = [];
      for (const item of this.orderBy) {
        ordering.push(item.term, item.direction);
      }
      result.push(kw("order-by"), ordering);
    }
    if (this.offset !== null && this.offset !== undefined) {
      result.push(kw("offset"), this.offset);
    }
    if (this.limit !== null && this.limit !== undefined) {
      result.push(kw("limit"), this.limit);
    }
    return immutableSnapshot(result);
  }

  asData() {
    return formData(this);
  }
}

function findValues(value) {
  if (
    typeof value === "string"
    || value instanceof Keyword
    || value instanceof DatalogSymbol
    || value instanceof Expression
    || value instanceof RawForm
    || !value?.[Symbol.iterator]
  ) {
    return [value];
  }
  return [...value];
}

export function query(options) {
  return new Query(options);
}

export function relation(...expressions) {
  return new FindSpec(expressions);
}

export function collection(expression) {
  return new FindSpec([[expression, ELLIPSIS]]);
}

export function tupleFind(...expressions) {
  return new FindSpec([expressions]);
}

export function scalar(expression) {
  return new FindSpec([expression, DOT]);
}

export function tupleScalar(...expressions) {
  return new FindSpec([...expressions, DOT]);
}

export function call(functionName, ...args) {
  return new Expression([callable(functionName), ...args]);
}

export function udf(reference, ...args) {
  return call("udf", udfReference(reference), ...args);
}

export function aggregate(functionName, ...args) {
  return call(functionName, ...args);
}

export function customAggregate(functionVariable, ...args) {
  return new Expression([sym("aggregate"), functionVariable, ...args]);
}

export function expression(operator, ...args) {
  return call(operator, ...args);
}

function pullLimit(value) {
  const validNumber = typeof value === "number" && Number.isInteger(value) && value > 0;
  const validBigInt = typeof value === "bigint" && value > 0n;
  if (value !== null && !validNumber && !validBigInt) {
    throw new TypeError("A pull limit must be a positive integer or null.");
  }
  return value;
}

function pullXform(value) {
  if (typeof value === "string") {
    return sym(value);
  }
  if (!(value instanceof DatalogSymbol)) {
    throw new TypeError("A pull xform must be a symbol or symbol name.");
  }
  return value;
}

function pullTokenName(value) {
  if (value instanceof Keyword || value instanceof DatalogSymbol) {
    return value.name;
  }
  if (typeof value === "string") {
    return value.startsWith(":") ? value.slice(1) : value;
  }
  return null;
}

function pullAttrExpression(values) {
  if (values.length === 0) {
    throw new TypeError("A pull attribute expression must not be empty.");
  }

  const legacyOp = pullTokenName(values[0]);
  if ((legacyOp === "default" || legacyOp === "limit") && values.length === 3) {
    const value = legacyOp === "limit" ? pullLimit(values[2]) : values[2];
    return [sym(legacyOp), pullAttrSpec(values[1]), value];
  }

  if (values.length % 2 === 0) {
    throw new TypeError("A pull attribute expression requires option/value pairs.");
  }

  const result = [pullAttrSpec(values[0])];
  for (let index = 1; index < values.length; index += 2) {
    const optionName = pullTokenName(values[index]);
    if (!["as", "limit", "default", "xform"].includes(optionName)) {
      throw new TypeError(`Unknown pull attribute option: ${String(values[index])}.`);
    }
    let value = values[index + 1];
    if (optionName === "limit") {
      value = pullLimit(value);
    } else if (optionName === "xform") {
      value = pullXform(value);
    }
    result.push(kw(optionName), value);
  }
  return result;
}

function pullAttrSpec(value) {
  if (typeof value === "string") {
    return kw(value);
  }
  if (Array.isArray(value)) {
    return pullAttrExpression(value);
  }
  return value;
}

function pullNestedPattern(value) {
  if (typeof value === "string") {
    return value === "..." ? ELLIPSIS : new PullSelector([value]);
  }
  if (value instanceof DatalogSymbol && value.name === "...") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    const validNumber = typeof value === "number" && Number.isInteger(value) && value > 0;
    const validBigInt = typeof value === "bigint" && value > 0n;
    if (!validNumber && !validBigInt) {
      throw new TypeError("A pull recursion depth must be a positive integer.");
    }
    return value;
  }
  if (value instanceof PullSelector || value instanceof RawForm) {
    return value;
  }
  if (Array.isArray(value)) {
    return new PullSelector(value);
  }
  if (value instanceof Map || plainObject(value)) {
    return new PullSelector([value]);
  }
  return value;
}

function pullSelectorItem(item) {
  if (typeof item === "string") {
    return item === "*" ? sym("*") : kw(item);
  }
  if (item instanceof Map) {
    return new Map(Array.from(
      item,
      ([key, value]) => [pullAttrSpec(key), pullNestedPattern(value)]
    ));
  }
  if (plainObject(item)) {
    return new Map(Object.entries(item).map(
      ([key, value]) => [pullAttrSpec(key), pullNestedPattern(value)]
    ));
  }
  if (Array.isArray(item)) {
    return pullAttrExpression(item);
  }
  return item;
}

function pullPattern(pattern) {
  if (
    pattern instanceof PullSelector
    || pattern instanceof RawForm
    || pattern instanceof DatalogSymbol
  ) {
    return pattern;
  }
  if (typeof pattern === "string") {
    return new PullSelector([pattern]);
  }
  if (Array.isArray(pattern)) {
    return new PullSelector(pattern);
  }
  if (pattern instanceof Map || plainObject(pattern)) {
    return new PullSelector([pattern]);
  }
  return pattern;
}

export function selector(...attributes) {
  if (attributes.length === 1 && attributes[0] instanceof PullSelector) {
    return attributes[0];
  }
  if (attributes.length === 1 && Array.isArray(attributes[0])) {
    attributes = attributes[0];
  }
  return new PullSelector(attributes);
}

export function pullAttr(attributeName, options = {}) {
  return new PullAttr(attributeName, options);
}

export function pullNested(attributeSpec, pattern) {
  return new PullNested(attributeSpec, pattern);
}

export function pullRecursive(attributeSpec, depth = null) {
  if (depth === null || depth === undefined) {
    return new PullNested(attributeSpec, ELLIPSIS);
  }
  const validNumber = typeof depth === "number" && Number.isInteger(depth) && depth > 0;
  const validBigInt = typeof depth === "bigint" && depth > 0n;
  if (!validNumber && !validBigInt) {
    throw new TypeError("A pull recursion depth must be a positive integer or null.");
  }
  return new PullNested(attributeSpec, depth);
}

export function pull(variableValue, pattern, { source: sourceValue = null } = {}) {
  const items = [sym("pull")];
  if (sourceValue !== null && sourceValue !== undefined) {
    items.push(sourceValue);
  }
  items.push(variableValue, pullPattern(pattern));
  return new Expression(items);
}

function databasePattern(sourceValue, terms) {
  if (terms.length === 0) {
    throw new TypeError("A database pattern requires at least one term.");
  }
  const items = sourceValue === null || sourceValue === undefined ? [] : [sourceValue];
  const normalized = [...terms];
  if (normalized.length >= 2) {
    normalized[1] = attribute(normalized[1]);
  }
  items.push(...normalized);
  return new Clause(items);
}

export function pattern(...terms) {
  return databasePattern(null, terms);
}

export function patternFrom(sourceValue, ...terms) {
  return databasePattern(sourceValue, terms);
}

export function datom(entity, attributeName, value, { source: sourceValue = null } = {}) {
  return databasePattern(sourceValue, [entity, attributeName, value]);
}

export function predicate(functionName, ...args) {
  if (functionName instanceof Expression) {
    if (args.length > 0) {
      throw new TypeError("An expression predicate does not accept extra arguments.");
    }
    return new Clause([asForm(functionName)]);
  }
  return new Clause([[callable(functionName), ...args]]);
}

export function bind(functionName, bindingValue, ...args) {
  if (functionName instanceof Expression) {
    if (args.length > 0) {
      throw new TypeError("An expression binding does not accept extra arguments.");
    }
    return new Clause([asForm(functionName), asForm(bindingValue)]);
  }
  return new Clause([[callable(functionName), ...args], asForm(bindingValue)]);
}

function searchSource(value) {
  if (!(value instanceof DatalogSymbol) || !value.name.startsWith("$")) {
    throw new TypeError("A search source must be a value created by q.source().");
  }
  return value;
}

function searchAttribute(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = attribute(value);
  if (!(normalized instanceof Keyword)) {
    throw new TypeError("A search attribute must be a keyword or attribute name.");
  }
  return normalized;
}

function searchOptionsValue(value, OptionType, builder) {
  if (value === null || value === undefined || value instanceof OptionType) {
    return { value: value ?? null, static: true };
  }
  if (value instanceof QueryOptions) {
    throw new TypeError(`Expected ${OptionType.name}, got ${value.constructor.name}.`);
  }
  if (value instanceof Map || plainObject(value)) {
    return { value: builder({ extra: value }), static: true };
  }
  if (isVariableSymbol(value) || value instanceof RawForm) {
    return { value, static: false };
  }
  throw new TypeError(
    `Search options must be ${OptionType.name}, a Map/object, or a query variable.`
  );
}

function relationResultBinding(results, allowedArities, context) {
  let bindingValue;
  if (results instanceof Binding) {
    bindingValue = results;
  } else {
    if (typeof results === "string" || !results?.[Symbol.iterator]) {
      throw new TypeError(`${context} results must be a sequence or relation binding.`);
    }
    bindingValue = relationBinding(...results);
  }

  const form = bindingValue.toForm();
  if (!(Array.isArray(form) && form.length === 1 && Array.isArray(form[0]))) {
    throw new TypeError(`${context} requires a relation result binding.`);
  }
  const items = form[0];
  if (!items.every((item) => isVariableSymbol(item) || item === IGNORE)) {
    throw new TypeError(`${context} result items must be q.var() or q.IGNORE values.`);
  }
  if (!allowedArities.has(items.length)) {
    const expected = [...allowedArities].sort().join(", ");
    throw new TypeError(`${context} requires ${expected} result items.`);
  }
  return bindingValue;
}

function staticDisplayArity(options, displays, defaultArity = 3) {
  if (options === null) {
    return new Set([defaultArity]);
  }
  const display = options.get(kw("display"));
  return new Set([display === undefined ? defaultArity : displays.get(display.name)]);
}

export function fulltext(
  queryValue,
  results,
  { attribute: attributeValue = null, options = null, source: sourceValue = DB } = {}
) {
  const normalizedSource = searchSource(sourceValue);
  const normalizedAttribute = searchAttribute(attributeValue);
  const normalizedOptions = searchOptionsValue(options, FulltextOptions, fulltextOptions);
  const arities = normalizedOptions.static
    ? staticDisplayArity(normalizedOptions.value, FULLTEXT_DISPLAYS)
    : new Set(FULLTEXT_DISPLAYS.values());
  const bindingValue = relationResultBinding(results, arities, "q.fulltext");
  const args = [normalizedSource];
  if (normalizedAttribute !== null) {
    args.push(normalizedAttribute);
  }
  args.push(queryValue);
  if (normalizedOptions.value !== null) {
    args.push(normalizedOptions.value);
  }
  return bind("fulltext", bindingValue, ...args);
}

export function vecNeighbors(
  queryVector,
  results,
  { attribute: attributeValue = null, options = null, source: sourceValue = DB } = {}
) {
  const normalizedSource = searchSource(sourceValue);
  const normalizedAttribute = searchAttribute(attributeValue);
  const normalizedOptions = searchOptionsValue(
    options, VectorSearchOptions, vectorSearchOptions
  );
  if (normalizedAttribute === null && normalizedOptions.static) {
    const domains = normalizedOptions.value?.get(kw("domains"));
    if (domains === undefined || domains.length === 0) {
      throw new TypeError(
        "q.vecNeighbors requires an attribute or vector search domains."
      );
    }
  }
  const arities = normalizedOptions.static
    ? staticDisplayArity(normalizedOptions.value, VECTOR_DISPLAYS)
    : new Set(VECTOR_DISPLAYS.values());
  const bindingValue = relationResultBinding(results, arities, "q.vecNeighbors");
  const args = [normalizedSource];
  if (normalizedAttribute !== null) {
    args.push(normalizedAttribute);
  }
  args.push(queryVector);
  if (normalizedOptions.value !== null) {
    args.push(normalizedOptions.value);
  }
  return bind("vec-neighbors", bindingValue, ...args);
}

export function idocMatch(
  queryValue,
  results,
  { attribute: attributeValue = null, options = null, source: sourceValue = DB } = {}
) {
  const normalizedSource = searchSource(sourceValue);
  const normalizedAttribute = searchAttribute(attributeValue);
  const normalizedOptions = searchOptionsValue(
    options, IdocMatchOptions, idocMatchOptions
  );
  const bindingValue = relationResultBinding(results, new Set([3]), "q.idocMatch");
  const args = [normalizedSource];
  if (normalizedAttribute !== null) {
    args.push(normalizedAttribute);
  }
  args.push(queryValue);
  if (normalizedOptions.value !== null) {
    args.push(normalizedOptions.value);
  }
  return bind("idoc-match", bindingValue, ...args);
}

export function bindUdf(reference, bindingValue, ...args) {
  return bind(udf(reference, ...args), bindingValue);
}

export function udfPredicate(reference, ...args) {
  return predicate(udf(reference, ...args));
}

export function rule(name, ...args) {
  return new Clause([callable(name), ...args]);
}

export function ruleFrom(sourceValue, name, ...args) {
  return new Clause([sourceValue, callable(name), ...args]);
}

function clauses(operator, values, sourceValue = null) {
  if (values.length === 0) {
    throw new TypeError(`${operator} requires at least one clause.`);
  }
  if (operator === "and" || operator === "not") {
    rejectAndClauses(values, `inside q.${operator}()`);
  }
  const result = sourceValue === null || sourceValue === undefined ? [] : [sourceValue];
  result.push(sym(operator), ...values.map(asForm));
  return new Clause(result, operator);
}

export function andClause(...values) {
  return clauses("and", values);
}

export function orClause(...values) {
  return clauses("or", values);
}

export function orFrom(sourceValue, ...values) {
  return clauses("or", values, sourceValue);
}

export function notClause(...values) {
  return clauses("not", values);
}

export function notFrom(sourceValue, ...values) {
  return clauses("not", values, sourceValue);
}

export function joinVars(...free) {
  let options = {};
  if (free.length > 0 && plainObject(free.at(-1)) && Object.hasOwn(free.at(-1), "required")) {
    options = free.pop();
  }
  return new JoinVars(free, options);
}

function normalizeJoinVars(value) {
  if (value instanceof JoinVars) {
    return value;
  }
  if (!Array.isArray(value)) {
    throw new TypeError("Join variables must be a JoinVars value or an array.");
  }
  return new JoinVars(value);
}

function joinClause(operator, variables, values, sourceValue = null) {
  if (values.length === 0) {
    throw new TypeError(`${operator} requires at least one clause.`);
  }
  const normalized = normalizeJoinVars(variables);
  if (operator === "not-join") {
    rejectAndClauses(values, "inside q.notJoin()");
  }
  if (operator === "not-join" && normalized.required.length > 0) {
    throw new TypeError("not-join does not support required join variables.");
  }
  const result = sourceValue === null || sourceValue === undefined ? [] : [sourceValue];
  result.push(sym(operator), normalized.toForm(), ...values.map(asForm));
  return new Clause(result);
}

export function orJoin(variables, ...values) {
  return joinClause("or-join", variables, values);
}

export function orJoinFrom(sourceValue, variables, ...values) {
  return joinClause("or-join", variables, values, sourceValue);
}

export function notJoin(variables, ...values) {
  return joinClause("not-join", variables, values);
}

export function notJoinFrom(sourceValue, variables, ...values) {
  return joinClause("not-join", variables, values, sourceValue);
}

export function tupleBinding(...bindings) {
  if (bindings.length === 0) {
    throw new TypeError("A tuple binding requires at least one item.");
  }
  return new Binding(bindings.map(asForm));
}

export function collectionBinding(bindingValue) {
  return new Binding([asForm(bindingValue), ELLIPSIS]);
}

export function relationBinding(...bindings) {
  return new Binding([bindings.map(asForm)]);
}

export function ignoreBinding() {
  return new Binding(IGNORE);
}

export function asc(term) {
  return new Order(term, kw("asc"));
}

export function desc(term) {
  return new Order(term, kw("desc"));
}

export function ruleBranch(name, variables, ...ruleClauses) {
  return new RuleBranch(name, variables, ruleClauses);
}

export function rules(...branches) {
  return new RuleSet(branches);
}

export function raw(form) {
  return new RawForm(form);
}

export const q = Object.freeze({
  DB,
  IGNORE,
  RULES,
  aggregate,
  and: andClause,
  asc,
  bind,
  bindUdf,
  call,
  collection,
  collectionBinding,
  customAggregate,
  datom,
  desc,
  ednList,
  expression,
  fulltext,
  fulltextOptions,
  ignoreBinding,
  idocMatch,
  idocMatchOptions,
  joinVars,
  kw,
  not: notClause,
  notFrom,
  notJoin,
  notJoinFrom,
  or: orClause,
  orFrom,
  orJoin,
  orJoinFrom,
  pattern,
  patternFrom,
  predicate,
  pull,
  pullAttr,
  pullNested,
  pullRecursive,
  query,
  quote,
  raw,
  relation,
  relationBinding,
  rule,
  ruleBranch,
  ruleFrom,
  rules,
  scalar,
  selector,
  source,
  sym,
  tuple: tupleFind,
  tupleBinding,
  tupleScalar,
  udf,
  udfPredicate,
  v,
  var: variable,
  vecNeighbors,
  vectorSearchOptions
});
