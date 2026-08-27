import { DatalogSymbol, Form, Keyword, RawForm, formData } from "./form.js";

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
    this.form = Object.freeze([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class Clause extends Form {
  constructor(form) {
    super();
    this.form = Object.freeze([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class Binding extends Form {
  constructor(form) {
    super();
    this.form = form;
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class FindSpec extends Form {
  constructor(form) {
    super();
    this.form = Object.freeze([...form]);
    Object.freeze(this);
  }

  toForm() {
    return this.form;
  }
}

export class Order {
  constructor(term, direction) {
    this.term = term;
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
      entries.push(Object.freeze([kw("as"), options.as]));
    }
    if (Object.hasOwn(options, "limit")) {
      entries.push(Object.freeze([kw("limit"), pullLimit(options.limit)]));
    }
    if (Object.hasOwn(options, "default")) {
      entries.push(Object.freeze([kw("default"), options.default]));
    }
    if (Object.hasOwn(options, "xform")) {
      entries.push(Object.freeze([kw("xform"), pullXform(options.xform)]));
    }

    this.attribute = attribute(attributeName);
    this.options = Object.freeze(entries);
    Object.freeze(this);
  }

  toForm() {
    const result = [this.attribute];
    for (const [key, value] of this.options) {
      result.push(key, value);
    }
    return result;
  }

  asData() {
    return formData(this);
  }
}

export class PullNested extends Form {
  constructor(attributeSpec, pattern) {
    super();
    this.attribute = pullAttrSpec(attributeSpec);
    this.pattern = pullNestedPattern(pattern);
    Object.freeze(this);
  }

  toForm() {
    return new Map([[this.attribute, this.pattern]]);
  }

  asData() {
    return formData(this);
  }
}

export class PullSelector extends Form {
  constructor(items = []) {
    super();
    this.items = Object.freeze(Array.from(items, pullSelectorItem));
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
    return [...this.items];
  }

  asData() {
    return formData(this);
  }
}

export class JoinVars extends Form {
  constructor(free = [], { required = [] } = {}) {
    super();
    this.required = Object.freeze([...required]);
    this.free = Object.freeze([...free]);
    Object.freeze(this);
  }

  toForm() {
    return this.required.length > 0
      ? [this.required, ...this.free]
      : [...this.free];
  }
}

export class RuleBranch extends Form {
  constructor(name, variables, clauses) {
    super();
    this.name = sym(name);
    this.variables = normalizeJoinVars(variables);
    this.clauses = Object.freeze([...clauses]);
    if (this.clauses.length === 0) {
      throw new TypeError("A rule branch requires at least one clause.");
    }
    Object.freeze(this);
  }

  toForm() {
    return [
      [this.name, ...this.variables.toForm()],
      ...this.clauses.map(asForm)
    ];
  }
}

export class RuleSet extends Form {
  constructor(branches) {
    super();
    this.branches = Object.freeze([...branches]);
    if (this.branches.length === 0) {
      throw new TypeError("A rule set requires at least one branch.");
    }
    Object.freeze(this);
  }

  toForm() {
    return this.branches.map((branch) => branch.toForm());
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
    this.where = Object.freeze([...where]);
    this.inputs = Object.freeze([...inputs]);
    this.withVars = Object.freeze([...withVars]);
    this.having = Object.freeze([...having]);
    this.orderBy = Object.freeze(orderBy.map((item) => item instanceof Order ? item : asc(item)));
    this.limit = limit;
    this.offset = offset;
    this.timeout = timeout;

    const selected = Object.entries({ keys, strs, syms }).filter(([, names]) => names !== null);
    if (selected.length > 1) {
      throw new TypeError("Only one of keys, strs, or syms may be supplied.");
    }
    if (selected.length === 1) {
      const [mode, names] = selected[0];
      if (names.length === 0) {
        throw new TypeError(`${mode} requires at least one field name.`);
      }
      this.returnMap = Object.freeze([
        kw(mode),
        Object.freeze(Array.from(names, (name) => typeof name === "string" ? sym(name) : name))
      ]);
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
    return result;
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
  return new Clause([[callable(functionName), ...args]]);
}

export function bind(functionName, bindingValue, ...args) {
  return new Clause([[callable(functionName), ...args], asForm(bindingValue)]);
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
  const result = sourceValue === null || sourceValue === undefined ? [] : [sourceValue];
  result.push(sym(operator), ...values.map(asForm));
  return new Clause(result);
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
  call,
  collection,
  collectionBinding,
  customAggregate,
  datom,
  desc,
  expression,
  ignoreBinding,
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
  v,
  var: variable
});
