function tokenText(value, prefix) {
  if (typeof value !== "string") {
    throw new TypeError(`Datalevin token names must be strings, got ${typeof value}.`);
  }
  const text = prefix && value.startsWith(prefix) ? value.slice(prefix.length) : value;
  if (text.length === 0) {
    throw new TypeError("Datalevin token names must not be empty.");
  }
  if (/\s/.test(text)) {
    throw new TypeError(`Datalevin token names must not contain whitespace: ${JSON.stringify(value)}.`);
  }
  return text;
}

export class Keyword {
  constructor(name) {
    this.name = tokenText(name, ":");
    Object.freeze(this);
  }

  toString() {
    return `:${this.name}`;
  }
}

export class DatalogSymbol {
  constructor(name) {
    this.name = tokenText(name, "");
    Object.freeze(this);
  }

  toString() {
    return this.name;
  }
}

export class Form {
  toForm() {
    throw new TypeError(`${this.constructor.name}.toForm() is not implemented.`);
  }
}

export class RawForm extends Form {
  constructor(value) {
    super();
    this.value = value;
    Object.freeze(this);
  }

  toForm() {
    return this.value;
  }
}

export function formData(value) {
  if (value instanceof Form) {
    return formData(value.toForm());
  }
  if (value instanceof Keyword || value instanceof DatalogSymbol) {
    return value.toString();
  }
  if (value instanceof Map) {
    return new Map(Array.from(value, ([key, item]) => [formData(key), formData(item)]));
  }
  if (value instanceof Set) {
    return new Set(Array.from(value, formData));
  }
  if (Array.isArray(value)) {
    return value.map(formData);
  }
  if (value !== null && typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype === Object.prototype || prototype === null) {
      return Object.fromEntries(
        Object.entries(value).map(([key, item]) => [key, formData(item)])
      );
    }
  }
  return value;
}
