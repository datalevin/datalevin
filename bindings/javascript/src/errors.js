export class DatalevinError extends Error {
  constructor(message, { typeName = null, data = null, cause = null } = {}) {
    super(message, cause === null ? undefined : { cause });
    this.name = new.target.name;
    this.typeName = typeName;
    this.data = data;
    if (cause !== null && this.cause === undefined) {
      this.cause = cause;
    }
  }
}

export class DatalevinConfigurationError extends DatalevinError {}

export class DatalevinJvmError extends DatalevinError {}

export class DatalevinJavaError extends DatalevinError {
  constructor(message, { javaClass = null, typeName = null, data = null, cause = null } = {}) {
    super(message, { typeName, data, cause });
    this.javaClass = javaClass;
  }
}
