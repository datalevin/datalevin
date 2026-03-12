import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  DatalevinConfigurationError,
  DatalevinJavaError,
  DatalevinJvmError
} from "./errors.js";

export const DATALEVIN_JAR_ENV = "DATALEVIN_JAR";
export const DATALEVIN_CLASSPATH_ENV = "DATALEVIN_CLASSPATH";
export const DATALEVIN_JVM_ARGS_ENV = "DATALEVIN_JVM_ARGS";
export const DATALEVIN_JAVACPP_CACHEDIR_ENV = "DATALEVIN_JAVACPP_CACHEDIR";
export const DEFAULT_JAVACPP_CACHEDIR = "/tmp/datalevin-javacpp-cache";

const TARGET_JAR_PATTERNS = [
  /^datalevin-runtime-.*\.jar$/,
  /^datalevin-python-runtime-.*\.jar$/,
  /^datalevin-java-.*\.jar$/
];

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const jarsDir = path.join(packageRoot, "jars");
const repoRoot = path.resolve(packageRoot, "../..");
const targetDir = path.join(repoRoot, "target");

let bridgeModulePromise = null;
let startJvmPromise = null;
let classesPromise = null;
let started = false;

function expandPath(value) {
  if (value.startsWith("~")) {
    return path.resolve(os.homedir(), value.slice(1));
  }
  return path.resolve(value);
}

function findRuntimeJars(dir) {
  if (!fs.existsSync(dir)) {
    return [];
  }

  return fs.readdirSync(dir)
    .filter((entry) => TARGET_JAR_PATTERNS.some((pattern) => pattern.test(entry)))
    .sort()
    .map((entry) => path.join(dir, entry));
}

function splitShellWords(input) {
  if (!input) {
    return [];
  }

  const words = [];
  let current = "";
  let quote = null;
  let escape = false;

  for (const ch of input) {
    if (escape) {
      current += ch;
      escape = false;
      continue;
    }

    if (ch === "\\") {
      escape = true;
      continue;
    }

    if (quote !== null) {
      if (ch === quote) {
        quote = null;
      } else {
        current += ch;
      }
      continue;
    }

    if (ch === "'" || ch === "\"") {
      quote = ch;
      continue;
    }

    if (/\s/.test(ch)) {
      if (current) {
        words.push(current);
        current = "";
      }
      continue;
    }

    current += ch;
  }

  if (escape || quote !== null) {
    throw new DatalevinConfigurationError(
      `Invalid ${DATALEVIN_JVM_ARGS_ENV} shell-style argument string.`
    );
  }

  if (current) {
    words.push(current);
  }

  return words;
}

function ensureJavacppCachedirArg(jvmArgs) {
  for (const arg of jvmArgs) {
    if (
      arg.startsWith("-Dorg.bytedeco.javacpp.cachedir=") ||
      arg.startsWith("-Dorg.bytedeco.javacpp.cacheDir=")
    ) {
      return jvmArgs;
    }
  }

  const cacheDir = process.env[DATALEVIN_JAVACPP_CACHEDIR_ENV] || DEFAULT_JAVACPP_CACHEDIR;
  fs.mkdirSync(cacheDir, { recursive: true });
  jvmArgs.push(`-Dorg.bytedeco.javacpp.cachedir=${cacheDir}`);
  return jvmArgs;
}

function resolveJvmArgs(jvmArgs = null) {
  if (jvmArgs && jvmArgs.length > 0) {
    return ensureJavacppCachedirArg([...jvmArgs]);
  }

  const envArgs = process.env[DATALEVIN_JVM_ARGS_ENV];
  return ensureJavacppCachedirArg(splitShellWords(envArgs));
}

export function resolveClasspath(classpath = null) {
  if (classpath && classpath.length > 0) {
    return classpath.map((entry) => expandPath(entry));
  }

  const envClasspath = process.env[DATALEVIN_CLASSPATH_ENV];
  if (envClasspath) {
    return envClasspath
      .split(path.delimiter)
      .filter(Boolean)
      .map((entry) => expandPath(entry));
  }

  const envJar = process.env[DATALEVIN_JAR_ENV];
  if (envJar) {
    return [expandPath(envJar)];
  }

  const vendored = findRuntimeJars(jarsDir);
  if (vendored.length > 0) {
    return vendored;
  }

  const repoLocal = findRuntimeJars(targetDir);
  if (repoLocal.length > 0) {
    return repoLocal;
  }

  throw new DatalevinConfigurationError(
    "Unable to resolve a Datalevin runtime jar. "
      + `Set ${DATALEVIN_JAR_ENV}, set ${DATALEVIN_CLASSPATH_ENV}, `
      + "or vendor a jar under bindings/javascript/jars/."
  );
}

export function jvmStarted() {
  return started;
}

export async function javaBridgeModule() {
  if (bridgeModulePromise !== null) {
    return bridgeModulePromise;
  }

  bridgeModulePromise = import("java-bridge")
    .then((module) => module.default ?? module)
    .catch((error) => {
      bridgeModulePromise = null;
      throw new DatalevinConfigurationError(
        "The `java-bridge` package is required for the Datalevin Node bindings.",
        { cause: error }
      );
    });
  return bridgeModulePromise;
}

function javaClassName(value) {
  try {
    return value.getClassSync().getNameSync();
  } catch {
    return null;
  }
}

function javaMessage(value) {
  try {
    return value.getMessageSync();
  } catch {
    return null;
  }
}

function coerceJavaError(error) {
  const cause = error?.cause && typeof error.cause.getClassSync === "function" ? error.cause : null;
  const javaClass = cause === null ? null : javaClassName(cause);
  const typeName =
    cause !== null && typeof cause.getErrorTypeSync === "function"
      ? cause.getErrorTypeSync()
      : null;
  const data =
    cause !== null && typeof cause.getDataSync === "function"
      ? cause.getDataSync()
      : null;

  return new DatalevinJavaError(
    javaMessage(cause) || error?.message || "Java call failed.",
    {
      javaClass,
      typeName,
      data,
      cause: error
    }
  );
}

export async function callJava(target, ...args) {
  try {
    return await target(...args);
  } catch (error) {
    throw coerceJavaError(error);
  }
}

export async function callJavaMethod(receiver, methodName, ...args) {
  return callJava(receiver[methodName].bind(receiver), ...args);
}

export async function startJvm({ classpath = null, jvmArgs = null } = {}) {
  if (startJvmPromise !== null) {
    return startJvmPromise;
  }

  startJvmPromise = (async () => {
    const bridge = await javaBridgeModule();
    const resolvedClasspath = resolveClasspath(classpath);
    const resolvedJvmArgs = resolveJvmArgs(jvmArgs);

    try {
      bridge.ensureJvm({
        classpath: resolvedClasspath,
        opts: resolvedJvmArgs
      });
      started = true;
    } catch (error) {
      startJvmPromise = null;
      throw new DatalevinJvmError(
        "Failed to start the JVM for Datalevin. "
          + "Check that Java 21+ is installed and that the runtime jar is valid.",
        { cause: error }
      );
    }
  })();

  return startJvmPromise;
}

export async function classes() {
  if (classesPromise !== null) {
    return classesPromise;
  }

  classesPromise = (async () => {
    await startJvm();
    const { importClass } = await javaBridgeModule();
    const opts = { asyncJavaExceptionObjects: true };
    const load = (className) => importClass(className, opts);

    return {
      datalevin: load("datalevin.Datalevin"),
      interop: load("datalevin.DatalevinInterop"),
      udfFunction: load("datalevin.UdfFunction"),
      datalevinException: load("datalevin.DatalevinException"),
      linkedHashMap: load("java.util.LinkedHashMap"),
      linkedHashSet: load("java.util.LinkedHashSet"),
      arrayList: load("java.util.ArrayList"),
      longClass: load("java.lang.Long"),
      uuid: load("java.util.UUID"),
      instant: load("java.time.Instant"),
      date: load("java.util.Date"),
      bigInteger: load("java.math.BigInteger"),
      bigDecimal: load("java.math.BigDecimal"),
      mapType: load("java.util.Map"),
      listType: load("java.util.List"),
      setType: load("java.util.Set"),
      collectionType: load("java.util.Collection"),
      keywordType: load("clojure.lang.Keyword"),
      symbolType: load("clojure.lang.Symbol")
    };
  })().catch((error) => {
    classesPromise = null;
    throw error;
  });

  return classesPromise;
}

export const __testing = {
  findRuntimeJars,
  splitShellWords,
  resolveJvmArgs,
  ensureJavacppCachedirArg
};
