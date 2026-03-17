import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { __testing, jvmStarted, resolveClasspath } from "../src/jvm.js";

test("findRuntimeJars prefers shared runtime naming and ignores unrelated jars", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-jars-"));

  try {
    const sharedJar = path.join(dir, "datalevin-runtime-0.10.10.jar");
    const legacyJar = path.join(dir, "datalevin-python-runtime-0.10.10.jar");
    const javaJar = path.join(dir, "datalevin-java-0.10.10.jar");
    fs.writeFileSync(sharedJar, "");
    fs.writeFileSync(legacyJar, "");
    fs.writeFileSync(javaJar, "");
    fs.writeFileSync(path.join(dir, "other.jar"), "");

    assert.deepEqual(__testing.findRuntimeJars(dir), [javaJar, legacyJar, sharedJar].sort());
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("splitShellWords handles quoted JVM args", () => {
  assert.deepEqual(
    __testing.splitShellWords('-Xmx1g "-Dfoo=bar baz" -Dabc=123'),
    ["-Xmx1g", "-Dfoo=bar baz", "-Dabc=123"]
  );
});

test("resolveClasspath normalizes explicit classpath entries", () => {
  assert.deepEqual(resolveClasspath(["./target/example.jar"]), [path.resolve("./target/example.jar")]);
});

test("jvmStarted is false before the bridge is initialized", () => {
  assert.equal(jvmStarted(), false);
});
