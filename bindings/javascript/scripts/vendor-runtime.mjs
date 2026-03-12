import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const packageRoot = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(packageRoot, "../..");
const nativePlatform = process.env.DATALEVIN_NATIVE_PLATFORM ?? "all";

const result = spawnSync("clojure", ["-T:build", "vendor-jar", ":native-platform", nativePlatform], {
  cwd: repoRoot,
  stdio: "inherit"
});

if (result.error) {
  throw result.error;
}

process.exit(result.status ?? 1);
