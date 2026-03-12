import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { resolveClasspath } from "../src/jvm.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const clojureBin = process.env.CLOJURE_BIN || "clojure";
const serverStartTimeoutMs = 30000;
const serverStopTimeoutMs = 10000;
const outputLimit = 20000;
const serverPortMarker = "__DTLV_PORT__=";

export function runtimeAvailable() {
  try {
    return Boolean(resolveClasspath());
  } catch {
    return false;
  }
}

export function clojureCliAvailable() {
  const result = spawnSync(clojureBin, ["-Sdescribe"], { stdio: "ignore" });
  return result.error?.code !== "ENOENT";
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function serverExpr(root) {
  return `
    (require '[datalevin.server :as srv]
             '[taoensso.timbre :as log])
    (log/set-min-level! :report)
    (let [server (srv/create {:port 0
                              :root ${JSON.stringify(root)}})]
      (.addShutdownHook
       (Runtime/getRuntime)
       (Thread. (fn []
                  (try
                    (srv/stop server)
                    (catch Throwable _)))))
      (srv/start server)
      (let [address ^java.net.InetSocketAddress
            (.getLocalAddress ^java.nio.channels.ServerSocketChannel
                              (.-server-socket ^datalevin.server.Server server))]
        (println (str "${serverPortMarker}" (.getPort address)))
        (flush))
      @(promise))
  `.trim();
}

async function waitForExit(proc, timeoutMs) {
  if (proc.exitCode !== null || proc.signalCode !== null) {
    return true;
  }

  return new Promise((resolve) => {
    const onExit = () => {
      clearTimeout(timer);
      proc.off("exit", onExit);
      resolve(true);
    };
    const timer = setTimeout(() => {
      proc.off("exit", onExit);
      resolve(false);
    }, timeoutMs);
    proc.once("exit", onExit);
  });
}

async function probeLocalPort(port, timeoutMs = 200) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => {
      socket.end();
      resolve();
    });
    socket.once("timeout", () => {
      socket.destroy();
      reject(new Error(`Timed out connecting to 127.0.0.1:${port}`));
    });
    socket.once("error", (error) => {
      socket.destroy();
      reject(error);
    });
  });
}

async function waitForServer(proc, getPort, readOutput, getStartupError) {
  const deadline = Date.now() + serverStartTimeoutMs;
  while (Date.now() < deadline) {
    const startupError = getStartupError();
    if (startupError !== null) {
      throw new Error(
        `Datalevin server failed to start.\n${startupError.message}\n${readOutput()}`
      );
    }
    if (proc.exitCode !== null) {
      throw new Error(
        `Datalevin server exited before becoming ready (code ${proc.exitCode}).\n${readOutput()}`
      );
    }
    const port = getPort();
    if (port !== null) {
      try {
        await probeLocalPort(port);
        return port;
      } catch {
        await delay(100);
        continue;
      }
    }
    await delay(50);
  }

  const port = getPort();
  throw new Error(
    `Timed out waiting for Datalevin server${port === null ? "" : ` on port ${port}`}.\n${readOutput()}`
  );
}

export async function startLiveServer() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-server-"));
  const proc = spawn(clojureBin, ["-M", "-e", serverExpr(root)], {
    cwd: repoRoot,
    stdio: ["ignore", "pipe", "pipe"]
  });

  let output = "";
  let port = null;
  let startupError = null;
  const appendOutput = (chunk) => {
    output += chunk.toString();
    const match = output.match(new RegExp(`${serverPortMarker}(\\d+)`));
    if (match !== null) {
      port = Number(match[1]);
    }
    if (output.length > outputLimit) {
      output = output.slice(-outputLimit);
    }
  };

  proc.stdout.on("data", appendOutput);
  proc.stderr.on("data", appendOutput);
  proc.once("error", (error) => {
    startupError = error;
  });

  try {
    port = await waitForServer(proc, () => port, () => output, () => startupError);
  } catch (error) {
    proc.kill("SIGTERM");
    if (!(await waitForExit(proc, serverStopTimeoutMs))) {
      proc.kill("SIGKILL");
      await waitForExit(proc, serverStopTimeoutMs);
    }
    fs.rmSync(root, { recursive: true, force: true });
    throw error;
  }

  return {
    port,
    root,
    adminUri(username = "datalevin", password = "datalevin") {
      return `dtlv://${username}:${password}@127.0.0.1:${port}`;
    },
    databaseUri(name, username = "datalevin", password = "datalevin") {
      return `${this.adminUri(username, password)}/${name}`;
    },
    readOutput() {
      return output;
    },
    async stop() {
      if (proc.exitCode === null && proc.signalCode === null) {
        proc.kill("SIGTERM");
        if (!(await waitForExit(proc, serverStopTimeoutMs))) {
          proc.kill("SIGKILL");
          await waitForExit(proc, serverStopTimeoutMs);
        }
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  };
}
