import http from "node:http";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { rm, stat } from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { pipeline } from "node:stream/promises";
import { fileURLToPath, pathToFileURL } from "node:url";

const DEFAULT_BODY_LIMIT_BYTES = Number.parseInt(process.env.BODY_LIMIT_BYTES || "65536", 10);
const DEFAULT_HOST = process.env.HOST || "0.0.0.0";
const DEFAULT_PORT = Number.parseInt(process.env.PORT || "3000", 10);
const DEFAULT_MAX_CONCURRENT_SYNTHESIS = 1;

function normalizePositiveInteger(value, fallback) {
  const normalized = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(normalized) || normalized < 1) {
    return fallback;
  }
  return normalized;
}

function createConcurrencyLimiter(maxConcurrent) {
  let activeCount = 0;
  const waitQueue = [];

  async function acquire() {
    if (activeCount < maxConcurrent) {
      activeCount += 1;
      return;
    }

    await new Promise((resolve) => {
      waitQueue.push(resolve);
    });
    activeCount += 1;
  }

  function release() {
    activeCount = Math.max(activeCount - 1, 0);
    const next = waitQueue.shift();
    if (next) {
      next();
    }
  }

  return {
    async run(task) {
      await acquire();
      try {
        return await task();
      } finally {
        release();
      }
    },
  };
}

function resolveWorkerRuntimeConfig(options = {}) {
  const serviceDir = path.dirname(fileURLToPath(import.meta.url));
  const workerScript = options.workerScript || path.join(serviceDir, "worker.py");
  const pythonExecutable = options.pythonExecutable || process.env.PYTHON_EXECUTABLE || "python3";
  const workerEnv = {
    ...process.env,
    COQUI_MODEL: process.env.COQUI_MODEL || "tts_models/es/css10/vits",
    COQUI_LANGUAGE: process.env.COQUI_LANGUAGE || "es",
    COQUI_DEVICE: process.env.COQUI_DEVICE || "cpu",
    PYTHONUNBUFFERED: "1",
  };

  return {
    serviceDir,
    workerScript,
    pythonExecutable,
    workerEnv,
    readyState: {
      model: workerEnv.COQUI_MODEL,
      device: workerEnv.COQUI_DEVICE,
    },
  };
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload));
}

async function respondWithAudioFile(response, synthesis, readyState) {
  const audioPath = typeof synthesis.audio_path === "string" ? synthesis.audio_path : "";
  if (!audioPath) {
    throw new Error("Worker response did not include `audio_path`.");
  }

  const fileStats = await stat(audioPath);
  const audioSize = Number.isFinite(synthesis.audio_size) ? synthesis.audio_size : fileStats.size;
  const audioStream = createReadStream(audioPath);

  await new Promise((resolve, reject) => {
    audioStream.once("open", resolve);
    audioStream.once("error", reject);
  });

  await rm(audioPath, { force: true }).catch(() => {});

  response.writeHead(200, {
    "content-type": synthesis.content_type || "audio/wav",
    "content-length": String(audioSize),
    "x-coqui-model": readyState.model,
    "x-sample-rate": String(synthesis.sample_rate || ""),
  });

  await pipeline(audioStream, response);
}

async function readJsonBody(request, limitBytes) {
  const chunks = [];
  let totalBytes = 0;

  for await (const chunk of request) {
    totalBytes += chunk.length;
    if (totalBytes > limitBytes) {
      const error = new Error(`Request body exceeds ${limitBytes} bytes.`);
      error.statusCode = 413;
      throw error;
    }
    chunks.push(chunk);
  }

  if (chunks.length === 0) {
    const error = new Error("Request body is required.");
    error.statusCode = 400;
    throw error;
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    const error = new Error("Request body must be valid JSON.");
    error.statusCode = 400;
    throw error;
  }
}

function createWorkerClient(options = {}) {
  const { serviceDir, workerScript, pythonExecutable, workerEnv } = resolveWorkerRuntimeConfig(options);

  const worker = spawn(pythonExecutable, [workerScript], {
    cwd: path.resolve(serviceDir, "..", ".."),
    env: workerEnv,
    stdio: ["pipe", "pipe", "pipe"],
  });

  let isReady = false;
  let readyResolve;
  let readyReject;
  const pendingRequests = new Map();

  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });

  const failPending = (error) => {
    if (!isReady) {
      readyReject(error);
    }
    for (const { reject } of pendingRequests.values()) {
      reject(error);
    }
    pendingRequests.clear();
  };

  worker.on("error", (error) => {
    failPending(error);
  });

  worker.on("exit", (code, signal) => {
    const exitError = new Error(
      `Coqui worker exited unexpectedly${code !== null ? ` with code ${code}` : ""}${signal ? ` (signal ${signal})` : ""}.`,
    );
    failPending(exitError);
  });

  worker.stderr.on("data", (chunk) => {
    process.stderr.write(chunk);
  });

  const outputReader = readline.createInterface({ input: worker.stdout });
  outputReader.on("line", (line) => {
    if (!line.trim()) {
      return;
    }

    let message;
    try {
      message = JSON.parse(line);
    } catch {
      process.stderr.write(`Invalid worker JSON: ${line}\n`);
      return;
    }

    if (message.ready) {
      isReady = true;
      readyResolve(message);
      return;
    }

    if (!message.id || !pendingRequests.has(message.id)) {
      return;
    }

    const requestState = pendingRequests.get(message.id);
    pendingRequests.delete(message.id);

    if (message.ok) {
      requestState.resolve(message);
      return;
    }

    const requestError = new Error(message.error || "Worker synthesis failed.");
    requestState.reject(requestError);
  });

  return {
    ready,
    async synthesize(text) {
      await ready;

      if (!worker.stdin.writable) {
        throw new Error("Coqui worker is not writable.");
      }

      const id = randomUUID();
      const payload = JSON.stringify({ action: "synthesize", id, text });

      return new Promise((resolve, reject) => {
        pendingRequests.set(id, { resolve, reject });
        worker.stdin.write(`${payload}\n`, (error) => {
          if (!error) {
            return;
          }
          pendingRequests.delete(id);
          reject(error);
        });
      });
    },
    async close() {
      outputReader.close();

      if (worker.exitCode !== null || worker.signalCode !== null) {
        return;
      }

      await new Promise((resolve) => {
        worker.once("exit", () => resolve());
        worker.kill("SIGTERM");
      });
    },
  };
}

async function synthesizeWithEphemeralWorker(text, options = {}) {
  const workerClient = createWorkerClient(options);

  try {
    const readyState = await workerClient.ready;
    const synthesis = await workerClient.synthesize(text);
    return { readyState, synthesis };
  } finally {
    await workerClient.close().catch(() => {});
  }
}

export async function startServer(options = {}) {
  const host = options.host || DEFAULT_HOST;
  const port = options.port || DEFAULT_PORT;
  const textLimit = options.textLimit || Number.parseInt(process.env.MAX_TEXT_LENGTH || "5000", 10);
  const bodyLimitBytes = options.bodyLimitBytes || DEFAULT_BODY_LIMIT_BYTES;
  const maxConcurrentSynthesis = normalizePositiveInteger(
    options.maxConcurrentSynthesis ?? process.env.COQUI_MAX_CONCURRENT_SYNTHESIS,
    DEFAULT_MAX_CONCURRENT_SYNTHESIS,
  );
  const readyState = resolveWorkerRuntimeConfig(options).readyState;
  const synthesisLimiter = createConcurrencyLimiter(maxConcurrentSynthesis);

  const server = http.createServer(async (request, response) => {
    try {
      if (request.method === "GET" && request.url === "/healthz") {
        sendJson(response, 200, {
          ok: true,
          model: readyState.model,
          device: readyState.device,
        });
        return;
      }

      if (request.method !== "POST" || request.url !== "/tts") {
        sendJson(response, 404, { error: "Route not found." });
        return;
      }

      const body = await readJsonBody(request, bodyLimitBytes);
      const text = typeof body.text === "string" ? body.text.trim() : "";
      if (!text) {
        sendJson(response, 400, { error: "Field `text` must be a non-empty string." });
        return;
      }
      if (text.length > textLimit) {
        sendJson(response, 400, {
          error: `Field \`text\` exceeds the maximum length of ${textLimit} characters.`,
        });
        return;
      }

      const { readyState: requestReadyState, synthesis } = await synthesisLimiter.run(
        () => synthesizeWithEphemeralWorker(text, options),
      );
      await respondWithAudioFile(response, synthesis, requestReadyState);
    } catch (error) {
      const statusCode = error.statusCode || 500;
      if (response.headersSent) {
        response.destroy(error);
        return;
      }
      sendJson(response, statusCode, { error: error.message || "Unexpected server error." });
    }
  });

  const closeServer = async () => {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  };

  process.on("SIGINT", () => {
    closeServer().finally(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    closeServer().finally(() => process.exit(0));
  });

  await new Promise((resolve) => {
    server.listen(port, host, resolve);
  });

  return {
    server,
    close: closeServer,
    host,
    port,
    readyState,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  startServer()
    .then(({ host, port, readyState }) => {
      console.log(
        JSON.stringify({
          ok: true,
          host,
          port,
          model: readyState.model,
          device: readyState.device,
        }),
      );
    })
    .catch((error) => {
      console.error(error.message || error);
      process.exit(1);
    });
}
