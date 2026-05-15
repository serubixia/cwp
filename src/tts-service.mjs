import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { access, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import { fileURLToPath } from 'node:url';

import {
  logDebug,
  logError,
  logInfo,
} from './logger.mjs';

export const DEFAULT_BODY_LIMIT_BYTES = 64 * 1024;
export const DEFAULT_MAX_TEXT_LENGTH = 5000;
export const DEFAULT_MAX_CONCURRENT_SYNTHESIS = 1;
export const DEFAULT_SYNTHESIS_TIMEOUT_MS = 120 * 1000;
const DEFAULT_AUDIO_CONTENT_TYPE = 'audio/wav';
const DEFAULT_AUDIO_FILENAME = 'tts.wav';
const WORKER_SHUTDOWN_GRACE_MS = 2000;

export function normalizePositiveInteger(value, fallback, label = 'value') {
  const normalizedValue = Number.parseInt(String(value ?? ''), 10);

  if (!Number.isFinite(normalizedValue) || normalizedValue < 1) {
    if (fallback != null) {
      return fallback;
    }

    throw new Error(`${label} must be a positive integer.`);
  }

  return normalizedValue;
}

function ensureNonEmptyString(value, label) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }

  return value.trim();
}

function normalizeOptionalString(value) {
  if (value == null) {
    return undefined;
  }

  if (typeof value !== 'string') {
    throw new Error('speaker_wav must be a non-empty string.');
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function formatStderrChunk(chunk) {
  const message = String(chunk).trim();
  if (!message) {
    return undefined;
  }

  return message.length > 1200 ? `...${message.slice(-1197)}` : message;
}

function normalizeAbortReason(reason, fallbackMessage = 'The operation was aborted.') {
  if (reason instanceof Error) {
    return reason;
  }

  const error = new Error(
    typeof reason === 'string' && reason.trim().length > 0
      ? reason.trim()
      : fallbackMessage
  );
  error.name = 'AbortError';
  error.code = 'ABORT_ERR';
  return error;
}

async function waitForAbortable(promise, signal) {
  if (!signal) {
    return promise;
  }

  if (signal.aborted) {
    throw normalizeAbortReason(signal.reason);
  }

  return new Promise((resolve, reject) => {
    const onAbort = () => {
      reject(normalizeAbortReason(signal.reason));
    };

    signal.addEventListener('abort', onAbort, { once: true });

    promise.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

async function runCommand(binary, args) {
  const startedAt = Date.now();
  const commandPreview = `${binary} ${args.join(' ')}`;

  logInfo('process.command.started', {
    binary,
    command: commandPreview.length > 1200 ? `${commandPreview.slice(0, 1197)}...` : commandPreview,
  });

  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });

    child.on('error', (error) => {
      logError('process.command.spawn_failed', {
        binary,
        duration_ms: Date.now() - startedAt,
        error: error.message,
      });
      reject(error);
    });

    child.on('close', (code) => {
      if (code === 0) {
        logInfo('process.command.completed', {
          binary,
          duration_ms: Date.now() - startedAt,
          stdout_bytes: Buffer.byteLength(stdout),
          stderr_bytes: Buffer.byteLength(stderr),
        });
        resolve({ stdout, stderr });
        return;
      }

      logError('process.command.failed', {
        binary,
        duration_ms: Date.now() - startedAt,
        exit_code: code,
        stderr_excerpt: stderr.trim().length > 1200 ? `...${stderr.trim().slice(-1197)}` : stderr.trim() || undefined,
        stdout_excerpt: stdout.trim().length > 1200 ? `...${stdout.trim().slice(-1197)}` : stdout.trim() || undefined,
      });

      reject(new Error([
        `${binary} exited with code ${code}.`,
        stderr.trim(),
        stdout.trim(),
      ].filter(Boolean).join('\n')));
    });
  });
}

export function createConcurrencyLimiter(maxConcurrent) {
  let activeCount = 0;
  const waitQueue = [];

  async function acquire(signal) {
    if (signal?.aborted) {
      throw normalizeAbortReason(signal.reason);
    }

    if (activeCount < maxConcurrent) {
      activeCount += 1;
      return;
    }

    await new Promise((resolve, reject) => {
      const queueEntry = {
        signal,
        resolve: () => {
          queueEntry.cleanup?.();
          resolve();
        },
        abortReject: (error) => {
          queueEntry.cleanup?.();
          reject(error);
        },
        cleanup: null,
      };

      if (signal) {
        const onAbort = () => {
          const entryIndex = waitQueue.indexOf(queueEntry);
          if (entryIndex !== -1) {
            waitQueue.splice(entryIndex, 1);
          }

          queueEntry.abortReject(normalizeAbortReason(signal.reason));
        };

        queueEntry.cleanup = () => {
          signal.removeEventListener('abort', onAbort);
        };

        signal.addEventListener('abort', onAbort, { once: true });
      } else {
        queueEntry.cleanup = () => {};
      }

      waitQueue.push(queueEntry);
    });

    if (signal?.aborted) {
      throw normalizeAbortReason(signal.reason);
    }

    activeCount += 1;
  }

  function release() {
    activeCount = Math.max(activeCount - 1, 0);

    while (waitQueue.length > 0) {
      const next = waitQueue.shift();
      if (!next) {
        return;
      }

      if (next.signal?.aborted) {
        next.cleanup?.();
        continue;
      }

      next.resolve();
      return;
    }
  }

  return {
    async run(task, { signal } = {}) {
      await acquire(signal);
      try {
        if (signal?.aborted) {
          throw normalizeAbortReason(signal.reason);
        }

        return await task();
      } finally {
        release();
      }
    },
  };
}

export function resolveWorkerRuntimeConfig(options = {}) {
  const srcDir = path.dirname(fileURLToPath(import.meta.url));
  const workerScript = path.resolve(options.workerScript || path.join(srcDir, 'worker.py'));
  const pythonExecutable = options.pythonExecutable || process.env.PYTHON_EXECUTABLE || 'python3';
  const speakerWav = normalizeOptionalString(options.speakerWav ?? process.env.COQUI_SPEAKER_WAV);
  const workerEnv = {
    ...process.env,
    COQUI_MODEL: process.env.COQUI_MODEL || 'tts_models/es/css10/vits',
    COQUI_LANGUAGE: process.env.COQUI_LANGUAGE || 'es',
    COQUI_DEVICE: process.env.COQUI_DEVICE || 'cpu',
    PYTHONUNBUFFERED: '1',
  };

  if (speakerWav) {
    workerEnv.COQUI_SPEAKER_WAV = speakerWav;
  }

  return {
    src_dir: srcDir,
    worker_script: workerScript,
    python_executable: pythonExecutable,
    worker_env: workerEnv,
    ready_state: {
      model: workerEnv.COQUI_MODEL,
      language: workerEnv.COQUI_LANGUAGE,
      device: workerEnv.COQUI_DEVICE,
      speaker_wav: speakerWav,
      python_executable: pythonExecutable,
      worker_script: workerScript,
    },
  };
}

function buildWorkerCacheKey(runtimeConfig) {
  return JSON.stringify({
    python_executable: runtimeConfig.python_executable,
    worker_script: runtimeConfig.worker_script,
    model: runtimeConfig.ready_state.model,
    language: runtimeConfig.ready_state.language,
    device: runtimeConfig.ready_state.device,
    speaker_wav: runtimeConfig.ready_state.speaker_wav ?? null,
  });
}

export function normalizeSynthesizeRequest(requestBody, { textLimit = DEFAULT_MAX_TEXT_LENGTH } = {}) {
  if (!requestBody || typeof requestBody !== 'object') {
    throw new Error('The request body must be an object.');
  }

  const text = ensureNonEmptyString(requestBody.text, 'text');
  const speakerWav = Object.hasOwn(requestBody, 'speaker_wav')
    ? ensureNonEmptyString(requestBody.speaker_wav, 'speaker_wav')
    : undefined;

  if (text.length > textLimit) {
    throw new Error(`Field \`text\` exceeds the maximum length of ${textLimit} characters.`);
  }

  return speakerWav ? { text, speaker_wav: speakerWav } : { text };
}

function createWorkerClient(options = {}) {
  const runtimeConfig = options.runtimeConfig || resolveWorkerRuntimeConfig(options);
  let onTerminated = typeof options.onTerminated === 'function'
    ? options.onTerminated
    : undefined;
  const worker = spawn(runtimeConfig.python_executable, [runtimeConfig.worker_script], {
    cwd: path.resolve(runtimeConfig.src_dir, '..'),
    env: runtimeConfig.worker_env,
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  const markTerminated = () => {
    onTerminated?.();
    onTerminated = undefined;
  };

  logInfo('worker.process.started', {
    python_executable: runtimeConfig.python_executable,
    worker_script: runtimeConfig.worker_script,
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

  worker.on('error', (error) => {
    logError('worker.process.spawn_failed', {
      error: error.message,
      python_executable: runtimeConfig.python_executable,
      worker_script: runtimeConfig.worker_script,
    });
    failPending(error);
    markTerminated();
  });

  worker.on('exit', (code, signal) => {
    logInfo('worker.process.exited', {
      exit_code: code ?? undefined,
      signal: signal ?? undefined,
      ready: isReady,
    });

    markTerminated();

    if (pendingRequests.size === 0 && isReady && code === 0 && signal == null) {
      return;
    }

    const exitError = new Error(
      `Coqui worker exited unexpectedly${code !== null ? ` with code ${code}` : ''}${signal ? ` (signal ${signal})` : ''}.`
    );
    failPending(exitError);
  });

  worker.stderr.on('data', (chunk) => {
    const message = formatStderrChunk(chunk);
    if (!message) {
      return;
    }

    logDebug('worker.process.stderr', { message });
  });

  const outputReader = readline.createInterface({ input: worker.stdout });
  outputReader.on('line', (line) => {
    if (!line.trim()) {
      return;
    }

    let message;
    try {
      message = JSON.parse(line);
    } catch {
      logError('worker.process.invalid_json', {
        line: line.length > 1200 ? `...${line.slice(-1197)}` : line,
      });
      return;
    }

    if (message.ready) {
      isReady = true;
      logInfo('worker.ready', {
        model: message.model,
        device: message.device,
      });
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

    const requestError = new Error(message.error || 'Worker synthesis failed.');
    requestState.reject(requestError);
  });

  return {
    ready,
    async synthesize(request) {
      await ready;

      if (!worker.stdin.writable) {
        throw new Error('Coqui worker is not writable.');
      }

      const text = typeof request === 'string'
        ? ensureNonEmptyString(request, 'text')
        : ensureNonEmptyString(request?.text, 'text');
      const speakerWav = request && typeof request === 'object' && Object.hasOwn(request, 'speaker_wav')
        ? ensureNonEmptyString(request.speaker_wav, 'speaker_wav')
        : undefined;
      const id = randomUUID();
      const payload = JSON.stringify({
        action: 'synthesize',
        id,
        text,
        ...(speakerWav ? { speaker_wav: speakerWav } : {}),
      });

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
        markTerminated();
        return;
      }

      await new Promise((resolve) => {
        const forceKillTimer = setTimeout(() => {
          if (worker.exitCode === null && worker.signalCode === null) {
            logError('worker.process.force_killed', {
              worker_script: runtimeConfig.worker_script,
            });
            worker.kill('SIGKILL');
          }
        }, WORKER_SHUTDOWN_GRACE_MS);
        forceKillTimer.unref?.();

        worker.once('exit', () => {
          clearTimeout(forceKillTimer);
          resolve();
        });

        worker.kill('SIGTERM');
      });
    },
  };
}

export function createSharedWorkerClientCache(createClient = createWorkerClient) {
  const workerClients = new Map();

  return {
    get(options = {}) {
      const runtimeConfig = options.runtimeConfig || resolveWorkerRuntimeConfig(options);
      const cacheKey = buildWorkerCacheKey(runtimeConfig);
      const existingClient = workerClients.get(cacheKey);

      if (existingClient) {
        return existingClient;
      }

      let workerClient;
      workerClient = createClient({
        ...options,
        runtimeConfig,
        onTerminated: () => {
          if (workerClients.get(cacheKey) === workerClient) {
            workerClients.delete(cacheKey);
          }
        },
      });
      workerClients.set(cacheKey, workerClient);
      return workerClient;
    },
    async closeAll() {
      const cachedClients = [...new Set(workerClients.values())];
      workerClients.clear();
      await Promise.allSettled(cachedClients.map((workerClient) => workerClient.close()));
    },
  };
}

export const defaultSharedWorkerClientCache = createSharedWorkerClientCache();

export async function warmSharedWorker(options = {}) {
  const sharedWorkerClientCache = options.sharedWorkerClientCache || defaultSharedWorkerClientCache;
  const workerClient = sharedWorkerClientCache.get(options);
  await waitForAbortable(workerClient.ready, options.signal);
  return workerClient;
}

export async function closeSharedWorkers(options = {}) {
  const sharedWorkerClientCache = options.sharedWorkerClientCache || defaultSharedWorkerClientCache;
  await sharedWorkerClientCache.closeAll();
}

export async function getHealthStatus(options = {}) {
  const runtimeConfig = resolveWorkerRuntimeConfig(options);

  await access(runtimeConfig.worker_script);
  const { stdout, stderr } = await runCommand(runtimeConfig.python_executable, ['--version']);
  const versionOutput = `${stdout}\n${stderr}`
    .split('\n')
    .map((line) => line.trim())
    .find(Boolean);

  return {
    ok: true,
    model: runtimeConfig.ready_state.model,
    language: runtimeConfig.ready_state.language,
    device: runtimeConfig.ready_state.device,
    speaker_wav: runtimeConfig.ready_state.speaker_wav,
    python_executable: runtimeConfig.python_executable,
    python_version: versionOutput,
    worker_script: runtimeConfig.worker_script,
  };
}

export async function synthesizeSpeech(requestBody, options = {}) {
  const signal = options.signal;
  const textLimit = normalizePositiveInteger(
    options.textLimit ?? process.env.MAX_TEXT_LENGTH,
    DEFAULT_MAX_TEXT_LENGTH,
    'text_limit'
  );

  if (signal?.aborted) {
    throw normalizeAbortReason(signal.reason, 'Speech synthesis was aborted before it started.');
  }

  const request = normalizeSynthesizeRequest(requestBody, { textLimit });
  const { text } = request;
  const synthesisStartedAt = Date.now();

  logInfo('audio.synthesize.started', {
    text_length: text.length,
    speaker_wav_override: Object.hasOwn(request, 'speaker_wav'),
  });

  const usesSharedWorker = options.workerClientFactory == null;
  const workerClient = usesSharedWorker
    ? (options.sharedWorkerClientCache || defaultSharedWorkerClientCache).get(options)
    : options.workerClientFactory(options);
  let shouldCloseWorker = !usesSharedWorker;

  try {
    const readyState = await waitForAbortable(workerClient.ready, signal);

    if (signal?.aborted) {
      throw normalizeAbortReason(signal.reason);
    }

    const synthesis = await waitForAbortable(workerClient.synthesize(request), signal);
    const audioPath = ensureNonEmptyString(synthesis.audio_path, 'audio_path');
    const fileStats = await stat(audioPath);
    const audioSize = Number.isFinite(synthesis.audio_size)
      ? Number(synthesis.audio_size)
      : fileStats.size;
    const sampleRate = Number.isFinite(synthesis.sample_rate)
      ? Number(synthesis.sample_rate)
      : undefined;

    logInfo('audio.synthesize.completed', {
      duration_ms: Date.now() - synthesisStartedAt,
      text_length: text.length,
      audio_size_bytes: audioSize,
      sample_rate: sampleRate,
      model: readyState.model,
      device: readyState.device,
    });

    return {
      audio_path: audioPath,
      audio_size: audioSize,
      content_type: synthesis.content_type || DEFAULT_AUDIO_CONTENT_TYPE,
      filename: DEFAULT_AUDIO_FILENAME,
      sample_rate: sampleRate,
      model: readyState.model,
      device: readyState.device,
      language: readyState.language,
      cleanup: async () => {
        await rm(audioPath, { force: true });
      },
    };
  } catch (error) {
    const logPayload = {
      duration_ms: Date.now() - synthesisStartedAt,
      text_length: text.length,
      error: error.message,
    };

    if (error.name === 'AbortError' || error.name === 'TimeoutError') {
      logInfo('audio.synthesize.cancelled', logPayload);
    } else {
      logError('audio.synthesize.failed', logPayload);
    }

    throw error;
  } finally {
    if (shouldCloseWorker) {
      await workerClient.close().catch(() => {});
    }
  }
}
