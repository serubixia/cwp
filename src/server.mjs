import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import {
  DEFAULT_BODY_LIMIT_BYTES,
  DEFAULT_MAX_CONCURRENT_SYNTHESIS,
  DEFAULT_MAX_TEXT_LENGTH,
  DEFAULT_SYNTHESIS_TIMEOUT_MS,
  createConcurrencyLimiter,
  getHealthStatus,
  normalizeSynthesizeRequest,
  normalizePositiveInteger,
  synthesizeSpeech,
} from './tts-service.mjs';
import {
  logError,
  logInfo,
  runWithLogContext,
} from './logger.mjs';

const DEFAULT_HOST = process.env.HOST || '0.0.0.0';
const DEFAULT_PORT = Number(process.env.PORT || 3000);
const DEFAULT_ASYNC_JOB_TTL_MS = 15 * 60 * 1000;
const DEFAULT_ASYNC_JOB_SWEEP_INTERVAL_MS = 60 * 1000;
const SYNTHESIZE_JOB_PATH_PATTERN = /^\/v1\/audio\/synthesize\/jobs\/([^/]+?)(?:\/(download))?$/;

function createHttpError(message, { name = 'Error', code, statusCode } = {}) {
  const error = new Error(message);
  error.name = name;

  if (code) {
    error.code = code;
  }

  if (statusCode) {
    error.statusCode = statusCode;
  }

  return error;
}

function createSynthesisSignal({ request, response, timeoutMs }) {
  const controller = new AbortController();
  let responseFinished = false;

  const abortWith = (error) => {
    if (!controller.signal.aborted) {
      controller.abort(error);
    }
  };

  const onRequestAborted = () => {
    if (!responseFinished) {
      abortWith(createHttpError(
        'The client closed the connection before the synthesis completed.',
        { name: 'AbortError', code: 'REQUEST_ABORTED', statusCode: 499 }
      ));
    }
  };

  const onResponseFinished = () => {
    responseFinished = true;
  };

  const onResponseClosed = () => {
    if (!responseFinished) {
      abortWith(createHttpError(
        'The client closed the response before the synthesis completed.',
        { name: 'AbortError', code: 'RESPONSE_CLOSED', statusCode: 499 }
      ));
    }
  };

  const timeoutId = setTimeout(() => {
    abortWith(createHttpError(
      `Speech synthesis timed out after ${timeoutMs} ms.`,
      { name: 'TimeoutError', code: 'SYNTHESIS_TIMEOUT', statusCode: 504 }
    ));
  }, timeoutMs);
  timeoutId.unref?.();

  const cleanup = () => {
    clearTimeout(timeoutId);
    request.removeListener('aborted', onRequestAborted);
    response.removeListener('finish', onResponseFinished);
    response.removeListener('close', onResponseClosed);
  };

  request.on('aborted', onRequestAborted);
  response.on('finish', onResponseFinished);
  response.on('close', onResponseClosed);
  controller.signal.addEventListener('abort', cleanup, { once: true });

  return {
    signal: controller.signal,
    cleanup,
  };
}

function sendJson(response, statusCode, payload, extraHeaders = {}) {
  response.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    ...extraHeaders,
  });
  response.end(`${JSON.stringify(payload, null, 2)}\n`);
}

function sendBinary(response, statusCode, body, contentType, extraHeaders = {}) {
  response.writeHead(statusCode, {
    'content-type': contentType,
    'content-length': String(body.length),
    ...extraHeaders,
  });
  response.end(body);
}

function setJobExpiry(job, ttlMs) {
  const expiresAtMilliseconds = Date.now() + ttlMs;

  job.expires_at = new Date(expiresAtMilliseconds).toISOString();
  job.expires_at_ms = expiresAtMilliseconds;
}

function buildSynthesizeJobPaths(jobId) {
  const encodedJobId = encodeURIComponent(jobId);
  const statusPath = `/v1/audio/synthesize/jobs/${encodedJobId}`;

  return {
    status_path: statusPath,
    download_path: `${statusPath}/download`,
  };
}

function matchSynthesizeJobPath(pathname) {
  const match = pathname.match(SYNTHESIZE_JOB_PATH_PATTERN);
  if (!match) {
    return null;
  }

  return {
    jobId: decodeURIComponent(match[1]),
    action: match[2] === 'download' ? 'download' : 'status',
  };
}

function sanitizeSynthesisResult(result) {
  if (result == null) {
    return null;
  }

  const {
    buffer,
    audio_path,
    cleanup,
    ...sanitizedResult
  } = result;

  return sanitizedResult;
}

function buildSynthesisJobPayload(job) {
  const {
    result,
    expires_at_ms,
    execution_promise,
    ...jobWithoutInternals
  } = job;

  return {
    ...jobWithoutInternals,
    ...buildSynthesizeJobPaths(job.job_id),
    result: sanitizeSynthesisResult(result),
  };
}

async function sendSynthesisJobDownload(response, result) {
  const extraHeaders = {
    'content-disposition': `inline; filename="${result.filename || 'tts.wav'}"`,
    'x-tts-model': result.model || '',
    'x-tts-device': result.device || '',
    'x-sample-rate': result.sample_rate == null ? '' : String(result.sample_rate),
    'x-coqui-model': result.model || '',
  };

  if (Buffer.isBuffer(result.buffer)) {
    sendBinary(response, 200, result.buffer, result.content_type || 'audio/wav', extraHeaders);
    return;
  }

  const audioStream = createReadStream(result.audio_path);

  try {
    await new Promise((resolve, reject) => {
      audioStream.once('open', resolve);
      audioStream.once('error', reject);
    });

    response.writeHead(200, {
      'content-type': result.content_type || 'audio/wav',
      'content-length': String(result.audio_size),
      ...extraHeaders,
    });

    await pipeline(audioStream, response);
  } finally {
    audioStream.destroy();
  }
}

function createAsyncSynthesisJobStore({
  synthesizeSpeechHandler = synthesizeSpeech,
  textLimit,
  synthesisTimeoutMs,
  maxConcurrentSynthesis,
  jobTtlMs = process.env.ASYNC_JOB_TTL_MS,
  jobSweepIntervalMs = process.env.ASYNC_JOB_SWEEP_INTERVAL_MS,
  ...serviceOptions
} = {}) {
  const jobs = new Map();
  const effectiveJobTtlMs = normalizePositiveInteger(
    jobTtlMs,
    DEFAULT_ASYNC_JOB_TTL_MS,
    'async_job_ttl_ms'
  );
  const effectiveJobSweepIntervalMs = normalizePositiveInteger(
    jobSweepIntervalMs,
    DEFAULT_ASYNC_JOB_SWEEP_INTERVAL_MS,
    'async_job_sweep_interval_ms'
  );
  const limiter = createConcurrencyLimiter(maxConcurrentSynthesis);

  async function cleanupJobResult(job) {
    if (typeof job?.result?.cleanup === 'function') {
      await job.result.cleanup();
    }

    if (job) {
      job.result = null;
    }
  }

  async function deleteJob(jobId) {
    const job = jobs.get(jobId);
    if (!job) {
      return null;
    }

    jobs.delete(jobId);
    await cleanupJobResult(job);
    return job;
  }

  async function expireJobIfNeeded(jobId, now = Date.now()) {
    const job = jobs.get(jobId);
    if (!job) {
      return null;
    }

    if (job.expires_at_ms != null && job.expires_at_ms <= now) {
      await deleteJob(jobId);
      return null;
    }

    return job;
  }

  const sweepTimer = setInterval(() => {
    void Promise.allSettled([...jobs.keys()].map((jobId) => expireJobIfNeeded(jobId)));
  }, effectiveJobSweepIntervalMs);
  sweepTimer.unref?.();

  async function processJob(job, payload) {
    job.status = 'running';
    job.started_at = new Date().toISOString();
    const startedAt = process.hrtime.bigint();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort(createHttpError(
        `Speech synthesis timed out after ${synthesisTimeoutMs} ms.`,
        { name: 'TimeoutError', code: 'SYNTHESIS_TIMEOUT', statusCode: 504 }
      ));
    }, synthesisTimeoutMs);
    timeoutId.unref?.();

    logInfo('job.started', {
      job_id: job.job_id,
      job_type: 'synthesize_speech',
    });

    try {
      job.result = await synthesizeSpeechHandler(payload, {
        textLimit,
        signal: controller.signal,
        ...serviceOptions,
      });
      job.status = 'completed';
      job.error = null;

      logInfo('job.completed', {
        job_id: job.job_id,
        job_type: 'synthesize_speech',
        duration_ms: getElapsedMilliseconds(startedAt),
      });
    } catch (error) {
      job.error = error instanceof Error ? error.message : String(error);
      job.status = 'failed';

      logError('job.failed', {
        job_id: job.job_id,
        job_type: 'synthesize_speech',
        duration_ms: getElapsedMilliseconds(startedAt),
        error: job.error,
      });
    } finally {
      clearTimeout(timeoutId);
      job.completed_at = new Date().toISOString();
      setJobExpiry(job, effectiveJobTtlMs);
    }
  }

  return {
    enqueue(payload) {
      const job = {
        ok: true,
        async: true,
        job: 'synthesize_speech',
        job_id: randomUUID(),
        status: 'queued',
        created_at: new Date().toISOString(),
        started_at: null,
        completed_at: null,
        expires_at: null,
        expires_at_ms: null,
        error: null,
        result: null,
        execution_promise: null,
      };

      jobs.set(job.job_id, job);
      job.execution_promise = limiter.run(() => processJob(job, payload));

      logInfo('job.queued', {
        job_id: job.job_id,
        job_type: 'synthesize_speech',
        payload: summarizeSynthesizePayload(payload),
      });

      return job;
    },
    async get(jobId) {
      return expireJobIfNeeded(jobId);
    },
    async cleanup() {
      clearInterval(sweepTimer);
      const jobSnapshots = [...jobs.values()];
      jobs.clear();

      await Promise.allSettled(jobSnapshots.map((job) => job.execution_promise).filter(Boolean));
      await Promise.allSettled(jobSnapshots.map((job) => cleanupJobResult(job)));
    },
  };
}

function getElapsedMilliseconds(startTime) {
  return Number((process.hrtime.bigint() - startTime) / 1000000n);
}

function getContentType(request) {
  return String(request.headers['content-type'] || '').toLowerCase();
}

function summarizeSynthesizePayload(payload) {
  return {
    text_length: typeof payload?.text === 'string' ? payload.text.trim().length : undefined,
    has_speaker_wav: typeof payload?.speaker_wav === 'string' && payload.speaker_wav.trim().length > 0,
  };
}

function readBodyBuffer(request, maxBodyBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalBytes = 0;

    request.on('data', (chunk) => {
      totalBytes += chunk.length;
      if (totalBytes > maxBodyBytes) {
        const error = new Error(`Request body exceeds the ${maxBodyBytes} byte limit.`);
        error.statusCode = 413;
        reject(error);
        request.destroy();
        return;
      }

      chunks.push(chunk);
    });

    request.on('end', () => {
      resolve(Buffer.concat(chunks));
    });

    request.on('error', reject);
  });
}

async function readJsonBody(request, maxBodyBytes = DEFAULT_BODY_LIMIT_BYTES) {
  const bodyBuffer = await readBodyBuffer(request, maxBodyBytes);
  const body = bodyBuffer.toString('utf8');

  if (!body.trim()) {
    return {};
  }

  try {
    return JSON.parse(body);
  } catch (error) {
    const jsonError = new Error(`Invalid JSON body: ${error.message}`);
    jsonError.statusCode = 400;
    throw jsonError;
  }
}

async function sendSynthesisResult(response, result) {
  const extraHeaders = {
    'content-disposition': `inline; filename="${result.filename || 'tts.wav'}"`,
    'x-tts-model': result.model || '',
    'x-tts-device': result.device || '',
    'x-sample-rate': result.sample_rate == null ? '' : String(result.sample_rate),
    'x-coqui-model': result.model || '',
  };

  if (Buffer.isBuffer(result.buffer)) {
    sendBinary(response, 200, result.buffer, result.content_type || 'audio/wav', extraHeaders);
    return;
  }

  const audioStream = createReadStream(result.audio_path);

  try {
    await new Promise((resolve, reject) => {
      audioStream.once('open', resolve);
      audioStream.once('error', reject);
    });

    response.writeHead(200, {
      'content-type': result.content_type || 'audio/wav',
      'content-length': String(result.audio_size),
      ...extraHeaders,
    });

    await pipeline(audioStream, response);
  } finally {
    await result.cleanup?.();
  }
}

export function createServer({
  synthesizeSpeechHandler = synthesizeSpeech,
  healthStatusHandler = getHealthStatus,
  synthesizeJobStore,
  bodyLimitBytes,
  textLimit,
  synthesisTimeoutMs,
  maxConcurrentSynthesis,
  synthesisLimiter,
  ...serviceOptions
} = {}) {
  const normalizedBodyLimitBytes = normalizePositiveInteger(
    bodyLimitBytes ?? process.env.BODY_LIMIT_BYTES,
    DEFAULT_BODY_LIMIT_BYTES,
    'body_limit_bytes'
  );
  const normalizedTextLimit = normalizePositiveInteger(
    textLimit ?? process.env.MAX_TEXT_LENGTH,
    DEFAULT_MAX_TEXT_LENGTH,
    'text_limit'
  );
  const normalizedSynthesisTimeoutMs = normalizePositiveInteger(
    synthesisTimeoutMs ?? process.env.COQUI_SYNTHESIS_TIMEOUT_MS,
    DEFAULT_SYNTHESIS_TIMEOUT_MS,
    'synthesis_timeout_ms'
  );
  const normalizedMaxConcurrentSynthesis = normalizePositiveInteger(
    maxConcurrentSynthesis ?? process.env.COQUI_MAX_CONCURRENT_SYNTHESIS,
    DEFAULT_MAX_CONCURRENT_SYNTHESIS,
    'max_concurrent_synthesis'
  );
  const effectiveSynthesisLimiter = synthesisLimiter ?? createConcurrencyLimiter(normalizedMaxConcurrentSynthesis);
  const effectiveSynthesizeJobStore = synthesizeJobStore ?? createAsyncSynthesisJobStore({
    synthesizeSpeechHandler,
    textLimit: normalizedTextLimit,
    synthesisTimeoutMs: normalizedSynthesisTimeoutMs,
    maxConcurrentSynthesis: normalizedMaxConcurrentSynthesis,
    ...serviceOptions,
  });

  const server = http.createServer((request, response) => {
    const url = new URL(request.url || '/', `http://${request.headers.host || 'localhost'}`);
    const requestContext = {
      request_id: randomUUID(),
      http_method: request.method || 'GET',
      http_path: url.pathname,
    };
    const startedAt = process.hrtime.bigint();

    response.on('finish', () => {
      void runWithLogContext(requestContext, async () => {
        logInfo('http.request.completed', {
          status_code: response.statusCode,
          duration_ms: getElapsedMilliseconds(startedAt),
          response_content_type: String(response.getHeader('content-type') || ''),
        });
      });
    });

    request.on('aborted', () => {
      void runWithLogContext(requestContext, async () => {
        logError('http.request.aborted', {
          duration_ms: getElapsedMilliseconds(startedAt),
        });
      });
    });

    void runWithLogContext(requestContext, async () => {
      logInfo('http.request.started', {
        content_type: getContentType(request) || undefined,
        content_length: request.headers['content-length'],
      });

      try {
        const synthesizeJobPath = matchSynthesizeJobPath(url.pathname);

        if (request.method === 'GET' && synthesizeJobPath) {
          const job = await effectiveSynthesizeJobStore.get(synthesizeJobPath.jobId);

          if (!job) {
            sendJson(response, 404, {
              ok: false,
              error: 'Job not found.',
            });
            return;
          }

          if (synthesizeJobPath.action === 'download') {
            if (job.status !== 'completed' || job.result == null) {
              sendJson(response, 409, {
                ok: false,
                error: `Job ${job.job_id} is not ready for download.`,
                status: job.status,
              });
              return;
            }

            await sendSynthesisJobDownload(response, job.result);
            return;
          }

          sendJson(response, 200, buildSynthesisJobPayload(job));
          return;
        }

        if (request.method === 'GET' && (url.pathname === '/health' || url.pathname === '/healthz')) {
          sendJson(response, 200, await healthStatusHandler(serviceOptions));
          return;
        }

        if (request.method === 'POST' && url.pathname === '/v1/audio/synthesize') {
          const payload = normalizeSynthesizeRequest(
            await readJsonBody(request, normalizedBodyLimitBytes),
            { textLimit: normalizedTextLimit }
          );

          logInfo('http.request.parsed', {
            operation: 'synthesize_async',
            payload: summarizeSynthesizePayload(payload),
          });

          const job = effectiveSynthesizeJobStore.enqueue(payload);
          const jobPayload = buildSynthesisJobPayload(job);
          sendJson(response, 202, jobPayload, {
            location: jobPayload.status_path,
          });
          return;
        }

        sendJson(response, 404, {
          ok: false,
          error: 'Not found.',
          available_routes: [
            'GET /health',
            'POST /v1/audio/synthesize',
            'GET /v1/audio/synthesize/jobs/:job_id',
            'GET /v1/audio/synthesize/jobs/:job_id/download',
            'GET /healthz (legacy alias)',
          ],
        });
      } catch (error) {
        logError('http.request.failed', {
          error: error.message,
        });

        if (response.headersSent) {
          response.destroy(error);
          return;
        }

        if (response.destroyed || request.aborted) {
          return;
        }

        sendJson(response, error.statusCode || 400, {
          ok: false,
          error: error.message,
        });
      }
    });
  });

  server.on('close', () => {
    void effectiveSynthesizeJobStore.cleanup().catch((error) => {
      logError('job.store.cleanup.failed', {
        job_type: 'synthesize_speech',
        error: error.message,
      });
    });
  });

  return server;
}

export async function startServer(options = {}) {
  const host = options.host || DEFAULT_HOST;
  const port = options.port || DEFAULT_PORT;
  const server = createServer(options);

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

  await new Promise((resolve) => {
    server.listen(port, host, resolve);
  });

  return {
    server,
    close: closeServer,
    host,
    port,
  };
}

const modulePath = fileURLToPath(import.meta.url);

if (process.argv[1] && path.resolve(process.argv[1]) === modulePath) {
  startServer()
    .then(({ host, port }) => {
      logInfo('server.started', {
        port,
        bind: host,
      });
    })
    .catch((error) => {
      logError('server.start_failed', {
        error: error.message,
      });
      process.exit(1);
    });
}
