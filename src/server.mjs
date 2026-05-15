import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

import {
  closeSharedWorkers,
  DEFAULT_BODY_LIMIT_BYTES,
  DEFAULT_MAX_CONCURRENT_SYNTHESIS,
  DEFAULT_MAX_TEXT_LENGTH,
  DEFAULT_SYNTHESIS_TIMEOUT_MS,
  createConcurrencyLimiter,
  getHealthStatus,
  normalizePositiveInteger,
  synthesizeSpeech,
  warmSharedWorker,
} from './tts-service.mjs';
import {
  logError,
  logInfo,
  runWithLogContext,
} from './logger.mjs';

const DEFAULT_HOST = process.env.HOST || '0.0.0.0';
const DEFAULT_PORT = Number(process.env.PORT || 3000);

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

  return http.createServer((request, response) => {
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
        if (request.method === 'GET' && (url.pathname === '/health' || url.pathname === '/healthz')) {
          sendJson(response, 200, await healthStatusHandler(serviceOptions));
          return;
        }

        if (request.method === 'POST' && (url.pathname === '/v1/audio/synthesize' || url.pathname === '/tts')) {
          const synthesisContext = createSynthesisSignal({
            request,
            response,
            timeoutMs: normalizedSynthesisTimeoutMs,
          });

          try {
            const payload = await readJsonBody(request, normalizedBodyLimitBytes);
            logInfo('http.request.parsed', {
              operation: 'synthesize',
              payload: summarizeSynthesizePayload(payload),
            });

            const result = await effectiveSynthesisLimiter.run(() => synthesizeSpeechHandler(payload, {
              textLimit: normalizedTextLimit,
              signal: synthesisContext.signal,
              ...serviceOptions,
            }), {
              signal: synthesisContext.signal,
            });
            await sendSynthesisResult(response, result);
            return;
          } finally {
            synthesisContext.cleanup();
          }
        }

        sendJson(response, 404, {
          ok: false,
          error: 'Not found.',
          available_routes: [
            'GET /health',
            'POST /v1/audio/synthesize',
            'GET /healthz (legacy alias)',
            'POST /tts (legacy alias)',
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
}

export async function startServer(options = {}) {
  const host = options.host || DEFAULT_HOST;
  const port = options.port || DEFAULT_PORT;
  const preloadWorkerOnStart = options.preloadWorkerOnStart ?? options.synthesizeSpeechHandler == null;

  if (preloadWorkerOnStart) {
    logInfo('worker.preload.started', {
      model: process.env.COQUI_MODEL,
      device: process.env.COQUI_DEVICE,
    });
    await warmSharedWorker(options);
    logInfo('worker.preload.completed', {
      model: process.env.COQUI_MODEL,
      device: process.env.COQUI_DEVICE,
    });
  }

  const server = createServer(options);

  const closeServer = async () => {
    await closeSharedWorkers(options);
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
