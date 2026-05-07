import { AsyncLocalStorage } from 'node:async_hooks';

const LOG_LEVEL_PRIORITIES = Object.freeze({
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
});

const DEFAULT_LOG_LEVEL = 'info';
const logContextStorage = new AsyncLocalStorage();

let logWriter = (entry) => {
  const stream = entry.level === 'error' ? process.stderr : process.stdout;
  stream.write(`${JSON.stringify(entry)}\n`);
};

function normalizeLogLevel(value) {
  const normalizedValue = String(value || DEFAULT_LOG_LEVEL).trim().toLowerCase();
  return LOG_LEVEL_PRIORITIES[normalizedValue] == null
    ? DEFAULT_LOG_LEVEL
    : normalizedValue;
}

function shouldLog(level) {
  const activeLevel = normalizeLogLevel(process.env.LOG_LEVEL || DEFAULT_LOG_LEVEL);
  return LOG_LEVEL_PRIORITIES[level] >= LOG_LEVEL_PRIORITIES[activeLevel];
}

function omitUndefinedProperties(payload) {
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== undefined));
}

function writeLog(level, event, payload = {}) {
  if (!shouldLog(level)) {
    return;
  }

  logWriter(omitUndefinedProperties({
    timestamp: new Date().toISOString(),
    level,
    event,
    ...getLogContext(),
    ...payload,
  }));
}

export function getLogContext() {
  return logContextStorage.getStore() ?? {};
}

export function runWithLogContext(context, callback) {
  return logContextStorage.run({
    ...getLogContext(),
    ...omitUndefinedProperties(context ?? {}),
  }, callback);
}

export function setLogWriter(writer) {
  const previousWriter = logWriter;
  logWriter = writer;

  return () => {
    logWriter = previousWriter;
  };
}

export function logDebug(event, payload) {
  writeLog('debug', event, payload);
}

export function logInfo(event, payload) {
  writeLog('info', event, payload);
}

export function logWarn(event, payload) {
  writeLog('warn', event, payload);
}

export function logError(event, payload) {
  writeLog('error', event, payload);
}