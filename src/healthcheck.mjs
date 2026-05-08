const DEFAULT_HEALTHCHECK_TIMEOUT_MS = 3000;

function normalizePositiveInteger(value, fallback) {
  const normalizedValue = Number.parseInt(String(value ?? ''), 10);

  if (!Number.isFinite(normalizedValue) || normalizedValue < 1) {
    return fallback;
  }

  return normalizedValue;
}

function resolveHealthcheckConfig() {
  const port = process.env.PORT || '3000';
  const timeoutMs = normalizePositiveInteger(
    process.env.HEALTHCHECK_TIMEOUT_MS,
    DEFAULT_HEALTHCHECK_TIMEOUT_MS
  );

  return {
    timeoutMs,
    url: process.env.HEALTHCHECK_URL || `http://127.0.0.1:${port}/health`,
  };
}

export async function runDockerHealthcheck({
  url,
  timeoutMs,
  fetchImpl = fetch,
} = {}) {
  const config = resolveHealthcheckConfig();
  const effectiveUrl = url || config.url;
  const effectiveTimeoutMs = normalizePositiveInteger(timeoutMs, config.timeoutMs);

  const response = await fetchImpl(effectiveUrl, {
    headers: {
      accept: 'application/json',
    },
    signal: AbortSignal.timeout(effectiveTimeoutMs),
  });

  if (!response.ok) {
    throw new Error(`Health endpoint returned status ${response.status}.`);
  }

  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    throw new Error(`Health endpoint did not return valid JSON: ${error.message}`);
  }

  if (!payload?.ok) {
    throw new Error('Health endpoint did not return ok=true.');
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runDockerHealthcheck()
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      console.error(error.message);
      process.exit(1);
    });
}