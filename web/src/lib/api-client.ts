const DEFAULT_API_BASE_URL = "http://127.0.0.1:8000";

type QueryValue = string | number | boolean | null | undefined;

export class ApiClientError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiClientError";
    this.status = status;
  }
}

export function isApiFetchError(error: unknown): error is ApiClientError | TypeError {
  return error instanceof ApiClientError || error instanceof TypeError;
}

/**
 * Return true only when the error is an HTTP 404 (entity not found).
 * All other API/network errors and programming bugs are re-thrown so they
 * propagate to the error boundary instead of degrading to "no data".
 */
export function isNotFoundError(error: unknown): error is ApiClientError {
  if (error instanceof ApiClientError && error.status === 404) return true;
  // Programming bugs (TypeError, ReferenceError, etc.) must never be swallowed.
  throw error;
}

function getApiBaseUrl(): string {
  const raw = process.env.ATLAS_STF_API_BASE_URL ?? DEFAULT_API_BASE_URL;
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

function buildUrl(pathname: string, query?: Record<string, QueryValue>): string {
  const url = new URL(`${getApiBaseUrl()}${pathname}`);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value == null || value === "") {
        continue;
      }
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

const DEFAULT_TIMEOUT_MS = 15_000;

function getTimeoutMs(): number {
  const raw = process.env.ATLAS_STF_API_TIMEOUT_MS;
  if (raw) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return DEFAULT_TIMEOUT_MS;
}

export async function fetchApiJson<T>(
  pathname: string,
  query?: Record<string, QueryValue>,
): Promise<T> {
  const response = await fetch(buildUrl(pathname, query), {
    cache: "no-store",
    headers: {
      Accept: "application/json",
    },
    signal: AbortSignal.timeout(getTimeoutMs()),
  });

  if (!response.ok) {
    throw new ApiClientError(`API request failed for ${pathname}`, response.status);
  }

  return (await response.json()) as T;
}
