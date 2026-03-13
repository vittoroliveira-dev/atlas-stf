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

export async function fetchApiJson<T>(
  pathname: string,
  query?: Record<string, QueryValue>,
): Promise<T> {
  const response = await fetch(buildUrl(pathname, query), {
    cache: "no-store",
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new ApiClientError(`API request failed for ${pathname}`, response.status);
  }

  return (await response.json()) as T;
}
