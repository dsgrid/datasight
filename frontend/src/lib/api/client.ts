/** Base API client with typed fetch wrapper. */

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export async function fetchJson<T>(
  url: string,
  options?: RequestInit,
): Promise<T> {
  const resp = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new ApiError(resp.status, text || `HTTP ${resp.status}`);
  }

  return resp.json() as Promise<T>;
}

export async function postJson<T>(
  url: string,
  body: unknown,
  options?: RequestInit,
): Promise<T> {
  return fetchJson<T>(url, {
    method: "POST",
    body: JSON.stringify(body),
    ...options,
  });
}

export async function deleteRequest<T>(url: string): Promise<T> {
  return fetchJson<T>(url, { method: "DELETE" });
}

export async function patchJson<T>(url: string, body: unknown): Promise<T> {
  return fetchJson<T>(url, {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}
