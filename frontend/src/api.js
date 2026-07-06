const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8080";

export function wsUrl(token) {
  const base = API_BASE_URL.replace(/^http/, "ws");
  return `${base}/ws?token=${encodeURIComponent(token)}`;
}

export async function apiRequest(path, options = {}, token) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers,
  });
  const text = await response.text();
  const body = text ? JSON.parse(text) : null;
  if (!response.ok) {
    throw new Error(body?.error || `HTTP ${response.status}`);
  }
  return body;
}

export const api = {
  login: (username, password) =>
    apiRequest("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  register: (username, password) =>
    apiRequest("/api/v1/auth/register", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  forecast: (readings, token) =>
    apiRequest("/api/v1/forecasts", {
      method: "POST",
      body: JSON.stringify({ readings }),
    }, token),
  forecasts: (token) => apiRequest("/api/v1/forecasts?limit=30", {}, token),
  sustainability: (token) => apiRequest("/api/v1/reports/sustainability", {}, token),
  clusterNodes: (token) => apiRequest("/api/v1/cluster/nodes", {}, token),
  trainings: (token) => apiRequest("/api/v1/trainings?limit=10", {}, token),
  startTraining: (payload, token) =>
    apiRequest("/api/v1/trainings", {
      method: "POST",
      body: JSON.stringify(payload),
    }, token),
  adminMetrics: (token) => apiRequest("/api/v1/admin/metrics", {}, token),
};
