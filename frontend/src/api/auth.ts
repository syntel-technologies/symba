// Operator session token handling for the SPA.
//
// The engine supports auth mode=none (loopback/trusted-proxy dev, no credential) and
// mode=token (Bearer shared-secret -> tenant). The SPA cannot know the engine's mode
// ahead of time, so it behaves lazily: it attaches a stored token when present, and
// only forces the login screen when the engine actually answers 401. That way the
// none-mode dev stack never sees a login, while token-mode surfaces it exactly when a
// credential is required or has gone stale.
//
// The token is a shared secret, so we keep it in localStorage (survives reloads) and
// send it only as `Authorization: Bearer <token>`. The SSE client uses fetch rather
// than EventSource so the same header-only credential policy applies everywhere.

const TOKEN_KEY = "symba.auth.token";

// Simple pub/sub so the login gate re-renders when auth state changes (login/logout/
// 401) without threading a context through every view.
type Listener = () => void;
const listeners = new Set<Listener>();

function notify(): void {
  for (const l of listeners) l();
}

export function subscribeAuth(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
  authRequired = false;
  notify();
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
  notify();
}

// Flipped true the moment the engine answers 401. The login gate reads this to decide
// whether to show the login screen (rather than assuming a mode from the client side).
let authRequired = false;

export function isAuthRequired(): boolean {
  return authRequired;
}

// Called by the API client on any 401. Clears the (now-rejected) token so a stale
// credential doesn't keep failing silently, and trips the gate to show login.
export function onUnauthorized(): void {
  if (getToken() !== null) localStorage.removeItem(TOKEN_KEY);
  authRequired = true;
  notify();
}

export function authHeaders(): Record<string, string> {
  const token = getToken();
  return token ? { authorization: `Bearer ${token}` } : {};
}
