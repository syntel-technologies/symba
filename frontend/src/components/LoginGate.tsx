import { useSyncExternalStore, useState } from "react";
import { getToken, isAuthRequired, setToken, subscribeAuth } from "../api/auth";

// Lazy auth gate. The engine may run auth mode=none (no credential) or mode=token
// (Bearer required). The SPA can't know which, so it optimistically renders the app;
// the API client trips `isAuthRequired()` the moment the engine answers 401, at which
// point this gate swaps in the login form. Under mode=none this never triggers, so the
// dev stack shows zero login. A stored token also rehydrates a session across reloads.
//
//   render app ─▶ any /v1 call ─▶ 200  ─▶ stay in app
//                                └▶ 401 ─▶ onUnauthorized() ─▶ show <Login/>
//                 <Login/> submit ─▶ setToken() ─▶ re-render ─▶ back to app

// Two separate subscriptions, each returning a PRIMITIVE. useSyncExternalStore compares
// snapshots with Object.is, so getSnapshot must be referentially stable when the data
// hasn't changed. Returning a fresh {token, required} object literal here would be a
// new reference every render -> React sees "changed" every time -> infinite loop
// (React error #185). Primitives compare by value, so these are stable.
function useToken(): string | null {
  return useSyncExternalStore(subscribeAuth, getToken);
}

function useAuthRequired(): boolean {
  return useSyncExternalStore(subscribeAuth, isAuthRequired);
}

export function LoginGate({ children }: { children: React.ReactNode }) {
  const token = useToken();
  const required = useAuthRequired();
  // Show login only when the engine has actually demanded a credential AND we don't
  // have a (fresh) one. Everything else renders the app.
  if (required && token === null) return <Login />;
  return <>{children}</>;
}

function Login() {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setError("Enter a token.");
      return;
    }
    // Store it; the next API call will use it. If the engine still 401s, the client
    // trips the gate again and clears the bad token, landing us right back here.
    setToken(trimmed);
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-symba-bg px-4">
      <form
        onSubmit={submit}
        className="w-full max-w-sm rounded-lg border border-symba-border bg-symba-panel p-6 shadow-lg"
      >
        <h1 className="mb-1 text-lg font-bold text-symba-text">Sign in to Symba</h1>
        <p className="mb-4 text-sm text-symba-muted">
          This engine requires an access token. Paste your operator token to continue.
        </p>
        <input
          type="password"
          autoFocus
          value={value}
          onChange={(e) => {
            setValue(e.target.value);
            setError(null);
          }}
          placeholder="Access token"
          className="mb-3 w-full rounded border border-symba-border bg-symba-bg px-3 py-2 text-sm text-symba-text outline-none focus:border-indigo-500"
        />
        {error && <p className="mb-3 text-sm text-red-400">{error}</p>}
        <button
          type="submit"
          className="w-full rounded bg-indigo-600 px-3 py-2 text-sm font-medium text-white hover:bg-indigo-500"
        >
          Sign in
        </button>
      </form>
    </div>
  );
}
