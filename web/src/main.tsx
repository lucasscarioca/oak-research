import { StrictMode, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';

type TabId = 'chat' | 'sources' | 'runs';
type RuntimeStatus = 'checking' | 'healthy' | 'degraded' | 'offline';
type AuthMode = 'loading' | 'onboarding' | 'login' | 'authenticated';

type HealthResponse = {
  status: 'ok' | 'degraded';
  database?: {
    status: 'ok' | 'degraded';
    detail?: string;
  };
};

type WorkerHealthResponse = {
  status: 'ok' | 'degraded';
};

type AuthStatusResponse = {
  onboarding_required: boolean;
  authenticated: boolean;
  user: {
    id: number;
    username: string;
  } | null;
};

type AuthUser = {
  id: number;
  username: string;
};

type Notebook = {
  id: number;
  name: string;
  is_default: boolean;
  created_at: string;
  updated_at: string;
};

type AuthFormState = {
  username: string;
  password: string;
  confirmPassword: string;
};

type StatusChipProps = {
  label: string;
  status: RuntimeStatus;
};

const tabs: Array<{ id: TabId; label: string; description: string }> = [
  { id: 'chat', label: 'Chat', description: 'Ask grounded questions' },
  { id: 'sources', label: 'Sources', description: 'Review notebook inputs' },
  { id: 'runs', label: 'Runs', description: 'Inspect history and traces' },
];

const sidebarItems: Array<{ label: string; value: string }> = [
  { label: 'Archive', value: 'Locked for MVP' },
  { label: 'Provider', value: 'Gemini' },
  { label: 'Diagnostics', value: 'System status' },
];

function StatusChip({ label, status }: StatusChipProps) {
  return (
    <div className="status-chip">
      <span className={`status-dot ${status}`} />
      <span>{label}</span>
    </div>
  );
}

function AuthScreen({
  mode,
  statusLabel,
  onSubmit,
  busy,
  error,
}: {
  mode: AuthMode;
  statusLabel: string;
  onSubmit: (form: AuthFormState) => Promise<void>;
  busy: boolean;
  error: string | null;
}) {
  const isOnboarding = mode === 'onboarding';
  const [form, setForm] = useState<AuthFormState>({
    username: '',
    password: '',
    confirmPassword: '',
  });

  return (
    <div className="auth-screen">
      <div className="auth-card">
        <div className="brand-block auth-brand">
          <div className="brand-mark">OR</div>
          <div>
            <h1>OakResearch</h1>
            <p>Self-hosted research notebook</p>
          </div>
        </div>

        <div className="auth-copy">
          <p className="eyebrow">{statusLabel}</p>
          <h2>{isOnboarding ? 'Create the owner account' : 'Sign in to continue'}</h2>
          <p>
            {isOnboarding
              ? 'This instance is locked to a single local owner account.'
              : 'Use the owner account created during onboarding to access notebooks and runs.'}
          </p>
        </div>

        <form
          className="auth-form"
          onSubmit={async (event) => {
            event.preventDefault();
            await onSubmit(form);
          }}
        >
          <label>
            <span>Username</span>
            <input
              value={form.username}
              onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
              autoComplete="username"
              required
            />
          </label>
          <label>
            <span>Password</span>
            <input
              type="password"
              value={form.password}
              onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
              autoComplete={isOnboarding ? 'new-password' : 'current-password'}
              required
            />
          </label>
          {isOnboarding && (
            <label>
              <span>Confirm password</span>
              <input
                type="password"
                value={form.confirmPassword}
                onChange={(event) =>
                  setForm((current) => ({ ...current, confirmPassword: event.target.value }))
                }
                autoComplete="new-password"
                required
              />
            </label>
          )}
          {error && <p className="form-error">{error}</p>}
          <button className="primary-action auth-submit" type="submit" disabled={busy}>
            {busy ? 'Working…' : isOnboarding ? 'Create owner account' : 'Sign in'}
          </button>
        </form>
      </div>
    </div>
  );
}

function Shell({ user, onLogout }: { user: AuthUser; onLogout: () => Promise<void> }) {
  const [activeTab, setActiveTab] = useState<TabId>('chat');
  const [apiStatus, setApiStatus] = useState<RuntimeStatus>('checking');
  const [workerStatus, setWorkerStatus] = useState<RuntimeStatus>('checking');
  const [databaseStatus, setDatabaseStatus] = useState<RuntimeStatus>('checking');
  const [notebook, setNotebook] = useState<Notebook | null>(null);

  const apiBaseUrl = useMemo(() => import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000', []);
  const workerBaseUrl = useMemo(() => import.meta.env.VITE_WORKER_BASE_URL || 'http://localhost:8001', []);

  useEffect(() => {
    let cancelled = false;

    async function loadStatus() {
      try {
        const apiResponse = await fetch(`${apiBaseUrl}/health`, { credentials: 'include' });
        const apiData = (await apiResponse.json()) as HealthResponse;
        if (!cancelled) {
          setApiStatus(apiData.status === 'ok' ? 'healthy' : 'degraded');
          setDatabaseStatus(apiData.database?.status === 'ok' ? 'healthy' : 'degraded');
        }
      } catch {
        if (!cancelled) {
          setApiStatus('offline');
          setDatabaseStatus('offline');
        }
      }

      try {
        const workerResponse = await fetch(`${workerBaseUrl}/health`, { credentials: 'include' });
        const workerData = (await workerResponse.json()) as WorkerHealthResponse;
        if (!cancelled) {
          setWorkerStatus(workerData.status === 'ok' ? 'healthy' : 'degraded');
        }
      } catch {
        if (!cancelled) {
          setWorkerStatus('offline');
        }
      }
    }

    loadStatus();
    const interval = window.setInterval(loadStatus, 15000);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [apiBaseUrl, workerBaseUrl]);

  useEffect(() => {
    let cancelled = false;

    async function loadNotebook() {
      try {
        const response = await fetch(`${apiBaseUrl}/notebooks/default`, { credentials: 'include' });
        if (!response.ok) {
          return;
        }
        const data = (await response.json()) as Notebook;
        if (!cancelled) {
          setNotebook(data);
        }
      } catch {
        if (!cancelled) {
          setNotebook(null);
        }
      }
    }

    loadNotebook();
    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <div className="brand-mark">OR</div>
          <div>
            <h1>OakResearch</h1>
            <p>Self-hosted research notebook</p>
          </div>
        </div>

        <div className="sidebar-panel">
          <div className="panel-label">Notebook</div>
          <div className="notebook-card active" aria-label="Active notebook">
            <div>
              <strong>{notebook?.name ?? 'Default notebook'}</strong>
              <span>
                {notebook ? 'Auto-created on first run' : 'Loading notebook…'}
              </span>
            </div>
            <span className="badge">Active</span>
          </div>
        </div>

        <nav className="sidebar-panel">
          <div className="panel-label">Workspace</div>
          <ul className="sidebar-links">
            {sidebarItems.map((item) => (
              <li key={item.label}>
                <button type="button" className="sidebar-link">
                  <span>{item.label}</span>
                  <span>{item.value}</span>
                </button>
              </li>
            ))}
          </ul>
        </nav>

        <div className="sidebar-panel status-panel">
          <div className="panel-label">Runtime</div>
          <StatusChip label="API" status={apiStatus} />
          <StatusChip label="Worker" status={workerStatus} />
          <StatusChip label="Database" status={databaseStatus} />
        </div>

        <div className="sidebar-panel user-card">
          <div className="panel-label">Account</div>
          <strong>{user.username}</strong>
          <p>Local owner account</p>
          <button type="button" className="secondary-action" onClick={() => void onLogout()}>
            Log out
          </button>
        </div>
      </aside>

      <main className="workspace">
        <header className="workspace-header">
          <div>
            <p className="eyebrow">Notebook workspace</p>
            <h2>{notebook?.name ?? 'Default notebook'}</h2>
            <p className="subtle">
              {notebook ? 'Ask grounded questions over your sources.' : 'Loading notebook…'}
            </p>
          </div>
          <div className="header-actions">
            <button type="button" className="secondary-action">Provider settings</button>
            <button type="button" className="primary-action">Add source</button>
          </div>
        </header>

        <section className="tab-strip" aria-label="Notebook tabs">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              className={`tab-button ${activeTab === tab.id ? 'active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              <span>{tab.label}</span>
              <small>{tab.description}</small>
            </button>
          ))}
        </section>

        <section className="workspace-content">
          {activeTab === 'chat' && (
            <div className="panel split-panel">
              <div className="conversation-area">
                <div className="empty-state">
                  <h3>Start with a question</h3>
                  <p>
                    Add a source, configure Gemini, and ask a grounded question. Answers will stream
                    here with inline citations.
                  </p>
                </div>
                <div className="message message-user">
                  <span className="message-label">You</span>
                  <p>What does the notebook contain?</p>
                </div>
                <div className="message message-assistant">
                  <span className="message-label">OakResearch</span>
                  <p>Waiting for sources and provider configuration before answering.</p>
                </div>
              </div>
              <div className="composer">
                <label htmlFor="question" className="panel-label">Question</label>
                <textarea id="question" placeholder="Ask something grounded in this notebook..." rows={5} />
                <div className="composer-actions">
                  <button type="button" className="secondary-action">Save draft</button>
                  <button type="button" className="primary-action">Run query</button>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'sources' && (
            <div className="panel empty-grid">
              <div>
                <h3>Sources</h3>
                <p>
                  Flat list view for files and URLs. In MVP, source ingestion happens through a single
                  add-source modal.
                </p>
              </div>
              <button type="button" className="primary-action">Add source</button>
              <div className="placeholder-card">
                <strong>No sources yet</strong>
                <p>Upload a PDF, markdown file, or URL to begin.</p>
              </div>
            </div>
          )}

          {activeTab === 'runs' && (
            <div className="panel empty-grid">
              <div>
                <h3>Runs</h3>
                <p>Every query attempt will appear here with status, citations, and trace details.</p>
              </div>
              <div className="placeholder-card">
                <strong>Run history is empty</strong>
                <p>Ask your first question to create a stored run.</p>
              </div>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

function App() {
  const apiBaseUrl = useMemo(() => import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000', []);
  const [mode, setMode] = useState<AuthMode>('loading');
  const [user, setUser] = useState<AuthUser | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadAuth() {
      try {
        const response = await fetch(`${apiBaseUrl}/auth/status`, { credentials: 'include' });
        const data = (await response.json()) as AuthStatusResponse;
        if (cancelled) {
          return;
        }

        setUser(data.user);
        if (data.authenticated && data.user) {
          setMode('authenticated');
        } else if (data.onboarding_required) {
          setMode('onboarding');
        } else {
          setMode('login');
        }
      } catch {
        if (!cancelled) {
          setMode('login');
        }
      }
    }

    loadAuth();
    return () => {
      cancelled = true;
    };
  }, [apiBaseUrl]);

  async function submitAuth(path: '/auth/onboarding' | '/auth/login', form: AuthFormState) {
    setBusy(true);
    setError(null);
    try {
      const response = await fetch(`${apiBaseUrl}${path}`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: form.username,
          password: form.password,
          ...(path === '/auth/onboarding' ? { confirm_password: form.confirmPassword } : {}),
        }),
      });
      const data = (await response.json()) as { user?: AuthUser; detail?: string };
      if (!response.ok) {
        throw new Error(data.detail || 'Authentication failed');
      }
      if (data.user) {
        setUser(data.user);
      }
      setMode('authenticated');
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : 'Authentication failed');
    } finally {
      setBusy(false);
    }
  }

  async function logout() {
    setBusy(true);
    setError(null);
    try {
      await fetch(`${apiBaseUrl}/auth/logout`, {
        method: 'POST',
        credentials: 'include',
      });
      setUser(null);
      setMode('login');
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : 'Logout failed');
    } finally {
      setBusy(false);
    }
  }

  if (mode === 'loading') {
    return (
      <div className="auth-screen">
        <div className="auth-card">
          <div className="brand-block auth-brand">
            <div className="brand-mark">OR</div>
            <div>
              <h1>OakResearch</h1>
              <p>Self-hosted research notebook</p>
            </div>
          </div>
          <div className="auth-copy">
            <p className="eyebrow">Loading</p>
            <h2>Checking owner access</h2>
            <p>Verifying whether this instance needs onboarding or an owner sign-in.</p>
          </div>
        </div>
      </div>
    );
  }

  if (mode !== 'authenticated' || user === null) {
    return (
      <AuthScreen
        mode={mode}
        statusLabel={mode === 'onboarding' ? 'First run setup' : 'Owner access'}
        onSubmit={async (form) => submitAuth(mode === 'onboarding' ? '/auth/onboarding' : '/auth/login', form)}
        busy={busy}
        error={error}
      />
    );
  }

  return <Shell user={user} onLogout={logout} />;
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
