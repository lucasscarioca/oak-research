import { StrictMode, type ReactNode, useEffect, useMemo, useRef, useState } from 'react';
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
    provider_configured?: boolean;
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

type ProviderConfig = {
  provider_name: string;
  validation_status: 'unknown' | 'valid' | 'invalid';
  validated_at: string | null;
  created_at: string | null;
  updated_at: string | null;
  api_key_present: boolean;
  validation_message?: string | null;
};

type SourceItem = {
  id: number;
  notebook_id: number;
  source_type: string;
  title: string;
  payload_uri: string;
  payload_sha256: string;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  latest_job_id?: number | null;
  job_status?: string | null;
  job_step_label?: string | null;
  job_error_message?: string | null;
  job_started_at?: string | null;
  job_finished_at?: string | null;
  status: string;
};

type AuthFormState = {
  username: string;
  password: string;
  confirmPassword: string;
};

type ProviderFormState = {
  apiKey: string;
};

type SourceFormState = {
  sourceType: 'pdf' | 'text' | 'markdown' | 'url';
  title: string;
  sourceUrl: string;
  fallbackText: string;
  originalName: string;
  mimeType: string;
  contentBase64: string;
};

type StatusChipProps = {
  label: string;
  status: RuntimeStatus;
};

type ProviderModalProps = {
  open: boolean;
  providerConfig: ProviderConfig | null;
  busy: boolean;
  testing: boolean;
  error: string | null;
  message: string | null;
  onClose: () => void;
  onSave: (apiKey: string) => Promise<void>;
  onTest: () => Promise<void>;
};

type SourceModalProps = {
  open: boolean;
  defaultNotebookName: string;
  busy: boolean;
  error: string | null;
  onClose: () => void;
  onSave: (form: SourceFormState) => Promise<void>;
};

const tabs: Array<{ id: TabId; label: string; description: string }> = [
  { id: 'chat', label: 'Chat', description: 'Ask grounded questions' },
  { id: 'sources', label: 'Sources', description: 'Review notebook inputs' },
  { id: 'runs', label: 'Runs', description: 'Inspect history and traces' },
];

export function statusLabel(status: string | null | undefined): string {
  switch (status) {
    case 'valid':
    case 'succeeded':
      return 'Healthy';
    case 'queued':
      return 'Queued';
    case 'running':
      return 'Running';
    case 'failed':
      return 'Failed';
    case 'blocked':
      return 'Blocked';
    case 'invalid':
      return 'Needs attention';
    case 'untracked':
      return 'Untracked';
    default:
      return status ? status.replaceAll('_', ' ') : 'Unknown';
  }
}

export function chipStatusFromLabel(label: string | null | undefined): RuntimeStatus {
  if (!label) {
    return 'checking';
  }
  if (label === 'valid' || label === 'succeeded') {
    return 'healthy';
  }
  if (label === 'queued' || label === 'running' || label === 'untracked' || label === 'unknown') {
    return 'checking';
  }
  return 'degraded';
}

function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error('Unable to read file'));
    reader.onload = () => {
      const result = reader.result;
      if (!(result instanceof ArrayBuffer)) {
        reject(new Error('Unable to read file'));
        return;
      }
      let binary = '';
      const bytes = new Uint8Array(result);
      for (let index = 0; index < bytes.length; index += 1) {
        binary += String.fromCharCode(bytes[index]);
      }
      resolve(window.btoa(binary));
    };
    reader.readAsArrayBuffer(file);
  });
}

function StatusChip({ label, status }: StatusChipProps) {
  return (
    <div className="status-chip">
      <span className={`status-dot ${status}`} />
      <span>{label}</span>
    </div>
  );
}

function ModalShell({
  open,
  title,
  description,
  onClose,
  children,
}: {
  open: boolean;
  title: string;
  description: string;
  onClose: () => void;
  children: ReactNode;
}) {
  if (!open) {
    return null;
  }

  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div className="modal-card" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true">
        <div className="modal-header">
          <div>
            <p className="eyebrow">{title}</p>
            <p className="subtle">{description}</p>
          </div>
          <button type="button" className="secondary-action" onClick={onClose}>
            Close
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

function ProviderModal({
  open,
  providerConfig,
  busy,
  testing,
  error,
  message,
  onClose,
  onSave,
  onTest,
}: ProviderModalProps) {
  const [form, setForm] = useState<ProviderFormState>({ apiKey: '' });

  useEffect(() => {
    if (open) {
      setForm({ apiKey: '' });
    }
  }, [open, providerConfig?.updated_at]);

  const status = providerConfig?.validation_status ?? 'unknown';

  return (
    <ModalShell
      open={open}
      title="Provider configuration"
      description="Configure the instance-wide Gemini key and verify it before asking questions."
      onClose={onClose}
    >
      <div className="modal-section">
        <div className="provider-summary">
          <StatusChip label={`Gemini: ${statusLabel(status)}`} status={chipStatusFromLabel(status)} />
          <p className="subtle">{providerConfig?.api_key_present ? 'Saved key present' : 'No key saved yet'}</p>
        </div>
        <form
          className="modal-form"
          onSubmit={async (event) => {
            event.preventDefault();
            await onSave(form.apiKey);
          }}
        >
          <label>
            <span>Gemini API key</span>
            <input
              value={form.apiKey}
              onChange={(event) => setForm({ apiKey: event.target.value })}
              placeholder="Paste your Gemini API key"
              autoComplete="off"
            />
          </label>
          {error && <p className="form-error">{error}</p>}
          {message && <p className="form-success">{message}</p>}
          <div className="modal-actions">
            <button type="button" className="secondary-action" onClick={() => void onTest()} disabled={testing || busy}>
              {testing ? 'Testing…' : 'Test saved key'}
            </button>
            <button type="submit" className="primary-action" disabled={busy}>
              {busy ? 'Saving…' : 'Save key'}
            </button>
          </div>
        </form>
      </div>
    </ModalShell>
  );
}

function SourceModal({ open, defaultNotebookName, busy, error, onClose, onSave }: SourceModalProps) {
  const [form, setForm] = useState<SourceFormState>({
    sourceType: 'pdf',
    title: '',
    sourceUrl: '',
    fallbackText: '',
    originalName: '',
    mimeType: '',
    contentBase64: '',
  });
  const [selectedFileName, setSelectedFileName] = useState<string>('');
  const [fileNotice, setFileNotice] = useState<string | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const fileReadToken = useRef(0);

  useEffect(() => {
    if (open) {
      setForm({
        sourceType: 'pdf',
        title: '',
        sourceUrl: '',
        fallbackText: '',
        originalName: '',
        mimeType: '',
        contentBase64: '',
      });
      setSelectedFileName('');
      setFileNotice(null);
      setLocalError(null);
      fileReadToken.current += 1;
    }
  }, [open]);

  const isUrl = form.sourceType === 'url';
  const requiresFile = form.sourceType === 'pdf';
  const acceptsTextUpload = form.sourceType === 'text' || form.sourceType === 'markdown';

  return (
    <ModalShell
      open={open}
      title="Add source"
      description={`Add a PDF, text/markdown file, or URL to ${defaultNotebookName}.`}
      onClose={onClose}
    >
      <form
        className="modal-form"
        onSubmit={async (event) => {
          event.preventDefault();
          setLocalError(null);
          const hasPastedText = form.fallbackText.trim().length > 0;
          const hasFileUpload = form.contentBase64.length > 0;
          if (isUrl) {
            if (!form.sourceUrl.trim()) {
              setLocalError('Add a URL before saving.');
              return;
            }
          } else if (requiresFile && !hasFileUpload && !hasPastedText) {
            setLocalError('Add a file before saving.');
            return;
          } else if (!hasFileUpload && !hasPastedText) {
            setLocalError('Paste text or upload a file before saving.');
            return;
          }
          await onSave(form);
        }}
      >
        <label>
          <span>Source type</span>
          <select
            value={form.sourceType}
            onChange={(event) => {
              setForm((current) => ({
                ...current,
                sourceType: event.target.value as SourceFormState['sourceType'],
                contentBase64: '',
                originalName: '',
                mimeType: '',
              }));
              setSelectedFileName('');
              setFileNotice(null);
              setLocalError(null);
              fileReadToken.current += 1;
            }}
          >
            <option value="pdf">PDF upload</option>
            <option value="text">Text upload</option>
            <option value="markdown">Markdown upload</option>
            <option value="url">URL</option>
          </select>
        </label>

        <label>
          <span>Title</span>
          <input
            value={form.title}
            onChange={(event) => setForm((current) => ({ ...current, title: event.target.value }))}
            placeholder="e.g. Project notes"
            required
          />
        </label>

        {isUrl ? (
          <div key="url-fields">
            <label>
              <span>URL</span>
              <input
                value={form.sourceUrl}
                onChange={(event) => setForm((current) => ({ ...current, sourceUrl: event.target.value }))}
                placeholder="https://example.com/article"
                required
              />
            </label>
            <label>
              <span>Manual fallback text</span>
              <textarea
                rows={5}
                value={form.fallbackText}
                onChange={(event) => setForm((current) => ({ ...current, fallbackText: event.target.value }))}
                placeholder="Paste the extracted text if the URL can’t be fetched cleanly."
              />
            </label>
          </div>
        ) : (
          <div key="upload-fields">
            <label>
              <span>{requiresFile ? 'PDF file' : 'Text or markdown file'}</span>
              <input
                type="file"
                accept={requiresFile ? '.pdf,application/pdf' : '.txt,.md,.markdown,text/plain,text/markdown'}
                onChange={async (event) => {
                  const file = event.target.files?.[0];
                  const readToken = fileReadToken.current + 1;
                  fileReadToken.current = readToken;
                  if (!file) {
                    setSelectedFileName('');
                    setForm((current) => ({
                      ...current,
                      contentBase64: '',
                      originalName: '',
                      mimeType: '',
                    }));
                    return;
                  }
                  setSelectedFileName(file.name);
                  setFileNotice(null);
                  try {
                    const contentBase64 = await readFileAsBase64(file);
                    if (fileReadToken.current !== readToken) {
                      return;
                    }
                    setForm((current) => ({
                      ...current,
                      contentBase64,
                      originalName: file.name,
                      mimeType: file.type,
                      sourceUrl: '',
                    }));
                  } catch {
                    if (fileReadToken.current === readToken) {
                      setFileNotice('Unable to read the selected file.');
                    }
                  }
                }}
                required={requiresFile}
              />
            </label>
            <p className="subtle">{selectedFileName || 'No file selected yet'}</p>
            {acceptsTextUpload && (
              <label>
                <span>Paste text instead of uploading</span>
                <textarea
                  rows={6}
                  value={form.fallbackText}
                  onChange={(event) => setForm((current) => ({ ...current, fallbackText: event.target.value }))}
                  placeholder="Optional pasted text fallback"
                />
              </label>
            )}
          </div>
        )}

        {fileNotice && <p className="form-success">{fileNotice}</p>}
        {localError && <p className="form-error">{localError}</p>}
        {error && <p className="form-error">{error}</p>}
        <div className="modal-actions">
          <button type="button" className="secondary-action" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary-action" disabled={busy}>
            {busy ? 'Saving…' : 'Add source'}
          </button>
        </div>
      </form>
    </ModalShell>
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
  const [providerConfig, setProviderConfig] = useState<ProviderConfig | null>(null);
  const [sources, setSources] = useState<SourceItem[]>([]);
  const [providerModalOpen, setProviderModalOpen] = useState(false);
  const [sourceModalOpen, setSourceModalOpen] = useState(false);
  const [providerBusy, setProviderBusy] = useState(false);
  const [providerTesting, setProviderTesting] = useState(false);
  const [providerError, setProviderError] = useState<string | null>(null);
  const [providerMessage, setProviderMessage] = useState<string | null>(null);
  const [sourceBusy, setSourceBusy] = useState(false);
  const [sourceError, setSourceError] = useState<string | null>(null);
  const [editingSourceId, setEditingSourceId] = useState<number | null>(null);
  const [editingSourceTitle, setEditingSourceTitle] = useState('');
  const [draftQuestion, setDraftQuestion] = useState('');
  const [loadError, setLoadError] = useState<string | null>(null);

  const apiBaseUrl = useMemo(() => import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000', []);
  const workerBaseUrl = useMemo(() => import.meta.env.VITE_WORKER_BASE_URL || 'http://localhost:8001', []);

  const providerReady = providerConfig?.validation_status === 'valid';

  async function refreshProvider() {
    const response = await fetch(`${apiBaseUrl}/provider/config`, { credentials: 'include' });
    if (!response.ok) {
      throw new Error('Unable to load provider config');
    }
    const data = (await response.json()) as ProviderConfig;
    setProviderConfig(data);
  }

  async function refreshSources() {
    const response = await fetch(`${apiBaseUrl}/sources`, { credentials: 'include' });
    if (!response.ok) {
      throw new Error('Unable to load sources');
    }
    const data = (await response.json()) as SourceItem[];
    setSources(data);
  }

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

    async function loadBootstrapData() {
      try {
        await Promise.all([loadNotebook(), refreshProvider(), refreshSources()]);
      } catch (error_) {
        if (!cancelled) {
          setLoadError(error_ instanceof Error ? error_.message : 'Unable to load notebook data');
        }
      }
    }

    void loadBootstrapData();
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
              <span>{notebook ? 'Auto-created on first run' : 'Loading notebook…'}</span>
            </div>
            <span className="badge">Active</span>
          </div>
        </div>

        <div className="sidebar-panel">
          <div className="panel-label">Provider</div>
          <button type="button" className="sidebar-link provider-link" onClick={() => setProviderModalOpen(true)}>
            <span>Gemini</span>
            <span>{statusLabel(providerConfig?.validation_status ?? 'unknown')}</span>
          </button>
          <button type="button" className="secondary-action" onClick={() => setProviderModalOpen(true)}>
            Configure provider
          </button>
        </div>

        <nav className="sidebar-panel">
          <div className="panel-label">Workspace</div>
          <ul className="sidebar-links">
            <li>
              <button type="button" className="sidebar-link" onClick={() => setProviderModalOpen(true)}>
                <span>Diagnostics</span>
                <span>Provider + health</span>
              </button>
            </li>
            <li>
              <button
                type="button"
                className="sidebar-link"
                onClick={() => {
                  setSourceError(null);
                  setSourceModalOpen(true);
                }}
              >
                <span>Add source</span>
                <span>PDF, text, URL</span>
              </button>
            </li>
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
            <button type="button" className="secondary-action" onClick={() => setProviderModalOpen(true)}>
              Provider settings
            </button>
            <button
              type="button"
              className="primary-action"
              onClick={() => {
                setSourceError(null);
                setSourceModalOpen(true);
              }}
            >
              Add source
            </button>
          </div>
        </header>

        {loadError && <div className="notice-banner">{loadError}</div>}
        {sourceError && <div className="notice-banner">{sourceError}</div>}

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
                {!providerReady && (
                  <div className="notice-banner">
                    Gemini is not validated yet. Save and test a key before running questions.
                  </div>
                )}
                <div className="message message-user">
                  <span className="message-label">You</span>
                  <p>What does the notebook contain?</p>
                </div>
                <div className="message message-assistant">
                  <span className="message-label">OakResearch</span>
                  <p>
                    {providerReady
                      ? 'Waiting for ingestion and run orchestration to be connected.'
                      : 'Configure Gemini to unlock the answering flow.'}
                  </p>
                </div>
              </div>
              <div className="composer">
                <label htmlFor="question" className="panel-label">
                  Question
                </label>
                <textarea
                  id="question"
                  value={draftQuestion}
                  onChange={(event) => setDraftQuestion(event.target.value)}
                  placeholder="Ask something grounded in this notebook..."
                  rows={5}
                  disabled={!providerReady}
                />
                <div className="composer-actions">
                  <button type="button" className="secondary-action" disabled={!providerReady}>
                    Save draft
                  </button>
                  <button type="button" className="primary-action" disabled={!providerReady}>
                    Run query
                  </button>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'sources' && (
            <div className="panel sources-panel">
              <div className="sources-header">
                <div>
                  <h3>Sources</h3>
                  <p>Flat list view for notebook inputs. Source additions are item-scoped and restart-safe.</p>
                </div>
                <button
                  type="button"
                  className="primary-action"
                  onClick={() => {
                    setSourceError(null);
                    setSourceModalOpen(true);
                  }}
                >
                  Add source
                </button>
              </div>

              <div className="sources-list">
                {sources.length === 0 ? (
                  <div className="placeholder-card">
                    <strong>No sources yet</strong>
                    <p>Upload a PDF, text/markdown file, or URL to begin.</p>
                  </div>
                ) : (
                  sources.map((source) => {
                    const isEditing = editingSourceId === source.id;
                    const sourceStatus = source.status ?? source.job_status ?? 'untracked';
                    return (
                      <div className="source-row" key={source.id}>
                        <div className="source-meta">
                          <div className="source-title-row">
                            {isEditing ? (
                              <input
                                className="source-title-input"
                                value={editingSourceTitle}
                                onChange={(event) => setEditingSourceTitle(event.target.value)}
                              />
                            ) : (
                              <strong>{source.title}</strong>
                            )}
                            <span className="badge">{source.source_type}</span>
                            <span className="badge source-status">{statusLabel(sourceStatus)}</span>
                          </div>
                          <p>{source.metadata.original_name ? `File: ${String(source.metadata.original_name)}` : source.payload_uri}</p>
                          <p className="subtle">{source.created_at}</p>
                          {source.job_error_message && <p className="form-error">{source.job_error_message}</p>}
                        </div>
                        <div className="source-actions">
                          {isEditing ? (
                            <>
                              <button
                                type="button"
                                className="secondary-action"
                                onClick={async () => {
                                  try {
                                    const response = await fetch(`${apiBaseUrl}/sources/${source.id}`, {
                                      method: 'PATCH',
                                      credentials: 'include',
                                      headers: { 'Content-Type': 'application/json' },
                                      body: JSON.stringify({ title: editingSourceTitle }),
                                    });
                                    if (!response.ok) {
                                      const data = (await response.json()) as { detail?: string };
                                      throw new Error(data.detail || 'Unable to update source title');
                                    }
                                    setEditingSourceId(null);
                                    setSourceError(null);
                                    await refreshSources();
                                  } catch (saveError) {
                                    setSourceError(saveError instanceof Error ? saveError.message : 'Unable to update source title');
                                  }
                                }}
                              >
                                Save
                              </button>
                              <button type="button" className="secondary-action" onClick={() => setEditingSourceId(null)}>
                                Cancel
                              </button>
                            </>
                          ) : (
                            <button
                              type="button"
                              className="secondary-action"
                              onClick={() => {
                                setEditingSourceId(source.id);
                                setEditingSourceTitle(source.title);
                              }}
                            >
                              Edit title
                            </button>
                          )}
                        </div>
                      </div>
                    );
                  })
                )}
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

      <ProviderModal
        open={providerModalOpen}
        providerConfig={providerConfig}
        busy={providerBusy}
        testing={providerTesting}
        error={providerError}
        message={providerMessage}
        onClose={() => {
          setProviderModalOpen(false);
          setProviderError(null);
        }}
        onSave={async (apiKey) => {
          setProviderBusy(true);
          setProviderError(null);
          setProviderMessage(null);
          try {
            const response = await fetch(`${apiBaseUrl}/provider/config`, {
              method: 'PUT',
              credentials: 'include',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ api_key: apiKey }),
            });
            const data = (await response.json()) as ProviderConfig & { detail?: string };
            if (!response.ok) {
              throw new Error(data.detail || data.validation_message || 'Unable to save provider config');
            }
            setProviderConfig(data);
            setProviderMessage(data.validation_status === 'valid' ? 'Gemini key validated successfully.' : data.validation_message || 'Key saved but validation failed.');
            if (data.validation_status === 'valid') {
              setProviderModalOpen(false);
            }
            await refreshProvider();
          } catch (saveError) {
            setProviderError(saveError instanceof Error ? saveError.message : 'Unable to save provider config');
          } finally {
            setProviderBusy(false);
          }
        }}
        onTest={async () => {
          setProviderTesting(true);
          setProviderError(null);
          setProviderMessage(null);
          try {
            const response = await fetch(`${apiBaseUrl}/provider/config/test`, {
              method: 'POST',
              credentials: 'include',
            });
            const data = (await response.json()) as ProviderConfig & { detail?: string };
            if (!response.ok) {
              throw new Error(data.detail || data.validation_message || 'Unable to test provider config');
            }
            setProviderConfig(data);
            setProviderMessage(data.validation_message || 'Saved key tested successfully.');
            await refreshProvider();
          } catch (testError) {
            setProviderError(testError instanceof Error ? testError.message : 'Unable to test provider config');
          } finally {
            setProviderTesting(false);
          }
        }}
      />

      <SourceModal
        open={sourceModalOpen}
        defaultNotebookName={notebook?.name ?? 'Default notebook'}
        busy={sourceBusy}
        error={sourceError}
        onClose={() => {
          setSourceModalOpen(false);
          setSourceError(null);
        }}
        onSave={async (form) => {
          setSourceBusy(true);
          setSourceError(null);
          try {
            const payload: Record<string, unknown> = {
              source_type: form.sourceType,
              title: form.title,
              original_name: form.originalName || undefined,
              mime_type: form.mimeType || undefined,
              metadata: {
                notebook_name: notebook?.name,
              },
            };
            if (form.sourceType === 'url') {
              payload.source_url = form.sourceUrl;
              if (form.fallbackText.trim()) {
                payload.content_text = form.fallbackText.trim();
              }
            } else if (form.fallbackText.trim()) {
              payload.content_text = form.fallbackText.trim();
            } else if (form.contentBase64) {
              payload.content_base64 = form.contentBase64;
            }

            const response = await fetch(`${apiBaseUrl}/sources`, {
              method: 'POST',
              credentials: 'include',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload),
            });
            const data = (await response.json()) as { detail?: string };
            if (!response.ok) {
              throw new Error(data.detail || 'Unable to add source');
            }
            setSourceModalOpen(false);
            await refreshSources();
            setActiveTab('sources');
          } catch (saveError) {
            setSourceError(saveError instanceof Error ? saveError.message : 'Unable to add source');
          } finally {
            setSourceBusy(false);
          }
        }}
      />
    </div>
  );
}

export function App() {
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

if (import.meta.env.MODE !== 'test') {
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
}
