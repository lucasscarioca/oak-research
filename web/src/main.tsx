import { StrictMode, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';

type TabId = 'chat' | 'sources' | 'runs';
type RuntimeStatus = 'checking' | 'healthy' | 'degraded' | 'offline';

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
  { label: 'Notebooks', value: '1 notebook' },
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

function Shell() {
  const [activeTab, setActiveTab] = useState<TabId>('chat');
  const [apiStatus, setApiStatus] = useState<RuntimeStatus>('checking');
  const [workerStatus, setWorkerStatus] = useState<RuntimeStatus>('checking');
  const [databaseStatus, setDatabaseStatus] = useState<RuntimeStatus>('checking');

  const apiBaseUrl = useMemo(() => import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000', []);
  const workerBaseUrl = useMemo(() => import.meta.env.VITE_WORKER_BASE_URL || 'http://localhost:8001', []);

  useEffect(() => {
    let cancelled = false;

    async function loadStatus() {
      try {
        const apiResponse = await fetch(`${apiBaseUrl}/health`);
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
        const workerResponse = await fetch(`${workerBaseUrl}/health`);
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
          <button className="notebook-card active" type="button">
            <div>
              <strong>Default notebook</strong>
              <span>Auto-created on first run</span>
            </div>
            <span className="badge">Active</span>
          </button>
          <button className="secondary-action" type="button">+ Create notebook</button>
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
          <strong>Owner</strong>
          <p>Local auth, single-user v1</p>
        </div>
      </aside>

      <main className="workspace">
        <header className="workspace-header">
          <div>
            <p className="eyebrow">Notebook workspace</p>
            <h2>Default notebook</h2>
            <p className="subtle">Ask grounded questions over your sources.</p>
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

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Shell />
  </StrictMode>,
);
