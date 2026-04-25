import { cleanup, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { App } from './main';

const DEFAULT_NOTEBOOK = {
  id: 1,
  name: 'Default notebook',
  is_default: true,
  created_at: '2026-04-24T00:00:00Z',
  updated_at: '2026-04-24T00:00:00Z',
};

const AUTH_USER = {
  id: 1,
  username: 'owner',
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

type SourceRecord = {
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

type RunRecord = {
  id: number;
  notebook_id: number;
  question: string;
  status: string;
  step_label: string | null;
  blocked_reason: string | null;
  error_message: string | null;
  rerun_of_run_id: number | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  answer?: {
    id: number;
    answer_text: string;
    trace_summary: string | null;
    model: string | null;
    citations: Array<{
      id: number;
      source_id: number;
      chunk_ref: string | null;
      citation_text: string;
      citation_index: number;
    }>;
  } | null;
};

type ApiState = {
  providerConfig: ProviderConfig;
  sources: SourceRecord[];
  runs: RunRecord[];
  healthStatus: 'ok' | 'degraded';
  workerStatus: 'ok' | 'degraded';
  defaultNotebook: typeof DEFAULT_NOTEBOOK;
  nextSourceId: number;
  nextRunId: number;
  patchShouldFail: boolean;
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function parseJsonBody(init?: RequestInit): Record<string, unknown> {
  if (init?.body == null) {
    return {};
  }
  if (typeof init.body === 'string') {
    return JSON.parse(init.body) as Record<string, unknown>;
  }
  if (init.body instanceof Uint8Array) {
    return JSON.parse(new TextDecoder().decode(init.body)) as Record<string, unknown>;
  }
  throw new Error('Unsupported request body type');
}

class MockFileReader {
  onerror: ((this: FileReader, ev: ProgressEvent<FileReader>) => unknown) | null = null;
  onload: ((this: FileReader, ev: ProgressEvent<FileReader>) => unknown) | null = null;
  result: string | ArrayBuffer | null = null;

  readAsArrayBuffer(file: Blob) {
    const fallbackBytes = new TextEncoder().encode('pdf bytes');
    const fallbackBuffer = new window.ArrayBuffer(fallbackBytes.length);
    new Uint8Array(fallbackBuffer).set(fallbackBytes);
    void file
      .arrayBuffer()
      .then((buffer) => {
        this.result = buffer;
        this.onload?.call(this as unknown as FileReader, {} as ProgressEvent<FileReader>);
      })
      .catch(() => {
        this.result = fallbackBuffer;
        this.onload?.call(this as unknown as FileReader, {} as ProgressEvent<FileReader>);
      });
  }
}

function installApiMock(state: ApiState) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const method = (init?.method ?? 'GET').toUpperCase();

    if (url.pathname === '/auth/status' && method === 'GET') {
      return jsonResponse({ onboarding_required: false, authenticated: true, user: AUTH_USER });
    }

    if (url.port === '8001' && url.pathname === '/health' && method === 'GET') {
      return jsonResponse({ status: state.workerStatus });
    }

    if (url.pathname === '/health' && method === 'GET') {
      return jsonResponse({
        status: state.healthStatus,
        database: {
          status: state.healthStatus,
          provider_configured: state.providerConfig.validation_status === 'valid',
        },
      });
    }

    if (url.pathname === '/ready' && method === 'GET') {
      return jsonResponse({ ready: true });
    }

    if (url.pathname === '/health' && method === 'HEAD') {
      return jsonResponse({}, 200);
    }

    if (url.pathname === '/worker/health' && method === 'GET') {
      return jsonResponse({ status: state.workerStatus });
    }

    if (url.pathname === '/notebooks/default' && method === 'GET') {
      return jsonResponse(state.defaultNotebook);
    }

    if (url.pathname === '/provider/config' && method === 'GET') {
      return jsonResponse(state.providerConfig);
    }

    if (url.pathname === '/provider/config' && method === 'PUT') {
      const body = parseJsonBody(init);
      const apiKey = String(body.api_key ?? '');
      state.providerConfig = {
        ...state.providerConfig,
        api_key_present: true,
        validation_status: apiKey === 'valid-key' ? 'valid' : 'invalid',
        validation_message: apiKey === 'valid-key' ? null : 'API key not valid. Please pass a valid API key.',
        validated_at: apiKey === 'valid-key' ? '2026-04-24T00:00:00Z' : null,
        updated_at: '2026-04-24T00:00:00Z',
      };
      return jsonResponse(state.providerConfig);
    }

    if (url.pathname === '/provider/config/test' && method === 'POST') {
      state.providerConfig = {
        ...state.providerConfig,
        validation_status: state.providerConfig.api_key_present ? 'valid' : 'invalid',
        validation_message: state.providerConfig.api_key_present ? null : 'No API key configured',
        validated_at: state.providerConfig.api_key_present ? '2026-04-24T00:00:00Z' : null,
        updated_at: '2026-04-24T00:00:00Z',
      };
      return jsonResponse(state.providerConfig);
    }

    if (url.pathname === '/diagnostics' && method === 'GET') {
      return jsonResponse({
        provider_config: state.providerConfig,
        provider_test_result: {
          status: state.providerConfig.validation_status,
          message: state.providerConfig.validation_status === 'valid'
            ? 'Saved key is validated'
            : state.providerConfig.api_key_present
              ? 'Saved key needs attention'
              : 'No API key configured',
          validated_at: state.providerConfig.validated_at,
          api_key_present: state.providerConfig.api_key_present,
        },
        recent_jobs: [
          ...state.sources.slice(0, 2).map((source) => ({
            job_kind: 'source',
            entity_type: 'source',
            job_id: source.latest_job_id ?? source.id,
            entity_id: source.id,
            label: source.title,
            status: source.status,
            step_label: source.job_step_label,
            error_message: source.job_error_message,
            created_at: source.created_at,
            started_at: source.job_started_at,
            finished_at: source.job_finished_at,
          })),
          ...state.runs.slice(0, 2).map((run) => ({
            job_kind: 'run-question',
            entity_type: 'run',
            job_id: run.id,
            entity_id: run.id,
            label: run.question,
            status: run.status,
            step_label: run.step_label,
            error_message: run.error_message,
            created_at: run.created_at,
            started_at: run.started_at,
            finished_at: run.finished_at,
          })),
        ],
        recent_failures: [
          ...state.sources
            .filter((source) => source.status === 'failed')
            .map((source) => ({
              job_kind: 'source',
              entity_type: 'source',
              job_id: source.latest_job_id ?? source.id,
              entity_id: source.id,
              label: source.title,
              status: source.status,
              step_label: source.job_step_label,
              error_message: source.job_error_message,
              created_at: source.created_at,
              started_at: source.job_started_at,
              finished_at: source.job_finished_at,
            })),
          ...state.runs
            .filter((run) => run.status === 'failed' || run.status === 'blocked')
            .map((run) => ({
              job_kind: 'run-question',
              entity_type: 'run',
              job_id: run.id,
              entity_id: run.id,
              label: run.question,
              status: run.status,
              step_label: run.step_label,
              error_message: run.error_message,
              created_at: run.created_at,
              started_at: run.started_at,
              finished_at: run.finished_at,
            })),
        ],
      });
    }

    if (url.pathname === '/sources' && method === 'GET') {
      return jsonResponse(state.sources);
    }

    if (url.pathname === '/sources' && method === 'POST') {
      const body = parseJsonBody(init);
      const sourceType = String(body.source_type ?? 'text');
      const title = String(body.title ?? 'Untitled source');
      const notebookId = Number(body.notebook_id ?? state.defaultNotebook.id);
      const sourceUrl = body.source_url ? String(body.source_url) : null;
      const originalName = body.original_name ? String(body.original_name) : null;
      const mimeType = body.mime_type ? String(body.mime_type) : null;
      const metadata = (body.metadata as Record<string, unknown> | undefined) ?? {};
      const contentText = body.content_text ? String(body.content_text) : null;
      const contentBase64 = body.content_base64 ? String(body.content_base64) : null;
      let rawBytes = Buffer.from('');
      if (contentBase64) {
        rawBytes = Buffer.from(contentBase64, 'base64');
      } else if (contentText) {
        rawBytes = Buffer.from(contentText, 'utf-8');
      } else if (sourceUrl) {
        rawBytes = Buffer.from(contentText ?? sourceUrl, 'utf-8');
      }

      const payloadUri = `/data/oakresearch/sources/${state.nextSourceId}/${state.nextSourceId}.bin`;
      const record: SourceRecord = {
        id: state.nextSourceId,
        notebook_id: notebookId,
        source_type: sourceType,
        title,
        payload_uri: payloadUri,
        payload_sha256: `sha-${state.nextSourceId}`,
        metadata: {
          ...metadata,
          input_kind: contentBase64 ? 'upload' : sourceUrl ? 'url' : 'text',
          original_name: originalName,
          mime_type: mimeType,
          source_url: sourceUrl,
          has_fallback_text: Boolean(contentText && sourceUrl),
        },
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        latest_job_id: state.nextSourceId,
        job_status: 'queued',
        job_step_label: 'queued-for-ingestion',
        job_error_message: null,
        job_started_at: null,
        job_finished_at: null,
        status: 'queued',
      };
      state.sources = [record, ...state.sources];
      state.nextSourceId += 1;
      const responseRecord = { ...record, metadata: { ...record.metadata, raw_bytes_length: rawBytes.length } };
      return jsonResponse(responseRecord);
    }

    if (url.pathname.startsWith('/sources/') && url.pathname.endsWith('/retry') && method === 'POST') {
      const sourceId = Number(url.pathname.split('/')[2]);
      const source = state.sources.find((item) => item.id === sourceId);
      if (!source) {
        return jsonResponse({ detail: 'Source not found' }, 404);
      }
      source.status = 'queued';
      source.job_status = 'queued';
      source.job_step_label = 'queued-for-ingestion';
      source.job_error_message = null;
      source.job_started_at = null;
      source.job_finished_at = null;
      source.latest_job_id = (source.latest_job_id ?? source.id) + 1;
      source.updated_at = '2026-04-24T00:00:00Z';
      return jsonResponse(source);
    }

    if (url.pathname.startsWith('/sources/') && method === 'PATCH') {
      if (state.patchShouldFail) {
        return jsonResponse({ detail: 'Unable to update source title' }, 500);
      }
      const sourceId = Number(url.pathname.split('/').pop());
      const body = parseJsonBody(init);
      const title = String(body.title ?? '');
      const source = state.sources.find((item) => item.id === sourceId);
      if (!source) {
        return jsonResponse({ detail: 'Source not found' }, 404);
      }
      source.title = title;
      source.updated_at = '2026-04-24T00:00:00Z';
      return jsonResponse(source);
    }

    if (url.pathname.startsWith('/sources/') && method === 'GET') {
      const sourceId = Number(url.pathname.split('/').pop());
      const source = state.sources.find((item) => item.id === sourceId) ?? state.sources[0];
      if (!source) {
        return jsonResponse({ detail: 'Source not found' }, 404);
      }
      return jsonResponse({
        ...source,
        chunks: [
          {
            id: source.id * 10,
            source_id: source.id,
            job_id: source.latest_job_id ?? source.id,
            chunk_index: 0,
            chunk_text: `Chunk for ${source.title}`,
            chunk_hash: `hash-${source.id}`,
            created_at: source.created_at,
          },
        ],
      });
    }

    if (url.pathname === '/runs' && method === 'GET') {
      return jsonResponse(state.runs);
    }

    if (url.pathname === '/runs' && method === 'POST') {
      const body = parseJsonBody(init);
      const question = String(body.question ?? 'What does the notebook contain?');
      const run: RunRecord = {
        id: state.nextRunId,
        notebook_id: state.defaultNotebook.id,
        question,
        status: 'queued',
        step_label: 'queued-for-answering',
        blocked_reason: null,
        error_message: null,
        rerun_of_run_id: typeof body.rerun_of_run_id === 'number' ? body.rerun_of_run_id : null,
        created_at: '2026-04-24T00:00:00Z',
        started_at: null,
        finished_at: null,
      };
      state.runs = [run, ...state.runs];
      state.nextRunId += 1;
      return jsonResponse(run);
    }

    if (url.pathname.startsWith('/runs/') && url.pathname.endsWith('/stream') && method === 'GET') {
      const runId = Number(url.pathname.split('/')[2]);
      const run = state.runs.find((item) => item.id === runId);
      const answerText =
        run?.answer?.answer_text ||
        (run?.status === 'blocked'
          ? 'I don’t have enough grounded evidence in the notebook sources to answer that confidently. Please add a more relevant source or ask a narrower question.'
          : 'OakResearch uses FastAPI and Postgres [1].');
      if (run) {
        run.status = run.status === 'blocked' ? 'blocked' : 'succeeded';
        run.step_label = run.status === 'blocked' ? 'grounding-insufficient' : 'answer-complete';
        run.blocked_reason = run.status === 'blocked' ? 'Insufficient grounding in notebook sources' : null;
        run.finished_at = '2026-04-24T00:00:00Z';
        run.answer = {
          id: run.id * 10,
          answer_text: answerText,
          trace_summary: 'Retrieved 1 chunk(s); Sources: OakResearch overview',
          model: 'gemini-2.0-flash',
          citations: run.status === 'blocked'
            ? []
            : [
                {
                  id: run.id * 100,
                  source_id: state.sources[0]?.id ?? 1,
                  chunk_ref: `${state.sources[0]?.id ?? 1}:0`,
                  citation_text: 'Chunk for OakResearch overview',
                  citation_index: 0,
                },
              ],
        };
      }
      return new Response(answerText, {
        status: 200,
        headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      });
    }

    if (url.pathname.startsWith('/runs/') && method === 'GET') {
      const runId = Number(url.pathname.split('/')[2]);
      const run = state.runs.find((item) => item.id === runId);
      if (!run) {
        return jsonResponse({ detail: 'Run not found' }, 404);
      }
      return jsonResponse(run);
    }

    if (url.pathname === '/auth/onboarding' && method === 'POST') {
      return jsonResponse({ authenticated: true, user: AUTH_USER, onboarding_required: false });
    }

    if (url.pathname === '/auth/login' && method === 'POST') {
      return jsonResponse({ authenticated: true, user: AUTH_USER });
    }

    if (url.pathname === '/auth/logout' && method === 'POST') {
      return jsonResponse({ authenticated: false });
    }

    throw new Error(`Unhandled request: ${method} ${url.href}`);
  });

  vi.stubGlobal('fetch', fetchMock as unknown as typeof fetch);
  return fetchMock;
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

beforeEach(() => {
  vi.spyOn(window, 'setInterval').mockImplementation(((callback: TimerHandler) => {
    void callback;
    return 1 as unknown as number;
  }) as typeof window.setInterval);
  vi.spyOn(window, 'clearInterval').mockImplementation(() => undefined);
  vi.stubGlobal('FileReader', MockFileReader as unknown as typeof FileReader);
});

describe('OakResearch shell', () => {
  it('gates the composer until Gemini is validated and allows provider save/test flow', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'invalid',
        validated_at: null,
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: false,
        validation_message: 'API key not valid. Please pass a valid API key.',
      },
      sources: [],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 2,
      nextRunId: 2,
      runs: [],
      patchShouldFail: false,
    };
    const fetchMock = installApiMock(state);
    const user = userEvent.setup();

    render(<App />);

    await screen.findByRole('button', { name: /Configure provider/i });
    const composer = screen.getByLabelText(/Question/i);
    expect(composer).toBeDisabled();
    expect(screen.getByText(/Gemini is not validated yet/i)).toBeInTheDocument();

    await user.click(screen.getAllByRole('button', { name: /Configure provider/i })[0]);
    const keyInput = await screen.findByLabelText(/Gemini API key/i);
    await user.type(keyInput, 'valid-key');
    await user.click(screen.getByRole('button', { name: /Save key/i }));

    await waitFor(() => expect(screen.queryByText(/Provider configuration/i)).not.toBeInTheDocument());
    await waitFor(() => expect(screen.getByLabelText(/Question/i)).not.toBeDisabled());
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/provider/config'),
      expect.objectContaining({ method: 'PUT' }),
    );

    await user.click(screen.getAllByRole('button', { name: /Configure provider/i })[0]);
    await user.click(screen.getByRole('button', { name: /Test saved key/i }));
    await waitFor(() => expect(screen.getByText(/Saved key tested successfully/i)).toBeInTheDocument());
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/provider/config/test'),
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('submits URL and file source payloads through the add-source modal', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'valid',
        validated_at: '2026-04-24T00:00:00Z',
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: true,
        validation_message: null,
      },
      sources: [],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 2,
      nextRunId: 2,
      runs: [],
      patchShouldFail: false,
    };
    const fetchMock = installApiMock(state);
    const user = userEvent.setup();

    render(<App />);
    await screen.findAllByRole('button', { name: /Add source/i });

    await user.click(screen.getAllByRole('button', { name: /Add source/i })[0]);
    await user.selectOptions(screen.getByLabelText(/Source type/i), 'url');
    await user.type(screen.getByLabelText(/Title/i), 'URL source');
    await user.type(screen.getByLabelText(/^URL$/i), 'https://example.com/article');
    await user.type(screen.getByLabelText(/Manual fallback text/i), 'Extracted text for the article');
    await user.click(screen.getAllByRole('button', { name: /Add source/i }).at(-1)!);

    await waitFor(() => expect(screen.queryByLabelText(/Source type/i)).not.toBeInTheDocument());
    expect(state.sources[0]?.source_type).toBe('url');
    expect(state.sources[0]?.metadata.source_url).toBe('https://example.com/article');
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/sources'),
      expect.objectContaining({ method: 'POST' }),
    );

    await user.click(screen.getByRole('button', { name: /Sources/i }));
    expect(screen.getByText(/URL source/i)).toBeInTheDocument();

    await user.click(screen.getAllByRole('button', { name: /Add source/i }).at(-1)!);
    await user.selectOptions(screen.getByLabelText(/Source type/i), 'text');
    await user.type(screen.getByLabelText(/Title/i), 'Text source');
    await user.type(screen.getByLabelText(/Paste text instead of uploading/i), 'Body text for the article');
    await user.click(screen.getAllByRole('button', { name: /Add source/i }).at(-1)!);

    await waitFor(() => expect(screen.getByText(/Text source/i)).toBeInTheDocument());

    const postCalls = fetchMock.mock.calls.filter(([input, init]) =>
      String(input).includes('/sources') && (init?.method ?? 'GET') === 'POST',
    );
    expect(postCalls.length).toBeGreaterThanOrEqual(2);
    const textPayload = JSON.parse(String(postCalls[1]?.[1]?.body)) as Record<string, unknown>;
    expect(textPayload.source_type).toBe('text');
    expect(textPayload.content_text).toBe('Body text for the article');
    expect(textPayload.source_url).toBeUndefined();
  });

  it('surfaces inline source-title edit failures instead of throwing', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'valid',
        validated_at: '2026-04-24T00:00:00Z',
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: true,
        validation_message: null,
      },
      sources: [
        {
          id: 1,
          notebook_id: 1,
          source_type: 'text',
          title: 'Source to rename',
          payload_uri: '/data/oakresearch/sources/1.bin',
          payload_sha256: 'sha-1',
          metadata: { input_kind: 'text' },
          created_at: '2026-04-24T00:00:00Z',
          updated_at: '2026-04-24T00:00:00Z',
          latest_job_id: 1,
          job_status: 'queued',
          job_step_label: 'queued-for-ingestion',
          job_error_message: null,
          job_started_at: null,
          job_finished_at: null,
          status: 'queued',
        },
      ],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 2,
      patchShouldFail: true,
    };
    installApiMock(state);
    const user = userEvent.setup();

    render(<App />);
    await screen.findByRole('button', { name: /Sources/i });
    await user.click(screen.getByRole('button', { name: /Sources/i }));
    await screen.findByText(/Source to rename/i);

    await user.click(screen.getByRole('button', { name: /Edit title/i }));
    const input = screen.getByDisplayValue('Source to rename');
    await user.clear(input);
    await user.type(input, 'Renamed source');
    await user.click(screen.getByRole('button', { name: /Save/i }));

    await waitFor(() => expect(screen.getByText(/Unable to update source title/i)).toBeInTheDocument());
    expect(screen.getByDisplayValue('Renamed source')).toBeInTheDocument();
  });

  it('retries failed source ingestion from the sources tab', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'valid',
        validated_at: '2026-04-24T00:00:00Z',
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: true,
        validation_message: null,
      },
      sources: [
        {
          id: 1,
          notebook_id: 1,
          source_type: 'url',
          title: 'Broken URL source',
          payload_uri: '/data/oakresearch/sources/1.txt',
          payload_sha256: 'sha-1',
          metadata: { input_kind: 'url', source_url: 'https://example.invalid/404' },
          created_at: '2026-04-24T00:00:00Z',
          updated_at: '2026-04-24T00:00:00Z',
          latest_job_id: 1,
          job_status: 'failed',
          job_step_label: 'ingestion-failed',
          job_error_message: 'Unable to fetch URL content from https://example.invalid/404',
          job_started_at: '2026-04-24T00:00:00Z',
          job_finished_at: '2026-04-24T00:00:00Z',
          status: 'failed',
        },
      ],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 2,
      nextRunId: 2,
      runs: [],
      patchShouldFail: false,
    };
    const fetchMock = installApiMock(state);
    const user = userEvent.setup();

    render(<App />);
    await screen.findByRole('button', { name: /Sources/i });
    await user.click(screen.getByRole('button', { name: /Sources/i }));
    await screen.findByText(/Broken URL source/i);

    await user.click(screen.getByRole('button', { name: /Retry ingest/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/sources/1/retry'),
      expect.objectContaining({ method: 'POST' }),
    ));
    await waitFor(() => expect(state.sources[0]?.status).toBe('queued'));
    await waitFor(() => expect(screen.getByText('Queued', { selector: '.badge.source-status' })).toBeInTheDocument());
  });

  it('streams question answers and opens cited source details', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'valid',
        validated_at: '2026-04-24T00:00:00Z',
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: true,
        validation_message: null,
      },
      sources: [
        {
          id: 1,
          notebook_id: 1,
          source_type: 'text',
          title: 'OakResearch overview',
          payload_uri: '/data/oakresearch/sources/1.txt',
          payload_sha256: 'sha-1',
          metadata: { input_kind: 'text' },
          created_at: '2026-04-24T00:00:00Z',
          updated_at: '2026-04-24T00:00:00Z',
          latest_job_id: 1,
          job_status: 'succeeded',
          job_step_label: 'ingestion-complete',
          job_error_message: null,
          job_started_at: '2026-04-24T00:00:00Z',
          job_finished_at: '2026-04-24T00:00:00Z',
          status: 'succeeded',
        },
      ],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 2,
      nextRunId: 2,
      runs: [],
      patchShouldFail: false,
    };
    const fetchMock = installApiMock(state);
    const user = userEvent.setup();

    render(<App />);
    const questionInput = await screen.findByRole('textbox', { name: /Question/i });

    await user.click(questionInput);
    await user.keyboard('What stack does OakResearch use?');
    await waitFor(() => expect((questionInput as HTMLTextAreaElement).value).toBe('What stack does OakResearch use?'));
    await user.click(screen.getByRole('button', { name: /Run query/i }));

    await waitFor(() => expect(screen.getByText(/OakResearch uses FastAPI and Postgres/i)).toBeInTheDocument());
    await waitFor(() => expect(screen.getByRole('button', { name: '[1]' })).toBeInTheDocument());
    const originalRunId = state.runs[0]?.id ?? 0;

    await user.click(screen.getByRole('button', { name: '[1]' }));
    await waitFor(() => expect(screen.getByText(/OakResearch overview/i, { selector: '.eyebrow' })).toBeInTheDocument());
    await waitFor(() => expect(screen.getByText(/Chunk for OakResearch overview/i)).toBeInTheDocument());

    await user.click(screen.getByRole('button', { name: /Runs/i }));
    await user.click(screen.getByRole('button', { name: /Rerun/i }));

    const runCalls = fetchMock.mock.calls.filter(([input, init]) =>
      String(input).includes('/runs') && (init?.method ?? 'GET') === 'POST',
    );
    const rerunPayload = JSON.parse(String(runCalls.at(-1)?.[1]?.body)) as Record<string, unknown>;
    expect(rerunPayload.rerun_of_run_id).toBe(originalRunId);
    await waitFor(() => expect(screen.getAllByText(new RegExp(`Rerun of #${originalRunId}`)).length).toBeGreaterThan(0));
  });

  it('shows diagnostics for provider health and recent jobs', async () => {
    const state: ApiState = {
      providerConfig: {
        provider_name: 'gemini',
        validation_status: 'valid',
        validated_at: '2026-04-24T00:00:00Z',
        created_at: '2026-04-24T00:00:00Z',
        updated_at: '2026-04-24T00:00:00Z',
        api_key_present: true,
        validation_message: null,
      },
      sources: [
        {
          id: 1,
          notebook_id: 1,
          source_type: 'text',
          title: 'OakResearch overview',
          payload_uri: '/data/oakresearch/sources/1.txt',
          payload_sha256: 'sha-1',
          metadata: { input_kind: 'text' },
          created_at: '2026-04-24T00:00:00Z',
          updated_at: '2026-04-24T00:00:00Z',
          latest_job_id: 11,
          job_status: 'succeeded',
          job_step_label: 'ingestion-complete',
          job_error_message: null,
          job_started_at: '2026-04-24T00:00:00Z',
          job_finished_at: '2026-04-24T00:00:00Z',
          status: 'succeeded',
        },
        {
          id: 2,
          notebook_id: 1,
          source_type: 'url',
          title: 'Broken URL source',
          payload_uri: '/data/oakresearch/sources/2.txt',
          payload_sha256: 'sha-2',
          metadata: { input_kind: 'url' },
          created_at: '2026-04-24T01:00:00Z',
          updated_at: '2026-04-24T01:00:00Z',
          latest_job_id: 12,
          job_status: 'failed',
          job_step_label: 'ingestion-failed',
          job_error_message: 'Unable to fetch URL content from https://example.invalid/404',
          job_started_at: '2026-04-24T01:00:00Z',
          job_finished_at: '2026-04-24T01:00:00Z',
          status: 'failed',
        },
      ],
      healthStatus: 'ok',
      workerStatus: 'ok',
      defaultNotebook: DEFAULT_NOTEBOOK,
      nextSourceId: 3,
      nextRunId: 3,
      runs: [
        {
          id: 1,
          notebook_id: 1,
          question: 'What stack does OakResearch use?',
          status: 'blocked',
          step_label: 'grounding-insufficient',
          blocked_reason: 'Insufficient grounding in notebook sources',
          error_message: null,
          rerun_of_run_id: null,
          created_at: '2026-04-24T01:30:00Z',
          started_at: '2026-04-24T01:30:00Z',
          finished_at: '2026-04-24T01:30:00Z',
        },
      ],
      patchShouldFail: false,
    };
    const fetchMock = installApiMock(state);
    const user = userEvent.setup();

    render(<App />);
    await screen.findAllByRole('button', { name: /Diagnostics/i });
    await user.click(screen.getAllByRole('button', { name: /Diagnostics/i })[0]);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining('/diagnostics'),
      expect.objectContaining({ credentials: 'include' }),
    ));
    await waitFor(() => expect(screen.getByRole('heading', { name: /Diagnostics/i })).toBeInTheDocument());
    expect(screen.getByText(/Saved key is validated/i)).toBeInTheDocument();
    expect(screen.getByText(/Runtime health/i)).toBeInTheDocument();
    expect(screen.getByText(/Recent jobs/i)).toBeInTheDocument();
    expect(screen.getByText(/Recent failures/i)).toBeInTheDocument();
    expect(screen.getAllByText(/Broken URL source/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/What stack does OakResearch use\?/i).length).toBeGreaterThan(0);
  });
});
