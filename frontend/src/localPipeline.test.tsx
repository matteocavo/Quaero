import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';

const MOCK_UPLOAD = {
  upload_id: 'upload_abc123def456',
  original_filename: 'customer_revenue.csv',
  size_bytes: 2048,
  row_count: 8,
  column_count: 4,
  delimiter: ',',
  columns: [
    {
      name: 'order_year',
      normalized_name: 'order_year',
      dtype: 'integer',
      null_count: 0,
      unique_count: 4,
      sample_values: ['2021', '2022', '2023'],
    },
    {
      name: 'segment',
      normalized_name: 'segment',
      dtype: 'string',
      null_count: 0,
      unique_count: 3,
      sample_values: ['Enterprise', 'SMB', 'Consumer'],
    },
    {
      name: 'revenue',
      normalized_name: 'revenue',
      dtype: 'float',
      null_count: 0,
      unique_count: 8,
      sample_values: ['14500.5', '18900.75', '22100.0'],
    },
  ],
  numeric_columns: ['revenue'],
  candidate_dimensions: ['segment', 'order_year'],
  candidate_time_columns: ['order_year'],
  created_at: '2026-10-28T10:00:00Z',
};

const READY_INTERPRETATION = {
  question: 'Which segment generates the highest average revenue?',
  language: 'en',
  status: 'ready',
  metric: { column: 'revenue', origin: 'auto_inferred' },
  dimension: { column: 'segment', origin: 'auto_inferred' },
  aggregation: { value: 'average', origin: 'auto_inferred' },
  ordering: { value: 'highest', origin: 'auto_inferred' },
  time: { column: 'order_year', origin: 'auto_inferred', grouping: null },
  filters: { requested: false },
  issues: [],
  summary: 'Average of revenue for each segment, highest first.',
  suggested_questions: [],
};

describe('Local Operations Pipeline Workflow (Import -> Configure -> Run -> Results)', () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    cleanup();
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('keeps Public Demo mode free of Local Operations upload wizard and network calls', () => {
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    render(<App />);
    fireEvent.click(screen.getAllByRole('button', { name: /Import & Profile Data/i })[0]);

    expect(screen.queryByTestId('local-pipeline-wizard')).toBeNull();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('completes CSV upload -> project creation -> pipeline execution -> results exploration in Local Operations mode', async () => {
    let projectsList = [
      { project: 'release_impact', marts_count: 8, has_summary: true },
    ];
    let uploadsList: (typeof MOCK_UPLOAD)[] = [];

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      const method = (init?.method || 'GET').toUpperCase();

      if (url === '/api/health') {
        return new Response(JSON.stringify({ status: 'ok', version: '1.0.0' }), { status: 200 });
      }
      if (url === '/api/projects' && method === 'GET') {
        return new Response(JSON.stringify({ projects: projectsList }), { status: 200 });
      }
      if (url === '/api/uploads' && method === 'GET') {
        return new Response(JSON.stringify({ uploads: uploadsList }), { status: 200 });
      }
      if (url === '/api/uploads' && method === 'POST') {
        uploadsList = [MOCK_UPLOAD];
        return new Response(JSON.stringify(MOCK_UPLOAD), { status: 201 });
      }
      if (url.endsWith('/interpretation') && method === 'POST') {
        return new Response(JSON.stringify(READY_INTERPRETATION), { status: 200 });
      }
      if (url === '/api/projects' && method === 'POST') {
        const body = JSON.parse(String(init?.body || '{}'));
        const createdProject = {
          project: 'customer_revenue',
          upload_id: body.upload_id,
          dataset_name: 'customer_revenue',
          question: body.question,
          metric_column: body.metric_column || 'revenue',
          dimension_column: body.dimension_column || 'segment',
          time_column: 'order_year',
          has_config: true,
          created_at: '2026-10-28T10:01:00Z',
        };
        projectsList = [
          ...projectsList,
          { project: 'customer_revenue', marts_count: 5, has_summary: true },
        ];
        return new Response(JSON.stringify(createdProject), { status: 201 });
      }
      if (url.endsWith('/run') && method === 'POST') {
        return new Response(
          JSON.stringify({
            run_id: 'run_local_001',
            project: 'customer_revenue',
            status: 'QUEUED',
            message: 'Run accepted and queued.',
          }),
          { status: 202 },
        );
      }
      if (url.includes('/runs/run_local_001')) {
        return new Response(
          JSON.stringify({
            run_id: 'run_local_001',
            project: 'customer_revenue',
            status: 'COMPLETED',
            error: null,
            result_keys: ['marts', 'dq_score', 'anomaly_framing_v2', 'lineage'],
          }),
          { status: 200 },
        );
      }
      if (url.endsWith('/runs?limit=50')) {
        return new Response(
          JSON.stringify({
            project: 'customer_revenue',
            total: 1,
            runs: [
              {
                run_id: 'run_local_001',
                project: 'customer_revenue',
                status: 'COMPLETED',
                question: 'Which segment generates the highest average revenue?',
                created_at: '2026-10-28T10:01:00Z',
                started_at: '2026-10-28T10:01:01Z',
                completed_at: '2026-10-28T10:01:03Z',
              },
            ],
          }),
          { status: 200 },
        );
      }
      if (url.endsWith('/overview')) {
        return new Response(
          JSON.stringify({
            project: 'customer_revenue',
            project_mode: 'single_dataset',
            question: 'Which segment generates the highest average revenue?',
            primary_mart: 'mart_question_answer',
            primary_kpi: 'avg_revenue',
            marts_count: 5,
            has_config: true,
            artifacts: {
              summary: true,
              data_quality: true,
              anomalies: true,
              lineage: true,
              metrics_definitions: true,
            },
            last_run: {
              run_id: 'run_local_001',
              project: 'customer_revenue',
              status: 'COMPLETED',
              question: 'Which segment generates the highest average revenue?',
              created_at: '2026-10-28T10:01:00Z',
            },
            history_available: true,
          }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ detail: 'Not mocked' }), { status: 404 });
    });

    globalThis.fetch = fetchMock as unknown as typeof fetch;

    render(<App />);

    // Switch to Local Operations mode via Header Settings modal
    fireEvent.click(screen.getByTitle(/Configure environment connection/i));
    fireEvent.click(screen.getByRole('button', { name: /Local Operations/i }));

    // Open Import & Profile Data tab
    fireEvent.click(screen.getAllByRole('button', { name: /Import & Profile Data/i })[0]);

    const wizard = await screen.findByTestId('local-pipeline-wizard');
    expect(wizard).toBeTruthy();

    // 1. Upload CSV to FastAPI
    const fileInput = screen.getByTestId('local-csv-upload-input') as HTMLInputElement;
    const csvFile = new File(
      ['order_year,segment,revenue\n2023,Enterprise,14500\n2024,SMB,18900\n'],
      'customer_revenue.csv',
      { type: 'text/csv' },
    );
    fireEvent.change(fileInput, { target: { files: [csvFile] } });

    await waitFor(() => {
      expect(screen.getByTestId('upload-metadata-card')).toBeTruthy();
    });
    expect(screen.getByText('upload_abc123def456')).toBeTruthy();

    // 2. Configure project name and question, then click Create Project & Run Python Pipeline
    const projectInput = screen.getByTestId('local-project-name-input') as HTMLInputElement;
    expect(projectInput.value).toBe('customer_revenue');

    // The run can only be confirmed on an interpretation the user has seen.
    expect((screen.getByTestId('create-and-run-btn') as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByTestId('check-interpretation-btn'));
    await screen.findByTestId('interpretation-panel');
    expect(screen.getByTestId('interpretation-status').textContent).toBe('Ready');
    expect((screen.getByTestId('create-and-run-btn') as HTMLButtonElement).disabled).toBe(false);

    fireEvent.click(screen.getByTestId('create-and-run-btn'));

    // 3 & 4. Transitions to Pipeline Operations and displays completed results panel
    await waitFor(() => {
      expect(screen.getByTestId('local-results-panel')).toBeTruthy();
    });
    expect(screen.getByTestId('goto-marts-btn')).toBeTruthy();
    expect(screen.getByTestId('goto-quality-btn')).toBeTruthy();
    expect(screen.getByTestId('goto-lineage-btn')).toBeTruthy();
    expect(screen.getByTestId('download-summary-btn')).toBeTruthy();
  });
});
