import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type { QuestionInterpretation } from './types/api';

const UPLOAD = {
  upload_id: 'upload_abc123def456',
  original_filename: 'customer_revenue.csv',
  size_bytes: 2048,
  row_count: 8,
  column_count: 4,
  delimiter: ',',
  columns: [
    { name: 'order_year', normalized_name: 'order_year', dtype: 'integer', null_count: 0, unique_count: 4, sample_values: ['2021'] },
    { name: 'segment', normalized_name: 'segment', dtype: 'string', null_count: 0, unique_count: 3, sample_values: ['SMB'] },
    { name: 'revenue', normalized_name: 'revenue', dtype: 'float', null_count: 0, unique_count: 8, sample_values: ['14500.5'] },
  ],
  numeric_columns: ['revenue'],
  candidate_dimensions: ['segment', 'order_year'],
  candidate_time_columns: ['order_year'],
  created_at: '2026-10-28T10:00:00Z',
  session_id: 'session_test',
};

const base = (over: Partial<QuestionInterpretation>): QuestionInterpretation => ({
  question: 'q',
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
  ...over,
});

/** A small stand-in for the API. The interpretation it returns is chosen by the test; every call is recorded. */
function backend(interpretation: (body: Record<string, unknown>) => QuestionInterpretation) {
  const calls: { url: string; method: string; body: Record<string, unknown> | null }[] = [];
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method || 'GET').toUpperCase();
    const body = init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : null;
    calls.push({ url, method, body });
    const json = (data: unknown, status = 200) => new Response(JSON.stringify(data), { status });
    if (url === '/api/health') return json({ status: 'ok', version: '1.0.0' });
    if (url === '/api/projects' && method === 'GET') return json({ projects: [] });
    if (url === '/api/uploads' && method === 'GET') return json({ uploads: [UPLOAD] });
    if (url === '/api/storage') return json({ detail: 'n/a' }, 404);
    if (url.endsWith('/interpretation') && method === 'POST') return json(interpretation(body ?? {}));
    if (url === '/api/projects' && method === 'POST') {
      return json(
        {
          project: 'customer_revenue',
          upload_id: UPLOAD.upload_id,
          dataset_name: 'customer_revenue',
          question: body?.question,
          metric_column: body?.metric_column ?? null,
          dimension_column: body?.dimension_column ?? null,
          time_column: body?.time_column ?? null,
          aggregation: body?.aggregation ?? null,
          has_config: true,
          created_at: '2026-10-28T10:01:00Z',
        },
        201,
      );
    }
    if (url.endsWith('/run') && method === 'POST') {
      return json({ run_id: 'run_1', project: 'customer_revenue', status: 'QUEUED', message: 'ok' }, 202);
    }
    return json({ detail: 'Not mocked' }, 404);
  });
  return { fetchMock, calls, of: (suffix: string, method = 'POST') => calls.filter((c) => c.url.endsWith(suffix) && c.method === method) };
}

async function openWizard() {
  fireEvent.click(screen.getByTitle(/Configure environment connection/i));
  fireEvent.click(screen.getByRole('button', { name: /Local Operations/i }));
  fireEvent.click(screen.getAllByRole('button', { name: /Import & Profile Data/i })[0]);
  await screen.findByTestId('local-pipeline-wizard');
  await waitFor(() => expect((screen.getByTestId('local-metric-select') as HTMLSelectElement).value).toBe('revenue'));
}

const ask = (text: string) => fireEvent.change(screen.getByTestId('local-project-question-input'), { target: { value: text } });
const check = () => fireEvent.click(screen.getByTestId('check-interpretation-btn'));
const runButton = () => screen.getByTestId('create-and-run-btn') as HTMLButtonElement;

describe('how Quaero reads the question, before anything runs', () => {
  const originalFetch = globalThis.fetch;
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    cleanup();
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('shows the question, every choice and where it came from, and only then lets the run be confirmed', async () => {
    const api = backend(() =>
      base({
        question: 'Which segment has the highest total revenue?',
        aggregation: { value: 'sum', origin: 'auto_inferred' },
        ordering: { value: 'highest', origin: 'assumed_default' },
        dimension: { column: 'segment', origin: 'user_provided' },
        issues: [{ code: 'aggregation_assumed', severity: 'warning', message: 'The average is assumed.', suggestion: 'Choose the aggregation yourself.' }],
        summary: 'Total of revenue for each segment, highest first.',
      }),
    );
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    ask('Which segment has the highest total revenue?');

    expect(runButton().disabled).toBe(true); // nothing has been checked yet
    expect(api.of('/interpretation')).toHaveLength(0); // and nothing was checked on its own
    check();

    const panel = within(await screen.findByTestId('interpretation-panel'));
    expect(panel.getByTestId('interpretation-status').textContent).toBe('Ready');
    expect(panel.getByText(/Which segment has the highest total revenue\?/)).toBeTruthy();
    expect(panel.getByText('revenue')).toBeTruthy();
    expect(panel.getByText('Total (sum)')).toBeTruthy();
    expect(panel.getAllByText('read from the question').length).toBeGreaterThan(0);
    expect(panel.getByText('you chose')).toBeTruthy(); // the grouping, marked as the user's
    expect(panel.getByText('assumed')).toBeTruthy(); // the order, marked as a default
    expect(panel.getByTestId('interpretation-summary').textContent).toBe('Total of revenue for each segment, highest first.');
    expect(panel.getByText(/^Check:/).parentElement?.textContent).toContain('The average is assumed.');
    expect(runButton().disabled).toBe(false);

    // the preview sent exactly what is in the form, and created or ran nothing
    expect(api.of('/interpretation')[0].body).toEqual({
      question: 'Which segment has the highest total revenue?',
      metric_column: 'revenue',
      dimension_column: 'segment',
      aggregation: null,
      time_column: 'order_year',
    });
    expect(api.of('/api/projects')).toHaveLength(0);
    expect(api.of('/run')).toHaveLength(0);
  });

  it('refuses a question that cannot be answered, says what is not supported, and offers a question that can', async () => {
    const api = backend(() =>
      base({
        status: 'unsupported',
        question: 'What is the median revenue per segment?',
        aggregation: { value: null, origin: null },
        summary: null,
        issues: [
          {
            code: 'aggregation_not_supported',
            severity: 'unsupported',
            message: "'median' asks for a median, which Quaero does not calculate. Only average, sum and count are available.",
            suggestion: 'Ask for the total, the average or the number of records instead.',
          },
        ],
        suggested_questions: ['Which segment has the highest total revenue?'],
      }),
    );
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    ask('What is the median revenue per segment?');
    check();

    const panel = within(await screen.findByTestId('interpretation-panel'));
    expect(panel.getByTestId('interpretation-status').textContent).toBe('Cannot be answered as asked');
    expect(panel.getByText(/Nothing will be run/)).toBeTruthy();
    expect(panel.getByText(/'median' asks for a median/)).toBeTruthy();
    expect(panel.queryByTestId('interpretation-summary')).toBeNull(); // nothing is presented as the answer
    expect(runButton().disabled).toBe(true);

    fireEvent.click(panel.getByRole('button', { name: 'Which segment has the highest total revenue?' }));
    expect((screen.getByTestId('local-project-question-input') as HTMLTextAreaElement).value).toBe('Which segment has the highest total revenue?');
    expect(screen.queryByTestId('interpretation-panel')).toBeNull(); // the new question has to be checked again
    expect(runButton().disabled).toBe(true);
    expect(api.of('/run')).toHaveLength(0);
  });

  it('asks for what is missing instead of guessing', async () => {
    const api = backend(() =>
      base({
        status: 'needs_input',
        metric: { column: null, origin: null },
        dimension: { column: null, origin: null },
        aggregation: { value: null, origin: null },
        summary: null,
        issues: [
          { code: 'metric_unclear', severity: 'needs_input', message: 'The question does not say what to measure.', suggestion: 'Name the measure.' },
        ],
      }),
    );
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    ask('What patterns are there?');
    check();

    const panel = within(await screen.findByTestId('interpretation-panel'));
    expect(panel.getByTestId('interpretation-status').textContent).toBe('Needs more information');
    expect(panel.getAllByText('not determined').length).toBe(3); // measure, grouping and aggregation: no placeholder values
    expect(runButton().disabled).toBe(true);
  });

  it('an interpretation is only good for the exact inputs it was computed for', async () => {
    const api = backend(() => base({}));
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    ask('Which segment generates the highest average revenue?');
    check();
    await screen.findByTestId('interpretation-panel');
    expect(runButton().disabled).toBe(false);

    ask('Which segment generates the highest average revenue in 2023?'); // the question changed
    expect(screen.queryByTestId('interpretation-panel')).toBeNull();
    expect(screen.getByTestId('interpretation-stale').textContent).toMatch(/Check again before running/);
    expect(runButton().disabled).toBe(true);

    ask('Which segment generates the highest average revenue?'); // back to what was checked
    expect(screen.getByTestId('interpretation-panel')).toBeTruthy();
    expect(runButton().disabled).toBe(false);

    fireEvent.change(screen.getByTestId('local-aggregation-select'), { target: { value: 'sum' } }); // a selection changed
    expect(screen.getByTestId('interpretation-stale')).toBeTruthy();
    expect(runButton().disabled).toBe(true);
  });

  it('sends the chosen aggregation with the check, the project and the run, so it wins over the wording', async () => {
    const api = backend((body) =>
      base({ aggregation: { value: String(body.aggregation), origin: 'user_provided' } as QuestionInterpretation['aggregation'] }),
    );
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    expect(Array.from((screen.getByTestId('local-aggregation-select') as HTMLSelectElement).options).map((o) => o.value)).toEqual(['', 'average', 'sum', 'count']);

    ask('Revenue by segment');
    fireEvent.change(screen.getByTestId('local-aggregation-select'), { target: { value: 'sum' } });
    check();
    const panel = within(await screen.findByTestId('interpretation-panel'));
    expect(panel.getByText('you chose')).toBeTruthy();
    expect(api.of('/interpretation')[0].body?.aggregation).toBe('sum');

    fireEvent.click(runButton());
    await waitFor(() => expect(api.of('/run')).toHaveLength(1));
    expect(api.of('/api/projects')[0].body).toMatchObject({ aggregation: 'sum', metric_column: 'revenue', dimension_column: 'segment' });
    expect(api.of('/run')[0].body).toMatchObject({ aggregation: 'sum', metric_column: 'revenue', dimension_column: 'segment' });
  });

  it('does not start a run through any other path while the interpretation is not ready', async () => {
    const api = backend(() => base({ status: 'unsupported', summary: null }));
    globalThis.fetch = api.fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    ask('Why is revenue higher in some segments?');
    check();
    await screen.findByTestId('interpretation-panel');
    fireEvent.click(runButton()); // a disabled button: nothing happens
    expect(api.of('/run')).toHaveLength(0);
    expect(api.of('/api/projects')).toHaveLength(0);
  });

  it('the suggested questions of the wizard all say what to calculate', async () => {
    globalThis.fetch = backend(() => base({})).fetchMock as unknown as typeof fetch;
    render(<App />);
    await openWizard();
    const chips = screen.getAllByRole('button').filter((b) => /^Which|^What/.test(b.textContent ?? ''));
    expect(chips.map((c) => c.textContent)).toEqual([
      'Which segment generates the highest average revenue?',
      'What is the total revenue by segment?',
      'Which segment has the highest total revenue?',
    ]);
  });
});
