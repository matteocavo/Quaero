import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError, QuaeroApiClient } from './apiClient';

const fetchMock = vi.fn();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const json = (body: unknown, status = 200) =>
  Promise.resolve(
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json' },
    }),
  );

describe('Public Demo mode', () => {
  it('never touches the network', async () => {
    const client = new QuaeroApiClient('demo');
    await client.checkHealth();
    const projects = await client.listProjects();
    const name = projects.projects[0].project;
    await client.getProjectOverview(name);
    await client.listMarts(name);
    await client.getRunHistory(name);
    await client.getRunStatus(name, 'run-8f3a12bc-e179');
    await client.getDataQuality(name);
    await client.getAnomalies(name);
    await client.getLineage(name);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('refuses to start a pipeline run', async () => {
    const client = new QuaeroApiClient('demo');
    await expect(client.startRun('release_impact', 'q?')).rejects.toMatchObject({
      statusCode: 403,
    });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('serves demo fixtures only for known projects', async () => {
    const client = new QuaeroApiClient('demo');
    await expect(client.getProjectOverview('ghost')).rejects.toMatchObject({ statusCode: 404 });
  });
});

describe('Public Demo mart previews', () => {
  it('serves a bundled preview', async () => {
    const preview = await new QuaeroApiClient('demo').getMartPreview('release_impact', 'mart_release_impact');
    expect(preview.rows.length).toBeGreaterThan(0);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('says plainly when no preview is bundled instead of inventing columns or rows', async () => {
    const client = new QuaeroApiClient('demo');
    const marts = await client.listMarts('release_impact');
    const missing: string[] = [];
    for (const mart of marts.marts) {
      try {
        await client.getMartPreview('release_impact', mart.name);
      } catch (err) {
        expect(err).toBeInstanceOf(ApiError);
        expect((err as ApiError).statusCode).toBe(404);
        expect((err as ApiError).message).toContain('No preview is bundled');
        missing.push(mart.name);
      }
    }
    expect(missing.length).toBeGreaterThan(0);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe('Static public host (GitHub Pages)', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('never attempts a request in live mode when the build is demo-only', async () => {
    vi.stubEnv('VITE_DEMO_ONLY', 'true');
    const client = new QuaeroApiClient('live');
    await expect(client.checkHealth()).rejects.toMatchObject({ statusCode: 400 });
    await expect(client.listProjects()).rejects.toThrow('not available on this public site');
    await expect(client.startRun('p', 'q?')).rejects.toBeInstanceOf(ApiError);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('still works in demo mode', async () => {
    vi.stubEnv('VITE_DEMO_ONLY', 'true');
    const projects = await new QuaeroApiClient('demo').listProjects();
    expect(projects.projects.length).toBeGreaterThan(0);
  });
});

describe('Public Demo run status', () => {
  it('reports the fixture status of a run shown in the demo history', async () => {
    const client = new QuaeroApiClient('demo');
    const history = await client.getRunHistory('release_impact');
    for (const run of history.runs) {
      const status = await client.getRunStatus('release_impact', run.run_id);
      expect(status.run_id).toBe(run.run_id);
      expect(status.project).toBe('release_impact');
      expect(status.status).toBe(run.status);
      expect(status.error ?? null).toBe(run.error ?? null);
    }
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('keeps a failed demo run failed, with its error', async () => {
    const status = await new QuaeroApiClient('demo').getRunStatus('release_impact', 'run-4c1d22aa-a332');
    expect(status.status).toBe('FAILED');
    expect(status.error).toContain('Network timeout');
  });

  it('never fabricates a status for a run that is not in the fixtures', async () => {
    const client = new QuaeroApiClient('demo');
    await expect(client.getRunStatus('release_impact', 'run-does-not-exist')).rejects.toMatchObject({
      statusCode: 404,
    });
    await expect(client.getRunStatus('unknown_project', 'run-8f3a12bc-e179')).rejects.toMatchObject({
      statusCode: 404,
    });
    // a real run id of another project is not valid for this one
    await expect(
      client.getRunStatus('global_economic_indicators', 'run-8f3a12bc-e179'),
    ).rejects.toMatchObject({ statusCode: 404 });
  });

  it('does not invent result keys for demo runs', async () => {
    const status = await new QuaeroApiClient('demo').getRunStatus('release_impact', 'run-8f3a12bc-e179');
    expect(status.result_keys).toEqual([]);
  });

  it('stays consistent with the run history shown in the interface', async () => {
    const client = new QuaeroApiClient('demo');
    const { runs } = await client.getRunHistory('global_economic_indicators');
    const statuses = await Promise.all(
      runs.map((run) => client.getRunStatus('global_economic_indicators', run.run_id)),
    );
    expect(statuses.map((s) => s.status)).toEqual(runs.map((r) => r.status));
  });
});

describe('Local Operations (live) mode', () => {
  it('uses the same-origin /api proxy and sends no credentials', async () => {
    fetchMock.mockImplementation(() => json({ status: 'ok', version: '1.0.0' }));
    await new QuaeroApiClient('live').checkHealth();

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('/api/health');
    const names = Object.keys((init?.headers ?? {}) as Record<string, string>).map((h) =>
      h.toLowerCase(),
    );
    expect(names).not.toContain('x-api-key');
    expect(names).not.toContain('authorization');
  });

  it('calls the real backend endpoints with the right paths and methods', async () => {
    fetchMock.mockImplementation(() => json({}));
    const client = new QuaeroApiClient('live');
    await client.listProjects();
    await client.getProjectOverview('my proj');
    await client.listMarts('p');
    await client.getMartPreview('p', 'mart_a', 3, 25);
    await client.getRunHistory('p', 7);
    await client.getRunStatus('p', 'run-1');
    await client.getDataQuality('p');
    await client.getAnomalies('p');
    await client.getLineage('p');
    await client.startRun('p', 'Which year?', 'https://example.com/d.csv');

    const calls = fetchMock.mock.calls.map(([url, init]) => [init?.method ?? 'GET', url]);
    expect(calls).toEqual([
      ['GET', '/api/projects'],
      ['GET', '/api/projects/my%20proj/overview'],
      ['GET', '/api/projects/p/marts'],
      ['GET', '/api/projects/p/marts/mart_a?page=3&page_size=25'],
      ['GET', '/api/projects/p/runs?limit=7'],
      ['GET', '/api/projects/p/runs/run-1'],
      ['GET', '/api/projects/p/data-quality'],
      ['GET', '/api/projects/p/anomalies'],
      ['GET', '/api/projects/p/lineage'],
      ['POST', '/api/projects/p/run'],
    ]);
    expect(JSON.parse(fetchMock.mock.calls[9][1].body)).toEqual({
      question: 'Which year?',
      source_url: 'https://example.com/d.csv',
    });
  });

  it('omits source_url when it is empty', async () => {
    fetchMock.mockImplementation(() => json({ run_id: 'r1' }));
    await new QuaeroApiClient('live').startRun('p', 'q?');
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ question: 'q?' });
  });

  it('never falls back to demo data when the backend fails', async () => {
    fetchMock.mockImplementation(() => Promise.reject(new TypeError('Failed to fetch')));
    const client = new QuaeroApiClient('live');
    await expect(client.listProjects()).rejects.toMatchObject({ statusCode: 503 });
    await expect(client.getProjectOverview('release_impact')).rejects.toBeInstanceOf(ApiError);
    await expect(client.getDataQuality('release_impact')).rejects.toBeInstanceOf(ApiError);
    await expect(client.getLineage('release_impact')).rejects.toBeInstanceOf(ApiError);
  });

  it('maps HTTP errors to typed ApiError flags', async () => {
    const client = new QuaeroApiClient('live');
    const cases: [number, string][] = [
      [401, 'isAuthError'],
      [409, 'isConflict'],
      [422, 'isValidation'],
      [400, 'isValidation'],
    ];
    for (const [status, flag] of cases) {
      fetchMock.mockImplementationOnce(() => json({ detail: 'nope' }, status));
      await expect(client.listProjects()).rejects.toMatchObject({ [flag]: true, message: 'nope' });
    }
  });

  it('surfaces 404 and 503 details from the API', async () => {
    const client = new QuaeroApiClient('live');
    fetchMock.mockImplementationOnce(() => json({ detail: 'No lineage found' }, 404));
    await expect(client.getLineage('p')).rejects.toMatchObject({
      statusCode: 404,
      message: 'No lineage found',
    });
    fetchMock.mockImplementationOnce(() =>
      json({ detail: 'Run history is temporarily unavailable.' }, 503),
    );
    await expect(client.getRunHistory('p')).rejects.toMatchObject({ statusCode: 503 });
  });

  it('can switch back to demo and stop using the network', async () => {
    const client = new QuaeroApiClient('live');
    client.setMode('demo');
    await client.listProjects();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
