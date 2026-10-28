import {
  HealthResponse,
  ProjectsResponse,
  ProjectDetailResponse,
  ProjectOverviewResponse,
  ProjectCreateRequest,
  ProjectCreateResponse,
  MartsResponse,
  MartPreviewResponse,
  RunAcceptedResponse,
  RunStatusResponse,
  RunHistoryResponse,
  DataQualityResponse,
  AnomalyResponse,
  LineageResponse,
  UploadResponse,
  UploadsResponse,
  StorageStatusResponse,
  LifecyclePolicy,
  LifecyclePolicyResponse,
  CleanupRequest,
  CleanupResponse,
  Aggregation,
  InterpretationRequest,
  QuestionInterpretation,
  AppMode,
} from '../types/api';
import { LOCAL_UNAVAILABLE_MESSAGE, localOperationsAvailable } from '../utils/runtime';
import {
  DEMO_PROJECTS,
  DEMO_OVERVIEWS,
  DEMO_RUN_HISTORY,
  DEMO_MARTS,
  DEMO_MART_PREVIEWS,
  DEMO_DATA_QUALITY,
  DEMO_ANOMALIES,
  DEMO_LINEAGE,
} from '../data/demoData';

export class ApiError extends Error {
  constructor(
    public override message: string,
    public statusCode?: number,
    public isAuthError: boolean = false,
    public isConflict: boolean = false,
    public isValidation: boolean = false,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export class QuaeroApiClient {
  private mode: AppMode;
  // Always use same-origin proxy in live mode; no API keys in frontend JS.
  private baseUrl: string = '/api';

  constructor(mode: AppMode = 'demo') {
    this.mode = mode;
  }

  public setMode(mode: AppMode) {
    this.mode = mode;
  }

  public getMode(): AppMode {
    return this.mode;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    if (this.mode === 'demo') {
      throw new ApiError('Cannot make live network requests in demo mode', 400);
    }
    // Defence in depth: on a static host there is no API, so never attempt a request.
    if (!localOperationsAvailable()) {
      throw new ApiError(LOCAL_UNAVAILABLE_MESSAGE, 400);
    }

    const cleanPath = path.startsWith('/') ? path : `/${path}`;
    const url = `${this.baseUrl}${cleanPath}`;

    const isFormData = typeof FormData !== 'undefined' && options.body instanceof FormData;
    const headers: Record<string, string> = {
      ...(isFormData ? {} : { 'Content-Type': 'application/json' }),
      ...((options.headers as Record<string, string>) || {}),
    };

    try {
      const response = await fetch(url, {
        ...options,
        headers,
      });

      if (!response.ok) {
        let detail = `HTTP ${response.status}: ${response.statusText}`;
        try {
          const body = await response.json();
          if (body && typeof body.detail === 'string') {
            detail = body.detail;
          } else if (body && body.detail) {
            detail = JSON.stringify(body.detail);
          }
        } catch {
          // ignore json parse error
        }

        const isAuth = response.status === 401;
        const isConflict = response.status === 409;
        const isValidation = response.status === 400 || response.status === 422;

        throw new ApiError(detail, response.status, isAuth, isConflict, isValidation);
      }

      return (await response.json()) as T;
    } catch (err: any) {
      if (err instanceof ApiError) {
        throw err;
      }
      throw new ApiError(
        `Unable to reach Quaero FastAPI via proxy (${this.baseUrl}). Verify that 'uvicorn api.main:app' is running on port 8000.`,
        503,
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Endpoints
  // ---------------------------------------------------------------------------

  public async checkHealth(): Promise<HealthResponse> {
    if (this.mode === 'demo') {
      return { status: 'ok', version: 'v0.7.0 (Public Demo Sandbox)' };
    }
    return this.request<HealthResponse>('/health');
  }

  public async listProjects(): Promise<ProjectsResponse> {
    if (this.mode === 'demo') {
      return { projects: DEMO_PROJECTS };
    }
    return this.request<ProjectsResponse>('/projects');
  }

  public async uploadCsv(file: File): Promise<UploadResponse> {
    if (this.mode === 'demo') {
      throw new ApiError(
        'Server CSV uploads are disabled in Public Demo mode. Use the in-browser workspace or switch to Local Operations mode.',
        403,
      );
    }
    const formData = new FormData();
    formData.append('file', file);
    return this.request<UploadResponse>('/uploads', {
      method: 'POST',
      body: formData,
    });
  }

  public async listUploads(): Promise<UploadsResponse> {
    if (this.mode === 'demo') {
      return { uploads: [] };
    }
    return this.request<UploadsResponse>('/uploads');
  }

  public async getUpload(uploadId: string): Promise<UploadResponse> {
    if (this.mode === 'demo') {
      throw new ApiError(`Upload '${uploadId}' is not available in Public Demo mode.`, 404);
    }
    return this.request<UploadResponse>(`/uploads/${encodeURIComponent(uploadId)}`);
  }

  public async getStorage(): Promise<StorageStatusResponse> {
    if (this.mode === 'demo') {
      throw new ApiError('Local storage is only available in Local Operations mode.', 403);
    }
    return this.request<StorageStatusResponse>('/storage');
  }

  public async setLifecyclePolicy(policy: LifecyclePolicy): Promise<LifecyclePolicyResponse> {
    if (this.mode === 'demo') {
      throw new ApiError('Local storage is only available in Local Operations mode.', 403);
    }
    return this.request<LifecyclePolicyResponse>('/storage/policy', {
      method: 'POST',
      body: JSON.stringify({ lifecycle_policy: policy }),
    });
  }

  /** Permanently deletes temporary data. The server refuses unless `confirm` is true. */
  public async cleanupStorage(payload: CleanupRequest): Promise<CleanupResponse> {
    if (this.mode === 'demo') {
      throw new ApiError('Local storage is only available in Local Operations mode.', 403);
    }
    return this.request<CleanupResponse>('/storage/cleanup', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  public async createProject(payload: ProjectCreateRequest): Promise<ProjectCreateResponse> {
    if (this.mode === 'demo') {
      throw new ApiError(
        'Creating backend projects is disabled in Public Demo mode. Switch to Local Operations mode with a running Quaero backend.',
        403,
      );
    }
    return this.request<ProjectCreateResponse>('/projects', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  public async getProjectOverview(project: string): Promise<ProjectOverviewResponse> {
    if (this.mode === 'demo') {
      const overview = DEMO_OVERVIEWS[project];
      if (!overview) {
        throw new ApiError(`Project '${project}' not found in demo fixtures`, 404);
      }
      return overview;
    }
    return this.request<ProjectOverviewResponse>(`/projects/${encodeURIComponent(project)}/overview`);
  }

  public async getProjectSummary(project: string): Promise<ProjectDetailResponse> {
    if (this.mode === 'demo') {
      return {
        project,
        summary: DEMO_OVERVIEWS[project] || {},
      };
    }
    return this.request<ProjectDetailResponse>(`/projects/${encodeURIComponent(project)}/summary`);
  }

  public async listMarts(project: string): Promise<MartsResponse> {
    if (this.mode === 'demo') {
      const marts = DEMO_MARTS[project];
      return marts || { project, marts: [] };
    }
    return this.request<MartsResponse>(`/projects/${encodeURIComponent(project)}/marts`);
  }

  public async getMartPreview(
    project: string,
    mart: string,
    page: number = 1,
    pageSize: number = 100,
  ): Promise<MartPreviewResponse> {
    if (this.mode === 'demo') {
      const projectMarts = DEMO_MART_PREVIEWS[project];
      if (projectMarts && projectMarts[mart]) {
        return projectMarts[mart];
      }
      // No placeholder columns or rows: say plainly that this demo mart has no bundled preview.
      throw new ApiError(
        `No preview is bundled for '${mart}' in the public demo. Run Quaero locally to browse the real mart.`,
        404,
      );
    }
    return this.request<MartPreviewResponse>(
      `/projects/${encodeURIComponent(project)}/marts/${encodeURIComponent(mart)}?page=${page}&page_size=${pageSize}`,
    );
  }

  public async getRunHistory(project: string, limit: number = 50): Promise<RunHistoryResponse> {
    if (this.mode === 'demo') {
      const history = DEMO_RUN_HISTORY[project];
      return history || { project, total: 0, runs: [] };
    }
    return this.request<RunHistoryResponse>(
      `/projects/${encodeURIComponent(project)}/runs?limit=${limit}`,
    );
  }

  public async getRunStatus(project: string, runId: string): Promise<RunStatusResponse> {
    if (this.mode === 'demo') {
      // Public Demo executes nothing: report only the runs shown in the demo history, with the
      // status stored in the fixture. Never invent a status or result for an unknown run.
      const item = DEMO_RUN_HISTORY[project]?.runs.find((run) => run.run_id === runId);
      if (!item) {
        throw new ApiError(
          `Run '${runId}' is not part of the demo fixtures. Public Demo does not execute pipelines.`,
          404,
        );
      }
      return {
        run_id: item.run_id,
        project: item.project,
        status: item.status,
        error: item.error ?? null,
        result_keys: [],
      };
    }
    return this.request<RunStatusResponse>(
      `/projects/${encodeURIComponent(project)}/runs/${encodeURIComponent(runId)}`,
    );
  }

  public async startRun(
    project: string,
    question: string,
    sourceUrl?: string,
    metricColumn?: string,
    dimensionColumn?: string,
    aggregation?: Aggregation,
  ): Promise<RunAcceptedResponse> {
    if (this.mode === 'demo') {
      throw new ApiError(
        'Pipeline execution is disabled in Public Demo mode. Use the interactive browser data import & profiler or switch to Local Operations mode with a running Quaero backend.',
        403,
      );
    }
    return this.request<RunAcceptedResponse>(`/projects/${encodeURIComponent(project)}/run`, {
      method: 'POST',
      body: JSON.stringify({
        question,
        ...(sourceUrl ? { source_url: sourceUrl } : {}),
        ...(metricColumn !== undefined ? { metric_column: metricColumn } : {}),
        ...(dimensionColumn !== undefined ? { dimension_column: dimensionColumn } : {}),
        ...(aggregation !== undefined ? { aggregation } : {}),
      }),
    });
  }

  /** Say how a question will be answered on an upload, or why it cannot be. Nothing is created or run. */
  public async interpretQuestion(
    uploadId: string,
    payload: InterpretationRequest,
  ): Promise<QuestionInterpretation> {
    if (this.mode === 'demo') {
      throw new ApiError('Question interpretation for uploads is only available in Local Operations mode.', 403);
    }
    return this.request<QuestionInterpretation>(`/uploads/${encodeURIComponent(uploadId)}/interpretation`, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  public async getDataQuality(project: string): Promise<DataQualityResponse> {
    if (this.mode === 'demo') {
      const dq = DEMO_DATA_QUALITY[project];
      if (!dq) {
        throw new ApiError(`Data quality report not generated for project '${project}'.`, 404);
      }
      return dq;
    }
    return this.request<DataQualityResponse>(`/projects/${encodeURIComponent(project)}/data-quality`);
  }

  public async getAnomalies(project: string): Promise<AnomalyResponse> {
    if (this.mode === 'demo') {
      const anomalies = DEMO_ANOMALIES[project];
      if (!anomalies) {
        throw new ApiError(`Anomaly report not generated for project '${project}'.`, 404);
      }
      return anomalies;
    }
    return this.request<AnomalyResponse>(`/projects/${encodeURIComponent(project)}/anomalies`);
  }

  public async getLineage(project: string): Promise<LineageResponse> {
    if (this.mode === 'demo') {
      const lineage = DEMO_LINEAGE[project];
      if (!lineage) {
        throw new ApiError(`Lineage graph not generated for project '${project}'.`, 404);
      }
      return lineage;
    }
    return this.request<LineageResponse>(`/projects/${encodeURIComponent(project)}/lineage`);
  }
}
