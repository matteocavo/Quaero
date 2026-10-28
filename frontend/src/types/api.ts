/**
 * Type definitions directly aligned with Quaero FastAPI Pydantic schemas (api/schemas.py).
 */

export type RunStatus = 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED';

export interface HealthResponse {
  status: string;
  version: string;
}

export interface ProjectSummary {
  project: string;
  marts_count: number;
  has_summary: boolean;
}

export interface ProjectsResponse {
  projects: ProjectSummary[];
}

export interface ProjectDetailResponse {
  project: string;
  summary: Record<string, any>;
}

export interface MartInfo {
  name: string;
  path: string;
}

export interface MartsResponse {
  project: string;
  marts: MartInfo[];
}

export interface MartPreviewResponse {
  project: string;
  mart: string;
  columns: string[];
  rows: Record<string, any>[];
  total_rows: number;
  page: number;
  page_size: number;
}

/** The aggregations the pipeline calculates. Anything else (median, minimum, ...) is refused. */
export type Aggregation = 'average' | 'sum' | 'count';

export interface RunRequest {
  question: string;
  source_url?: string;
  metric_column?: string | null;
  dimension_column?: string | null;
  aggregation?: Aggregation | null;
}

export interface RunAcceptedResponse {
  run_id: string;
  project: string;
  status: RunStatus;
  message: string;
}

export interface RunStatusResponse {
  run_id: string;
  project: string;
  status: RunStatus;
  error?: string | null;
  result_keys: string[];
}

export interface RunHistoryItem {
  run_id: string;
  project: string;
  status: RunStatus;
  question?: string | null;
  source_url?: string | null;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  error?: string | null;
  artifact_path?: string | null;
}

export interface RunHistoryResponse {
  project: string;
  runs: RunHistoryItem[];
  total: number;
}

export interface ArtifactAvailability {
  summary: boolean;
  data_quality: boolean;
  anomalies: boolean;
  lineage: boolean;
  metrics_definitions: boolean;
}

export interface ProjectOverviewResponse {
  project: string;
  project_mode?: 'single_dataset' | 'multi_dataset' | string | null;
  question?: string | null;
  primary_mart?: string | null;
  primary_kpi?: string | null;
  marts_count: number;
  has_config: boolean;
  artifacts: ArtifactAvailability;
  last_run?: RunHistoryItem | null;
  history_available: boolean;
}

export interface DataQualityColumn {
  column: string;
  dtype?: string | null;
  completeness: number; // 0 - 100
  uniqueness: number;   // 0 - 100
  outlier_flag: boolean;
  format_score: number; // 0 - 100
  dq_score: number;     // 0 - 100
}

export interface DataQualityReport {
  source: string;
  generated_at?: string | null;
  overall_score: number;
  column_count: number;
  columns: DataQualityColumn[];
}

export interface DataQualityResponse {
  project: string;
  reports: DataQualityReport[];
}

export interface AnomalyItem {
  column: string;
  method: string;
  threshold?: string | null;
  anomaly_count: number;
  example_values: any[];
  frequency?: number | null; // v2 only: fraction of total rows
  impact?: 'low' | 'medium' | 'high' | string | null;
  suggested_action?: string | null;
}

export interface AnomalyResponse {
  project: string;
  source?: string | null;
  version: 'v1' | 'v2' | string;
  anomaly_row_count: number;
  business_summary?: string | null;
  anomalies: AnomalyItem[];
}

export interface LineageNode {
  id: string;
  layer: 'raw' | 'staging' | 'mart' | string;
  name: string;
  dtype?: string | null;
  mart?: string | null;
}

export interface LineageEdge {
  from: string;
  to: string;
  type: 'passthrough' | 'normalized' | 'type_cast' | 'aggregated' | string;
}

export interface LineageResponse {
  project: string;
  source?: string | null;
  generated_at?: string | null;
  node_count: number;
  edge_count: number;
  nodes: LineageNode[];
  edges: LineageEdge[];
}

// ---------------------------------------------------------------------------
// Local CSV Uploads & Project Creation (FastAPI — Local Operations mode)
// ---------------------------------------------------------------------------

export interface UploadColumnInfo {
  name: string;
  normalized_name: string;
  dtype: string;
  null_count: number;
  unique_count: number;
  sample_values: string[];
}

export interface UploadResponse {
  upload_id: string;
  original_filename: string;
  size_bytes: number;
  row_count: number;
  column_count: number;
  delimiter: string;
  columns: UploadColumnInfo[];
  numeric_columns: string[];
  candidate_dimensions: string[];
  candidate_time_columns: string[];
  created_at: string;
  session_id: string | null;
}

export interface UploadsResponse {
  uploads: UploadResponse[];
}

export interface ProjectCreateRequest {
  project: string;
  upload_id: string;
  question: string;
  dataset_name?: string | null;
  metric_column?: string | null;
  dimension_column?: string | null;
  time_column?: string | null;
  aggregation?: Aggregation | null;
}

export interface ProjectCreateResponse {
  project: string;
  upload_id: string;
  dataset_name: string;
  question: string;
  metric_column: string | null;
  dimension_column: string | null;
  time_column: string | null;
  aggregation: Aggregation | null;
  has_config: boolean;
  created_at: string;
}

// ---------------------------------------------------------------------------
// Question interpretation: what Quaero will do with a question, or why it will not
// ---------------------------------------------------------------------------

/** Where a choice came from: made by the user, read from the question or columns, or assumed because unstated. */
export type SelectionOrigin = 'user_provided' | 'auto_inferred' | 'assumed_default';

export interface InterpretationRequest {
  question: string;
  metric_column?: string | null;
  dimension_column?: string | null;
  aggregation?: Aggregation | null;
  time_column?: string | null;
}

export interface InterpretedColumn {
  column: string | null;
  origin: SelectionOrigin | null;
}

export interface InterpretedValue {
  value: string | null;
  origin: SelectionOrigin | null;
}

export interface InterpretedTime {
  column: string | null;
  origin: SelectionOrigin | null;
  /** `exact_values`: groups are the exact values of a date column, not periods. */
  grouping: 'exact_values' | null;
}

export interface InterpretedFilters {
  /** True when the question asks for a period or a value filter (which is then refused). */
  requested: boolean;
}

export interface InterpretationIssue {
  code: string;
  /** `unsupported` and `needs_input` block the run; `warning` and `note` inform. */
  severity: 'unsupported' | 'needs_input' | 'warning' | 'note';
  message: string;
  suggestion: string | null;
}

export interface QuestionInterpretation {
  question: string;
  language: 'en' | 'it';
  status: 'ready' | 'needs_input' | 'unsupported';
  metric: InterpretedColumn;
  dimension: InterpretedColumn;
  aggregation: InterpretedValue;
  ordering: InterpretedValue;
  time: InterpretedTime;
  filters: InterpretedFilters;
  issues: InterpretationIssue[];
  summary: string | null;
  suggested_questions: string[];
}

// ---------------------------------------------------------------------------
// Local data lifecycle (FastAPI, Local Operations mode only)
// ---------------------------------------------------------------------------

export type LifecyclePolicy = 'ask_on_exit' | 'auto_cleanup' | 'keep_files';
export type CleanupScope = 'current_session' | 'orphaned' | 'single_upload' | 'project_artifacts';

export interface StorageUploadItem {
  upload_id: string;
  original_filename: string | null;
  size_bytes: number;
  created_at: string | null;
  /** `saved` = a project uses it; otherwise temporary: made by this session or left by an earlier one. */
  state: 'current_session' | 'orphaned' | 'saved';
  linked_projects: string[];
  incomplete: boolean;
}

export interface StorageProjectItem {
  project: string;
  kind: 'upload_project' | 'preexisting';
  upload_id: string | null;
  config_bytes: number;
  artifacts_bytes: number;
  artifact_files: number;
  marts_count: number;
  can_clear_artifacts: boolean;
}

export interface StorageTotals {
  temporary_uploads_bytes: number;
  saved_uploads_bytes: number;
  project_configs_bytes: number;
  artifacts_bytes: number;
  total_bytes: number;
}

export interface StorageStatusResponse {
  session_id: string;
  lifecycle_policy: LifecyclePolicy;
  totals: StorageTotals;
  current_session_uploads: number;
  orphaned_uploads: number;
  uploads: StorageUploadItem[];
  projects: StorageProjectItem[];
}

export interface LifecyclePolicyRequest {
  lifecycle_policy: LifecyclePolicy;
}

export interface LifecyclePolicyResponse {
  lifecycle_policy: LifecyclePolicy;
}

export interface CleanupRequest {
  scope: CleanupScope;
  confirm: boolean;
  target_id?: string | null;
}

export interface CleanupSkipped {
  id: string;
  reason: string;
}

export interface CleanupResponse {
  scope: CleanupScope;
  deleted: string[];
  freed_bytes: number;
  skipped: CleanupSkipped[];
  message: string;
}

export interface MetricDefinition {
  metric_name: string;
  source_mart: string;
  source_column: string;
  source_metric_column?: string | null;
  formula_description: string;
  default_aggregation: string;
  unit?: string | null;
}

export type AppMode = 'demo' | 'live';

export type NavigationTab =
  | 'home'
  | 'import'
  | 'analyze'
  | 'overview'
  | 'explorer'
  | 'operations'
  | 'marts'
  | 'quality'
  | 'lineage'
  | 'storage';
