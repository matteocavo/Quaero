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

export interface RunRequest {
  question: string;
  source_url?: string;
  metric_column?: string | null;
  dimension_column?: string | null;
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
}

export interface ProjectCreateResponse {
  project: string;
  upload_id: string;
  dataset_name: string;
  question: string;
  metric_column: string | null;
  dimension_column: string | null;
  time_column: string | null;
  has_config: boolean;
  created_at: string;
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
  | 'lineage';
