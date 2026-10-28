import {
  ProjectSummary,
  ProjectOverviewResponse,
  MartsResponse,
  MartPreviewResponse,
  DataQualityResponse,
  AnomalyResponse,
  LineageResponse,
  RunHistoryResponse,
  MetricDefinition,
} from '../types/api';

/**
 * Bundled demonstration fixtures shaped after Quaero's schemas and sample projects in the
 * repository. They are demo data, not the output of a live run.
 *
 * Provenance:
 * - release_impact: from projects/release_impact/metadata/final_summary_report.json,
 *   metrics_definitions.json, and sample_data/release_impact_sample.csv
 * - global_economic_indicators: from projects/global_economic_indicators/project_config.json,
 *   final_summary_report.json, and metadata
 */

export const DEMO_PROJECTS: ProjectSummary[] = [
  {
    project: 'release_impact',
    marts_count: 8,
    has_summary: true,
  },
  {
    project: 'global_economic_indicators',
    marts_count: 5,
    has_summary: true,
  },
];

export const DEMO_OVERVIEWS: Record<string, ProjectOverviewResponse> = {
  release_impact: {
    project: 'release_impact',
    project_mode: 'single_dataset',
    question: 'Which release years generate the strongest average streams?',
    primary_mart: 'mart_release_impact',
    primary_kpi: 'avg_streams',
    marts_count: 8,
    has_config: false,
    artifacts: {
      summary: true,
      data_quality: true,
      anomalies: true,
      lineage: true,
      metrics_definitions: true,
    },
    last_run: {
      run_id: 'run-8f3a12bc-e179',
      project: 'release_impact',
      status: 'COMPLETED',
      question: 'Which release years generate the strongest average streams?',
      source_url: 'sample_data/release_impact_sample.csv',
      created_at: '2026-03-14T14:41:35Z',
      started_at: '2026-03-14T14:41:36Z',
      completed_at: '2026-03-14T14:41:42Z',
      error: null,
      artifact_path: 'projects/release_impact/marts',
    },
    history_available: true,
  },
  global_economic_indicators: {
    project: 'global_economic_indicators',
    project_mode: 'multi_dataset',
    question: 'How do global GDP, inflation, and unemployment evolve over time and how are they related?',
    primary_mart: 'master_dataset',
    primary_kpi: 'gdp',
    marts_count: 5,
    has_config: true,
    artifacts: {
      summary: true,
      data_quality: false,
      anomalies: true,
      lineage: false,
      metrics_definitions: true,
    },
    last_run: {
      run_id: 'run-912b45ca-f341',
      project: 'global_economic_indicators',
      status: 'COMPLETED',
      question: 'How do global GDP, inflation, and unemployment evolve over time and how are they related?',
      source_url: 'projects/global_economic_indicators/project_config.json',
      created_at: '2026-03-10T11:20:10Z',
      started_at: '2026-03-10T11:20:12Z',
      completed_at: '2026-03-10T11:20:25Z',
      error: null,
      artifact_path: 'projects/global_economic_indicators/marts',
    },
    history_available: true,
  },
};

export const DEMO_RUN_HISTORY: Record<string, RunHistoryResponse> = {
  release_impact: {
    project: 'release_impact',
    total: 3,
    runs: [
      {
        run_id: 'run-8f3a12bc-e179',
        project: 'release_impact',
        status: 'COMPLETED',
        question: 'Which release years generate the strongest average streams?',
        source_url: 'sample_data/release_impact_sample.csv',
        created_at: '2026-03-14T14:41:35Z',
        started_at: '2026-03-14T14:41:36Z',
        completed_at: '2026-03-14T14:41:42Z',
        error: null,
      },
      {
        run_id: 'run-7b2a99ff-d001',
        project: 'release_impact',
        status: 'COMPLETED',
        question: 'How does catalog genre correlate with total stream distribution?',
        source_url: 'sample_data/release_impact_sample.csv',
        created_at: '2026-03-13T09:12:00Z',
        started_at: '2026-03-13T09:12:01Z',
        completed_at: '2026-03-13T09:12:07Z',
        error: null,
      },
      {
        run_id: 'run-4c1d22aa-a332',
        project: 'release_impact',
        status: 'FAILED',
        question: 'Benchmark provider retention vs release year volume',
        source_url: 'https://invalid-host-unreachable.test/raw.csv',
        created_at: '2026-03-12T16:04:10Z',
        started_at: '2026-03-12T16:04:11Z',
        completed_at: '2026-03-12T16:04:14Z',
        error: 'Network timeout: Unable to fetch source data from provided URL.',
      },
    ],
  },
  global_economic_indicators: {
    project: 'global_economic_indicators',
    total: 2,
    runs: [
      {
        run_id: 'run-912b45ca-f341',
        project: 'global_economic_indicators',
        status: 'COMPLETED',
        question: 'How do global GDP, inflation, and unemployment evolve over time and how are they related?',
        source_url: 'projects/global_economic_indicators/project_config.json',
        created_at: '2026-03-10T11:20:10Z',
        started_at: '2026-03-10T11:20:12Z',
        completed_at: '2026-03-10T11:20:25Z',
        error: null,
      },
      {
        run_id: 'run-1a98bc33-0091',
        project: 'global_economic_indicators',
        status: 'COMPLETED',
        question: 'Initial integration benchmark across 3 World Bank time series',
        source_url: 'projects/global_economic_indicators/project_config.json',
        created_at: '2026-03-08T18:05:00Z',
        started_at: '2026-03-08T18:05:02Z',
        completed_at: '2026-03-08T18:05:15Z',
        error: null,
      },
    ],
  },
};

export const DEMO_MARTS: Record<string, MartsResponse> = {
  release_impact: {
    project: 'release_impact',
    marts: [
      { name: 'mart_question_answer', path: 'projects/release_impact/marts/mart_question_answer.parquet' },
      { name: 'mart_top_entities', path: 'projects/release_impact/marts/mart_top_entities.parquet' },
      { name: 'mart_release_impact', path: 'projects/release_impact/marts/mart_release_impact.parquet' },
      { name: 'mart_provider_performance', path: 'projects/release_impact/marts/mart_provider_performance.parquet' },
      { name: 'mart_catalog_summary', path: 'projects/release_impact/marts/mart_catalog_summary.parquet' },
      { name: 'mart_distribution', path: 'projects/release_impact/marts/mart_distribution.parquet' },
      { name: 'mart_summary_stats', path: 'projects/release_impact/marts/mart_summary_stats.parquet' },
      { name: 'mart_time_trend', path: 'projects/release_impact/marts/mart_time_trend.parquet' },
    ],
  },
  global_economic_indicators: {
    project: 'global_economic_indicators',
    marts: [
      { name: 'master_dataset', path: 'projects/global_economic_indicators/marts/master_dataset.parquet' },
      { name: 'mart_gdp_trend', path: 'projects/global_economic_indicators/marts/mart_gdp_trend.parquet' },
      { name: 'mart_inflation_trend', path: 'projects/global_economic_indicators/marts/mart_inflation_trend.parquet' },
      { name: 'mart_unemployment_trend', path: 'projects/global_economic_indicators/marts/mart_unemployment_trend.parquet' },
      { name: 'mart_correlation_matrix', path: 'projects/global_economic_indicators/marts/mart_correlation_matrix.parquet' },
    ],
  },
};

export const DEMO_MART_PREVIEWS: Record<string, Record<string, MartPreviewResponse>> = {
  release_impact: {
    mart_question_answer: {
      project: 'release_impact',
      mart: 'mart_question_answer',
      columns: ['release_year', 'avg_streams', 'total_streams', 'record_count', 'rank'],
      rows: [
        { release_year: 2024, avg_streams: 450.0, total_streams: 450, record_count: 1, rank: 1 },
        { release_year: 2023, avg_streams: 210.0, total_streams: 210, record_count: 1, rank: 2 },
        { release_year: 2025, avg_streams: 190.0, total_streams: 190, record_count: 1, rank: 3 },
        { release_year: 2022, avg_streams: 180.0, total_streams: 180, record_count: 1, rank: 4 },
        { release_year: 2021, avg_streams: 120.0, total_streams: 120, record_count: 1, rank: 5 },
      ],
      total_rows: 5,
      page: 1,
      page_size: 100,
    },
    mart_provider_performance: {
      project: 'release_impact',
      mart: 'mart_provider_performance',
      columns: ['provider', 'total_streams', 'avg_streams', 'catalog_count', 'record_count'],
      rows: [
        { provider: 'Alpha', total_streams: 500, avg_streams: 166.7, catalog_count: 3, record_count: 3 },
        { provider: 'Gamma', total_streams: 450, avg_streams: 450.0, catalog_count: 1, record_count: 1 },
        { provider: 'Beta', total_streams: 180, avg_streams: 180.0, catalog_count: 1, record_count: 1 },
      ],
      total_rows: 3,
      page: 1,
      page_size: 100,
    },
    mart_catalog_summary: {
      project: 'release_impact',
      mart: 'mart_catalog_summary',
      columns: ['catalog', 'total_streams', 'avg_streams', 'record_count'],
      rows: [
        { catalog: 'Rock', total_streams: 660, avg_streams: 330.0, record_count: 2 },
        { catalog: 'Pop', total_streams: 300, avg_streams: 150.0, record_count: 2 },
        { catalog: 'Jazz', total_streams: 190, avg_streams: 190.0, record_count: 1 },
      ],
      total_rows: 3,
      page: 1,
      page_size: 100,
    },
    mart_release_impact: {
      project: 'release_impact',
      mart: 'mart_release_impact',
      columns: ['release_year', 'provider', 'catalog', 'streams', 'stream_zscore'],
      rows: [
        { release_year: 2024, provider: 'Gamma', catalog: 'Rock', streams: 450, stream_zscore: 1.68 },
        { release_year: 2023, provider: 'Alpha', catalog: 'Rock', streams: 210, stream_zscore: -0.15 },
        { release_year: 2025, provider: 'Alpha', catalog: 'Jazz', streams: 190, stream_zscore: -0.30 },
        { release_year: 2022, provider: 'Beta', catalog: 'Pop', streams: 180, stream_zscore: -0.38 },
        { release_year: 2021, provider: 'Alpha', catalog: 'Pop', streams: 120, stream_zscore: -0.84 },
      ],
      total_rows: 5,
      page: 1,
      page_size: 100,
    },
  },
  global_economic_indicators: {
    master_dataset: {
      project: 'global_economic_indicators',
      mart: 'master_dataset',
      columns: ['year', 'country', 'gdp_trillions', 'inflation_pct', 'unemployment_pct'],
      rows: [
        { year: 2023, country: 'World', gdp_trillions: 105.4, inflation_pct: 5.8, unemployment_pct: 5.1 },
        { year: 2022, country: 'World', gdp_trillions: 100.8, inflation_pct: 8.7, unemployment_pct: 5.3 },
        { year: 2021, country: 'World', gdp_trillions: 96.5, inflation_pct: 4.7, unemployment_pct: 6.2 },
        { year: 2020, country: 'World', gdp_trillions: 85.2, inflation_pct: 3.2, unemployment_pct: 6.6 },
        { year: 2019, country: 'World', gdp_trillions: 87.7, inflation_pct: 2.3, unemployment_pct: 5.4 },
        { year: 2018, country: 'World', gdp_trillions: 86.4, inflation_pct: 2.4, unemployment_pct: 5.4 },
        { year: 2017, country: 'World', gdp_trillions: 81.4, inflation_pct: 2.2, unemployment_pct: 5.6 },
      ],
      total_rows: 7,
      page: 1,
      page_size: 100,
    },
  },
};

export const DEMO_DATA_QUALITY: Record<string, DataQualityResponse> = {
  release_impact: {
    project: 'release_impact',
    reports: [
      {
        source: 'release_impact',
        generated_at: '2026-03-14T14:41:38Z',
        overall_score: 93.3,
        column_count: 4,
        columns: [
          {
            column: 'release_year',
            dtype: 'int64',
            completeness: 100.0,
            uniqueness: 100.0,
            outlier_flag: false,
            format_score: 100.0,
            dq_score: 100.0,
          },
          {
            column: 'provider',
            dtype: 'object',
            completeness: 100.0,
            uniqueness: 60.0,
            outlier_flag: false,
            format_score: 100.0,
            dq_score: 92.0,
          },
          {
            column: 'catalog',
            dtype: 'object',
            completeness: 100.0,
            uniqueness: 60.0,
            outlier_flag: false,
            format_score: 100.0,
            dq_score: 92.0,
          },
          {
            column: 'streams',
            dtype: 'int64',
            completeness: 100.0,
            uniqueness: 100.0,
            outlier_flag: true,
            format_score: 73.3,
            dq_score: 89.3,
          },
        ],
      },
    ],
  },
};

export const DEMO_ANOMALIES: Record<string, AnomalyResponse> = {
  release_impact: {
    project: 'release_impact',
    source: 'release_impact',
    version: 'v2',
    anomaly_row_count: 1,
    business_summary: '1 row (20.0% of total) flagged for unusual metric amplitude in `streams` (IQR test threshold exceeded).',
    anomalies: [
      {
        column: 'streams',
        method: 'iqr_outlier_detection',
        threshold: 'Upper fence > 390.0',
        anomaly_count: 1,
        example_values: [450],
        frequency: 0.2,
        impact: 'high',
        suggested_action: 'Investigate outlier value 450 (Gamma / 2024). Verify if this reflects viral release promotion or upstream telemetry error.',
      },
      {
        column: 'provider',
        method: 'frequency_imbalance',
        threshold: 'Imbalance ratio > 3.0',
        anomaly_count: 0,
        example_values: [],
        frequency: 0.0,
        impact: 'low',
        suggested_action: 'Distribution balanced across major content providers.',
      },
    ],
  },
  global_economic_indicators: {
    project: 'global_economic_indicators',
    source: 'global_economic_indicators',
    version: 'v1',
    anomaly_row_count: 2,
    business_summary: null,
    anomalies: [
      {
        column: 'inflation_pct',
        method: 'z_score',
        threshold: '3.0 sigma',
        anomaly_count: 1,
        example_values: [8.7],
        frequency: null,
        impact: null,
        suggested_action: null,
      },
      {
        column: 'gdp_trillions',
        method: 'historical_delta',
        threshold: 'Delta < -5%',
        anomaly_count: 1,
        example_values: [85.2],
        frequency: null,
        impact: null,
        suggested_action: null,
      },
    ],
  },
};

export const DEMO_LINEAGE: Record<string, LineageResponse> = {
  release_impact: {
    project: 'release_impact',
    source: 'release_impact',
    generated_at: '2026-03-14T14:41:40Z',
    node_count: 16,
    edge_count: 19,
    nodes: [
      // Raw
      { id: 'raw::release_year', layer: 'raw', name: 'release_year' },
      { id: 'raw::provider', layer: 'raw', name: 'provider' },
      { id: 'raw::catalog', layer: 'raw', name: 'catalog' },
      { id: 'raw::streams', layer: 'raw', name: 'streams' },
      // Staging
      { id: 'staging::release_year', layer: 'staging', name: 'release_year', dtype: 'int64' },
      { id: 'staging::provider', layer: 'staging', name: 'provider', dtype: 'string' },
      { id: 'staging::catalog', layer: 'staging', name: 'catalog', dtype: 'string' },
      { id: 'staging::streams', layer: 'staging', name: 'streams', dtype: 'int64' },
      // Marts
      { id: 'mart::mart_release_impact::release_year', layer: 'mart', name: 'release_year', mart: 'mart_release_impact' },
      { id: 'mart::mart_release_impact::streams', layer: 'mart', name: 'streams', mart: 'mart_release_impact' },
      { id: 'mart::mart_release_impact::avg_streams', layer: 'mart', name: 'avg_streams', mart: 'mart_release_impact' },
      { id: 'mart::mart_question_answer::release_year', layer: 'mart', name: 'release_year', mart: 'mart_question_answer' },
      { id: 'mart::mart_question_answer::avg_streams', layer: 'mart', name: 'avg_streams', mart: 'mart_question_answer' },
      { id: 'mart::mart_provider_performance::provider', layer: 'mart', name: 'provider', mart: 'mart_provider_performance' },
      { id: 'mart::mart_provider_performance::total_streams', layer: 'mart', name: 'total_streams', mart: 'mart_provider_performance' },
      { id: 'mart::mart_catalog_summary::catalog', layer: 'mart', name: 'catalog', mart: 'mart_catalog_summary' },
    ],
    edges: [
      // Raw -> Staging
      { from: 'raw::release_year', to: 'staging::release_year', type: 'type_cast' },
      { from: 'raw::provider', to: 'staging::provider', type: 'normalized' },
      { from: 'raw::catalog', to: 'staging::catalog', type: 'normalized' },
      { from: 'raw::streams', to: 'staging::streams', type: 'type_cast' },
      // Staging -> Mart release_impact
      { from: 'staging::release_year', to: 'mart::mart_release_impact::release_year', type: 'passthrough' },
      { from: 'staging::streams', to: 'mart::mart_release_impact::streams', type: 'passthrough' },
      { from: 'staging::streams', to: 'mart::mart_release_impact::avg_streams', type: 'aggregated' },
      // Staging -> Mart question_answer
      { from: 'staging::release_year', to: 'mart::mart_question_answer::release_year', type: 'passthrough' },
      { from: 'staging::streams', to: 'mart::mart_question_answer::avg_streams', type: 'aggregated' },
      // Staging -> Provider mart
      { from: 'staging::provider', to: 'mart::mart_provider_performance::provider', type: 'passthrough' },
      { from: 'staging::streams', to: 'mart::mart_provider_performance::total_streams', type: 'aggregated' },
      // Staging -> Catalog mart
      { from: 'staging::catalog', to: 'mart::mart_catalog_summary::catalog', type: 'passthrough' },
    ],
  },
};

export const DEMO_METRICS_DEFINITIONS: Record<string, MetricDefinition[]> = {
  release_impact: [
    {
      metric_name: 'catalog_summary_total_streams',
      source_mart: 'mart_catalog_summary',
      source_column: 'total_streams',
      source_metric_column: 'streams',
      formula_description: 'Total of streams in mart_catalog_summary.',
      default_aggregation: 'sum',
      unit: 'streams',
    },
    {
      metric_name: 'catalog_summary_avg_streams',
      source_mart: 'mart_catalog_summary',
      source_column: 'avg_streams',
      source_metric_column: 'streams',
      formula_description: 'Average of streams in mart_catalog_summary.',
      default_aggregation: 'average',
      unit: 'streams/release',
    },
    {
      metric_name: 'provider_performance_avg_streams',
      source_mart: 'mart_provider_performance',
      source_column: 'avg_streams',
      source_metric_column: 'streams',
      formula_description: 'Average of streams per provider in mart_provider_performance.',
      default_aggregation: 'average',
      unit: 'streams',
    },
    {
      metric_name: 'question_answer_avg_streams',
      source_mart: 'mart_question_answer',
      source_column: 'avg_streams',
      source_metric_column: 'streams',
      formula_description: 'Primary answer KPI: Average streams grouped by release_year.',
      default_aggregation: 'average',
      unit: 'streams',
    },
  ],
  global_economic_indicators: [
    {
      metric_name: 'world_gdp_mean',
      source_mart: 'master_dataset',
      source_column: 'gdp_trillions',
      source_metric_column: 'Value',
      formula_description: 'Annual global GDP average across reporting territories.',
      default_aggregation: 'average',
      unit: 'USD Trillions',
    },
    {
      metric_name: 'world_inflation_headline',
      source_mart: 'master_dataset',
      source_column: 'inflation_pct',
      source_metric_column: 'Value',
      formula_description: 'Annual consumer price index percentage change.',
      default_aggregation: 'average',
      unit: '% annual',
    },
  ],
};
