import React, { useState } from 'react';
import {
  FolderSearch,
  FileText,
  Database,
  Tag,
  Key,
  Layers,
  CheckCircle2,
  AlertCircle,
  Code,
  ArrowRight,
  ExternalLink,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { DEMO_METRICS_DEFINITIONS } from '../data/demoData';

const LiveOnlyNote: React.FC<{ what: string }> = ({ what }) => (
  <div className="rounded-xl border border-slate-200 bg-white p-5 text-xs text-slate-600 shadow-2xs dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300">
    {what} is not exposed by the Quaero API yet, so it is not shown in Local Operations mode (demo fixtures are never used as a fallback).
  </div>
);

export const ProjectExplorerView: React.FC = () => {
  const { currentOverview, selectedProject, setActiveTab, mode } = useApp();
  const [activeTabSub, setActiveTabSub] = useState<'details' | 'metrics' | 'config'>('details');

  const metrics = mode === 'demo' ? DEMO_METRICS_DEFINITIONS[selectedProject] || [] : [];

  return (
    <div className="space-y-6">
      {/* Header bar */}
      <div className="flex flex-col justify-between space-y-3 sm:flex-row sm:items-center sm:space-y-0">
        <div>
          <div className="flex items-center space-x-2">
            <h2 className="text-xl font-bold tracking-tight text-slate-900 dark:text-white">
              Project Explorer: <span className="text-sky-600 dark:text-sky-400 font-mono">{selectedProject}</span>
            </h2>
            <span className="rounded-full bg-sky-100 px-2 py-0.5 text-xs font-semibold text-sky-800 dark:bg-sky-950 dark:text-sky-300">
              {currentOverview?.project_mode || 'single_dataset'}
            </span>
          </div>
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            Inspect operational schemas, metric definitions, and pipeline artifacts.
          </p>
        </div>

        {/* Quick jump to analytics views */}
        <div className="flex space-x-2">
          <button
            onClick={() => setActiveTab('marts')}
            className="flex items-center space-x-1.5 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 shadow-2xs hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
          >
            <span>View Marts</span>
            <ArrowRight className="h-3 w-3" />
          </button>
          <button
            onClick={() => setActiveTab('lineage')}
            className="flex items-center space-x-1.5 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 shadow-2xs hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
          >
            <span>Lineage DAG</span>
            <ArrowRight className="h-3 w-3" />
          </button>
        </div>
      </div>

      {/* Sub Tabs */}
      <div className="flex space-x-1 border-b border-slate-200 dark:border-slate-800">
        <button
          onClick={() => setActiveTabSub('details')}
          className={`flex items-center space-x-2 border-b-2 px-4 py-2.5 text-xs font-semibold transition ${
            activeTabSub === 'details'
              ? 'border-sky-600 text-sky-600 dark:border-sky-400 dark:text-sky-400'
              : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'
          }`}
        >
          <FileText className="h-3.5 w-3.5" />
          <span>Overview & Summary</span>
        </button>
        <button
          onClick={() => setActiveTabSub('metrics')}
          className={`flex items-center space-x-2 border-b-2 px-4 py-2.5 text-xs font-semibold transition ${
            activeTabSub === 'metrics'
              ? 'border-sky-600 text-sky-600 dark:border-sky-400 dark:text-sky-400'
              : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'
          }`}
        >
          <Tag className="h-3.5 w-3.5" />
          <span>Semantic Metric Definitions ({metrics.length})</span>
        </button>
        <button
          onClick={() => setActiveTabSub('config')}
          className={`flex items-center space-x-2 border-b-2 px-4 py-2.5 text-xs font-semibold transition ${
            activeTabSub === 'config'
              ? 'border-sky-600 text-sky-600 dark:border-sky-400 dark:text-sky-400'
              : 'border-transparent text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200'
          }`}
        >
          <Code className="h-3.5 w-3.5" />
          <span>Configuration Specs</span>
        </button>
      </div>

      {/* Tab 1: Details */}
      {activeTabSub === 'details' && (
        <div className="space-y-6">
          <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              Formulated Business Question
            </h3>
            <p className="mt-2 text-base font-semibold text-slate-900 dark:text-white">
              "{currentOverview?.question || 'No question recorded'}"
            </p>

            <div className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-3">
              <div className="rounded-lg border border-slate-100 bg-slate-50/70 p-3 dark:border-slate-800 dark:bg-slate-800/40">
                <span className="text-[11px] text-slate-500 dark:text-slate-400">Primary Mart</span>
                <p className="mt-1 font-mono text-xs font-semibold text-slate-800 dark:text-slate-200">
                  {currentOverview?.primary_mart || 'None'}
                </p>
              </div>
              <div className="rounded-lg border border-slate-100 bg-slate-50/70 p-3 dark:border-slate-800 dark:bg-slate-800/40">
                <span className="text-[11px] text-slate-500 dark:text-slate-400">Primary KPI</span>
                <p className="mt-1 font-mono text-xs font-semibold text-slate-800 dark:text-slate-200">
                  {currentOverview?.primary_kpi || 'None'}
                </p>
              </div>
              <div className="rounded-lg border border-slate-100 bg-slate-50/70 p-3 dark:border-slate-800 dark:bg-slate-800/40">
                <span className="text-[11px] text-slate-500 dark:text-slate-400">Project Configuration</span>
                <p className="mt-1 text-xs font-semibold text-slate-800 dark:text-slate-200">
                  {currentOverview?.has_config ? 'project_config.json present' : 'URL or single CSV source'}
                </p>
              </div>
            </div>
          </div>

          {/* Artifacts Breakdown */}
          <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              Artifact Audit
            </h3>
            <div className="mt-3 divide-y divide-slate-100 dark:divide-slate-800">
              <div className="flex items-center justify-between py-2.5 text-xs">
                <span className="font-medium text-slate-700 dark:text-slate-300">
                  Final Summary Report (final_summary_report.json)
                </span>
                <span className="flex items-center text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="mr-1.5 h-4 w-4" /> Available
                </span>
              </div>
              <div className="flex items-center justify-between py-2.5 text-xs">
                <span className="font-medium text-slate-700 dark:text-slate-300">
                  Data Quality Scores (dq_score_*.json)
                </span>
                {currentOverview?.artifacts.data_quality ? (
                  <span className="flex items-center text-emerald-600 dark:text-emerald-400">
                    <CheckCircle2 className="mr-1.5 h-4 w-4" /> Available
                  </span>
                ) : (
                  <span className="flex items-center text-slate-400 dark:text-slate-500">
                    <AlertCircle className="mr-1.5 h-4 w-4" /> Single-dataset runs only
                  </span>
                )}
              </div>
              <div className="flex items-center justify-between py-2.5 text-xs">
                <span className="font-medium text-slate-700 dark:text-slate-300">
                  Anomaly Report (v2 Framed / v1 Raw)
                </span>
                <span className="flex items-center text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="mr-1.5 h-4 w-4" /> Available
                </span>
              </div>
              <div className="flex items-center justify-between py-2.5 text-xs">
                <span className="font-medium text-slate-700 dark:text-slate-300">
                  Lineage DAG (lineage.json)
                </span>
                {currentOverview?.artifacts.lineage ? (
                  <span className="flex items-center text-emerald-600 dark:text-emerald-400">
                    <CheckCircle2 className="mr-1.5 h-4 w-4" /> Available
                  </span>
                ) : (
                  <span className="flex items-center text-slate-400 dark:text-slate-500">
                    <AlertCircle className="mr-1.5 h-4 w-4" /> Multi-dataset lineage pending
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Tab 2: Semantic Metrics */}
      {activeTabSub === 'metrics' && mode === 'live' && (
        <LiveOnlyNote what="The metrics catalogue" />
      )}
      {activeTabSub === 'metrics' && mode === 'demo' && (
        <div className="rounded-xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="border-b border-slate-100 p-4 dark:border-slate-800">
            <h3 className="text-sm font-semibold text-slate-900 dark:text-white">
              Semantic Metrics Definitions
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Formulas and aggregation intents extracted automatically by the KPI Engine (dbt-style semantics).
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-100 bg-slate-50/70 font-medium text-slate-500 dark:border-slate-800 dark:bg-slate-800/40 dark:text-slate-400">
                <tr>
                  <th className="px-4 py-3">Metric Name</th>
                  <th className="px-4 py-3">Source Mart</th>
                  <th className="px-4 py-3">Source Column</th>
                  <th className="px-4 py-3">Aggregation</th>
                  <th className="px-4 py-3">Unit</th>
                  <th className="px-4 py-3">Formula Description</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                {metrics.map((m) => (
                  <tr key={m.metric_name} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40">
                    <td className="px-4 py-3 font-mono font-semibold text-sky-700 dark:text-sky-400">
                      {m.metric_name}
                    </td>
                    <td className="px-4 py-3 font-mono text-slate-600 dark:text-slate-300">
                      {m.source_mart}
                    </td>
                    <td className="px-4 py-3 font-mono text-slate-600 dark:text-slate-300">
                      {m.source_column}
                    </td>
                    <td className="px-4 py-3">
                      <span className="rounded bg-slate-100 px-2 py-0.5 font-medium text-slate-700 dark:bg-slate-800 dark:text-slate-300">
                        {m.default_aggregation}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-slate-500 dark:text-slate-400">
                      {m.unit || '—'}
                    </td>
                    <td className="px-4 py-3 text-slate-600 dark:text-slate-300">
                      {m.formula_description}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Tab 3: Configuration Specs */}
      {activeTabSub === 'config' && mode === 'live' && (
        <LiveOnlyNote what="The project configuration" />
      )}
      {activeTabSub === 'config' && mode === 'demo' && (
        <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between pb-3">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
              Pipeline Blueprint & Project Config
            </h3>
            <span className="rounded bg-slate-100 px-2 py-0.5 font-mono text-[10px] text-slate-600 dark:bg-slate-800 dark:text-slate-400">
              JSON Specification
            </span>
          </div>
          <pre className="mt-2 overflow-x-auto rounded-lg bg-slate-950 p-4 font-mono text-xs text-sky-300 dark:bg-slate-950">
            {selectedProject === 'global_economic_indicators'
              ? JSON.stringify(
                  {
                    project: 'global_economic_indicators',
                    question: 'How do global GDP, inflation, and unemployment evolve over time and how are they related?',
                    integrated_output_name: 'master_dataset',
                    datasets: [
                      {
                        name: 'gdp',
                        url: 'https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv',
                        loader: 'world_indicator_series',
                        time_column: 'year',
                        metric_column: 'gdp',
                      },
                      {
                        name: 'inflation',
                        url: 'https://api.worldbank.org/v2/country/all/indicator/FP.CPI.TOTL.ZG?format=json',
                        loader: 'world_indicator_series',
                        time_column: 'year',
                        metric_column: 'inflation',
                      },
                      {
                        name: 'unemployment',
                        url: 'https://api.worldbank.org/v2/country/all/indicator/SL.UEM.TOTL.ZS?format=json',
                        loader: 'world_indicator_series',
                        time_column: 'year',
                        metric_column: 'unemployment',
                      },
                    ],
                  },
                  null,
                  2,
                )
              : JSON.stringify(
                  {
                    source: 'release_impact',
                    question: 'Which release years generate the strongest average streams?',
                    metric_column: 'streams',
                    dimension_column: 'release_year',
                    aggregation_intent: {
                      aggregation: 'average',
                      ordering: 'highest',
                    },
                    mart_paths: {
                      mart_question_answer: 'projects/release_impact/marts/mart_question_answer.parquet',
                      mart_top_entities: 'projects/release_impact/marts/mart_top_entities.parquet',
                      mart_release_impact: 'projects/release_impact/marts/mart_release_impact.parquet',
                      mart_provider_performance: 'projects/release_impact/marts/mart_provider_performance.parquet',
                    },
                  },
                  null,
                  2,
                )}
          </pre>
        </div>
      )}
    </div>
  );
};
