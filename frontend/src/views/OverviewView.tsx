import React, { useState, useEffect } from 'react';
import {
  Layers,
  Table,
  ShieldCheck,
  AlertTriangle,
  Play,
  ArrowRight,
  CheckCircle2,
  Info,
  Sparkles,
  Upload,
} from 'lucide-react';
import { useApp } from '../context/AppContext';

export const OverviewView: React.FC = () => {
  const {
    projects,
    currentOverview,
    setSelectedProject,
    setActiveTab,
    selectedProject,
    mode,
    apiClient,
  } = useApp();

  const [liveDqScore, setLiveDqScore] = useState<number | null>(null);
  const [liveAnomalyCount, setLiveAnomalyCount] = useState<number | null>(null);

  const isSingleDataset = currentOverview?.project_mode
    ? currentOverview.project_mode === 'single_dataset'
    : selectedProject !== 'global_economic_indicators';
  const hasDQ = currentOverview?.artifacts.data_quality ?? false;
  const hasAnomalies = currentOverview?.artifacts.anomalies ?? false;
  const isV2Anomalies = isSingleDataset;

  useEffect(() => {
    if (mode !== 'live' || !selectedProject) {
      setLiveDqScore(null);
      setLiveAnomalyCount(null);
      return;
    }
    if (hasDQ) {
      apiClient
        .getDataQuality(selectedProject)
        .then((res) => setLiveDqScore(res.reports[0]?.overall_score ?? null))
        .catch(() => setLiveDqScore(null));
    } else {
      setLiveDqScore(null);
    }
    if (hasAnomalies) {
      apiClient
        .getAnomalies(selectedProject)
        .then((res) => setLiveAnomalyCount(res.anomaly_row_count))
        .catch(() => setLiveAnomalyCount(null));
    } else {
      setLiveAnomalyCount(null);
    }
  }, [apiClient, hasAnomalies, hasDQ, mode, selectedProject]);

  return (
    <div className="space-y-6">
      {/* Top Banner / Mode Notice */}
      {mode === 'demo' && (
        <div className="flex flex-col justify-between space-y-2 rounded-2xl border border-amber-200/80 bg-amber-50/70 p-4 text-xs text-amber-900 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-200 sm:flex-row sm:items-center sm:space-y-0">
          <div className="flex items-center space-x-3">
            <Info className="h-5 w-5 shrink-0 text-amber-600 dark:text-amber-400" />
            <div>
              <span className="font-bold">Public Static Demo Mode:</span>
              <span className="ml-1 text-amber-800/90 dark:text-amber-300/90">
                Viewing bundled demo fixtures (not live data). Browser-side profiling is enabled in "Import & Profile".
              </span>
            </div>
          </div>
          <button
            onClick={() => setActiveTab('import')}
            className="flex items-center space-x-1 font-semibold text-amber-900 underline hover:text-amber-700 dark:text-amber-300"
          >
            <span>Try Interactive Profiler</span>
            <ArrowRight className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {/* Metrics Row */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {/* Projects Card */}
        <div className="rounded-2xl border border-slate-200 bg-white p-4.5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              Registered Projects
            </span>
            <div className="rounded-xl bg-sky-50 p-2 text-sky-600 dark:bg-sky-950/60 dark:text-sky-400">
              <Layers className="h-4 w-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">
              {projects.length}
            </span>
            <span className="text-xs text-slate-500 dark:text-slate-400">active</span>
          </div>
          <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
            Selected: <span className="font-semibold text-slate-700 dark:text-slate-300 font-mono">{selectedProject}</span>
          </p>
        </div>

        {/* Marts Card */}
        <div className="rounded-2xl border border-slate-200 bg-white p-4.5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              Analytical Marts
            </span>
            <div className="rounded-xl bg-emerald-50 p-2 text-emerald-600 dark:bg-emerald-950/60 dark:text-emerald-400">
              <Table className="h-4 w-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">
              {currentOverview?.marts_count ?? 8}
            </span>
            <span className="text-xs text-slate-500 dark:text-slate-400">Parquet tables</span>
          </div>
          <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
            Primary: <span className="font-mono text-slate-700 dark:text-slate-300">{currentOverview?.primary_mart || 'mart_release_impact'}</span>
          </p>
        </div>

        {/* Quality Score Card */}
        <div className="rounded-2xl border border-slate-200 bg-white p-4.5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              Data Quality Score
            </span>
            <div className="rounded-xl bg-blue-50 p-2 text-blue-600 dark:bg-blue-950/60 dark:text-blue-400">
              <ShieldCheck className="h-4 w-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">
              {hasDQ
                ? liveDqScore !== null
                  ? `${liveDqScore}%`
                  : '93.3%'
                : '—'}
            </span>
            <span className="text-xs text-emerald-600 dark:text-emerald-400">
              {hasDQ ? 'High fidelity' : 'Single-dataset runs only'}
            </span>
          </div>
          <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
            {hasDQ ? 'Weighted across completeness & formatting' : 'Not generated for multi-dataset'}
          </p>
        </div>

        {/* Anomalies Card */}
        <div className="rounded-2xl border border-slate-200 bg-white p-4.5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              Anomaly Framing
            </span>
            <div className="rounded-xl bg-amber-50 p-2 text-amber-600 dark:bg-amber-950/60 dark:text-amber-400">
              <AlertTriangle className="h-4 w-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline space-x-2">
            <span className="text-2xl font-bold tracking-tight text-slate-900 dark:text-white">
              {mode === 'live' && !hasAnomalies
                ? '—'
                : liveAnomalyCount !== null
                ? `${liveAnomalyCount} Flagged`
                : isV2Anomalies
                ? '1 Flagged'
                : '2 Flagged'}
            </span>
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-800 dark:bg-amber-950 dark:text-amber-300">
              {isV2Anomalies ? 'v2 Framed' : 'v1 Raw'}
            </span>
          </div>
          <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
            {isV2Anomalies ? 'IQR detection on streams' : 'z-score & historical deltas'}
          </p>
        </div>
      </div>

      {/* Main Focus: Active Project Business Question & Status */}
      {currentOverview && (
        <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs dark:border-slate-800 dark:bg-slate-900 sm:p-7">
          <div className="flex flex-col justify-between space-y-4 md:flex-row md:items-center md:space-y-0">
            <div className="space-y-1.5">
              <div className="flex items-center space-x-2">
                <span className="text-xs font-bold uppercase tracking-wider text-sky-600 dark:text-sky-400">
                  Formulated Analytics Question
                </span>
                <span className="rounded-md bg-slate-100 px-2 py-0.5 text-[11px] font-mono text-slate-700 dark:bg-slate-800 dark:text-slate-300">
                  {currentOverview.project}
                </span>
              </div>
              <h2 className="text-lg font-bold text-slate-900 dark:text-white">
                "{currentOverview.question || 'No question recorded'}"
              </h2>
            </div>
            <div className="flex items-center space-x-2.5">
              <button
                onClick={() => setActiveTab('marts')}
                className="flex items-center space-x-1.5 rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs font-semibold text-slate-700 shadow-2xs hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
              >
                <Table className="h-3.5 w-3.5 text-slate-500" />
                <span>Explore Marts</span>
              </button>
              <button
                onClick={() => setActiveTab('operations')}
                className="flex items-center space-x-1.5 rounded-xl bg-sky-600 px-4 py-2 text-xs font-semibold text-white shadow-xs hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400"
              >
                <Play className="h-3.5 w-3.5" />
                <span>Pipeline Console</span>
              </button>
            </div>
          </div>

          {/* Artifact Availability Badges */}
          <div className="mt-6 border-t border-slate-100 pt-4 dark:border-slate-800">
            <div className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              Pipeline Artifact Audit Checklist
            </div>
            <div className="mt-2.5 flex flex-wrap gap-2.5">
              {Object.entries(currentOverview.artifacts).map(([key, present]) => (
                <div
                  key={key}
                  className={`flex items-center space-x-1.5 rounded-xl border px-3 py-1.5 text-xs font-semibold ${
                    present
                      ? 'border-emerald-200 bg-emerald-50/70 text-emerald-800 dark:border-emerald-900/60 dark:bg-emerald-950/30 dark:text-emerald-300'
                      : 'border-slate-200 bg-slate-50 text-slate-400 dark:border-slate-800 dark:bg-slate-800/40 dark:text-slate-500'
                  }`}
                >
                  <CheckCircle2
                    className={`h-3.5 w-3.5 ${
                      present ? 'text-emerald-600 dark:text-emerald-400' : 'text-slate-300 dark:text-slate-600'
                    }`}
                  />
                  <span className="capitalize">{key.replace('_', ' ')}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Projects Explorer Table */}
      <div className="rounded-3xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
        <div className="flex items-center justify-between border-b border-slate-200 p-5 dark:border-slate-800">
          <div>
            <h3 className="text-sm font-bold text-slate-900 dark:text-white">
              Registered Projects Repository
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Analytical projects managed by the Quaero pipeline
            </p>
          </div>
          <button
            onClick={() => setActiveTab('explorer')}
            className="flex items-center space-x-1 text-xs font-semibold text-sky-600 hover:text-sky-700 dark:text-sky-400"
          >
            <span>Open Project Explorer</span>
            <ArrowRight className="h-3.5 w-3.5" />
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-slate-100 bg-slate-50/80 font-semibold text-slate-600 dark:border-slate-800 dark:bg-slate-800/50 dark:text-slate-300">
              <tr>
                <th className="px-5 py-3.5">Project Name</th>
                <th className="px-5 py-3.5">Mode</th>
                <th className="px-5 py-3.5">Marts</th>
                <th className="px-5 py-3.5">Last Run Status</th>
                <th className="px-5 py-3.5">Artifacts</th>
                <th className="px-5 py-3.5 text-right">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
              {projects.map((proj) => {
                const isSelected = proj.project === selectedProject;
                return (
                  <tr
                    key={proj.project}
                    className={`transition hover:bg-slate-50/60 dark:hover:bg-slate-800/40 ${
                      isSelected ? 'bg-sky-50/40 dark:bg-sky-950/20' : ''
                    }`}
                  >
                    <td className="px-5 py-4">
                      <div className="font-bold text-slate-900 dark:text-white font-mono">
                        {proj.project}
                      </div>
                      <div className="text-[11px] text-slate-400 font-sans">
                        {proj.has_summary ? 'Summary report available' : 'No summary'}
                      </div>
                    </td>
                    <td className="px-5 py-4 text-slate-600 dark:text-slate-300">
                      <span className="rounded-lg bg-slate-100 px-2.5 py-0.5 text-[11px] font-mono dark:bg-slate-800">
                        {proj.project === 'global_economic_indicators' ? 'multi_dataset' : 'single_dataset'}
                      </span>
                    </td>
                    <td className="px-5 py-4 font-bold text-slate-700 dark:text-slate-300">
                      {proj.marts_count} Parquet
                    </td>
                    <td className="px-5 py-4">
                      <span className="inline-flex items-center rounded-full bg-emerald-50 px-2.5 py-0.5 text-[11px] font-bold text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-400">
                        <CheckCircle2 className="mr-1 h-3 w-3" />
                        COMPLETED
                      </span>
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex space-x-1.5 font-mono text-[10px]">
                        <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                          SUMMARY
                        </span>
                        {proj.project === 'release_impact' && (
                          <>
                            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                              DQ
                            </span>
                            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                              LINEAGE
                            </span>
                          </>
                        )}
                        <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                          METRICS
                        </span>
                      </div>
                    </td>
                    <td className="px-5 py-4 text-right">
                      <button
                        onClick={() => {
                          setSelectedProject(proj.project);
                          setActiveTab('explorer');
                        }}
                        className="rounded-xl border border-slate-200 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
