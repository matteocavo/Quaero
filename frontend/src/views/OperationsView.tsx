import React, { useState, useEffect, useCallback } from 'react';
import {
  PlayCircle,
  RefreshCw,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Loader2,
  ArrowRight,
  Info,
  FolderPlus,
  Table2,
  ShieldAlert,
  GitBranch,
  Download,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import { InterpretationPanel } from '../components/InterpretationPanel';
import { LocalPipelineWizard } from '../components/LocalPipelineWizard';
import { useApp } from '../context/AppContext';
import { Aggregation, QuestionInterpretation, RunHistoryItem, RunStatusResponse } from '../types/api';
import { downloadTextFile } from '../utils/download';

export const OperationsView: React.FC = () => {
  const {
    selectedProject,
    currentOverview,
    apiClient,
    mode,
    showFlash,
    setActiveTab,
    refreshOverview,
    refreshProjects,
  } = useApp();

  const [question, setQuestion] = useState('');
  const [sourceUrl, setSourceUrl] = useState('');
  const [metricColumn, setMetricColumn] = useState('');
  const [dimensionColumn, setDimensionColumn] = useState('');
  const [aggregation, setAggregation] = useState<Aggregation | ''>('');
  const [resultInterpretation, setResultInterpretation] = useState<QuestionInterpretation | null>(null);
  const [runs, setRuns] = useState<RunHistoryItem[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [selectedError, setSelectedError] = useState<string | null>(null);
  const [showWizard, setShowWizard] = useState(false);
  const [monitoredRun, setMonitoredRun] = useState<RunStatusResponse | null>(null);
  const [downloadingSummary, setDownloadingSummary] = useState(false);

  const loadHistory = useCallback(async () => {
    if (!selectedProject) return;
    setLoadingHistory(true);
    try {
      const res = await apiClient.getRunHistory(selectedProject);
      setRuns(res.runs);
      const activeInHistory = res.runs.find(
        (r) => r.status === 'QUEUED' || r.status === 'RUNNING',
      );
      if (activeInHistory) {
        setMonitoredRun((prev) =>
          prev && prev.run_id === activeInHistory.run_id
            ? prev
            : {
                run_id: activeInHistory.run_id,
                project: selectedProject,
                status: activeInHistory.status,
                error: activeInHistory.error ?? null,
                result_keys: [],
              },
        );
      }
    } catch (err: any) {
      showFlash(`Could not load run history: ${err.message}`, 'error');
    } finally {
      setLoadingHistory(false);
    }
  }, [apiClient, selectedProject, showFlash]);

  useEffect(() => {
    setMonitoredRun(null);
    loadHistory();
    if (currentOverview?.question) {
      setQuestion(currentOverview.question);
    }
  }, [selectedProject, currentOverview?.question, loadHistory]);

  const pollRunStatus = useCallback(
    async (project: string, runId: string) => {
      if (mode !== 'live') return;
      try {
        const statusRes = await apiClient.getRunStatus(project, runId);
        setMonitoredRun(statusRes);
        if (statusRes.status === 'COMPLETED') {
          showFlash(`Pipeline run ${runId} completed for '${project}'.`, 'success');
          await refreshProjects(project);
          await refreshOverview();
          await loadHistory();
        } else if (statusRes.status === 'FAILED') {
          showFlash(
            `Pipeline run ${runId} failed: ${statusRes.error || 'Unknown error'}`,
            'error',
          );
          await refreshOverview();
          await loadHistory();
        }
      } catch {
        // Ignore transient poll error
      }
    },
    [apiClient, loadHistory, mode, refreshOverview, refreshProjects, showFlash],
  );

  // Automatic status polling while a run is QUEUED or RUNNING in Local Operations mode
  useEffect(() => {
    if (
      mode !== 'live' ||
      !monitoredRun ||
      (monitoredRun.status !== 'QUEUED' && monitoredRun.status !== 'RUNNING')
    ) {
      return;
    }
    const timer = setInterval(() => {
      pollRunStatus(monitoredRun.project, monitoredRun.run_id);
    }, 1000);
    // Also trigger an immediate check
    pollRunStatus(monitoredRun.project, monitoredRun.run_id);
    return () => clearInterval(timer);
  }, [mode, monitoredRun?.project, monitoredRun?.run_id, monitoredRun?.status, pollRunStatus]);

  // Client-side validation matching console/formatting.py
  const validateForm = (): boolean => {
    const errors: string[] = [];
    if (!selectedProject.trim()) {
      errors.push('Project name is required.');
    } else if (!/^[A-Za-z0-9_-]+$/.test(selectedProject.trim())) {
      errors.push('Project name may contain only letters, digits, underscores, and hyphens.');
    }

    if (!question.trim()) {
      errors.push('A business question is required.');
    } else if (question.trim().length > 1000) {
      errors.push('The business question is limited to 1000 characters.');
    }

    if (sourceUrl.trim()) {
      try {
        const u = new URL(sourceUrl.trim());
        if (u.protocol !== 'http:' && u.protocol !== 'https:') {
          errors.push('The source URL must use http or https.');
        }
      } catch {
        errors.push('The source URL is not a valid URL.');
      }
    }

    setValidationErrors(errors);
    return errors.length === 0;
  };

  const handleStartRun = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateForm()) return;

    if (mode === 'demo') {
      showFlash(
        'Pipeline execution requires running the local Python FastAPI engine. Switch to Local Operations mode or use the Browser Data Import & Profiler.',
        'info',
      );
      return;
    }

    setSubmitting(true);
    try {
      const res = await apiClient.startRun(
        selectedProject,
        question.trim(),
        sourceUrl.trim() || undefined,
        metricColumn.trim() || undefined,
        dimensionColumn.trim() || undefined,
        aggregation || undefined,
      );
      setMonitoredRun({
        run_id: res.run_id,
        project: res.project,
        status: res.status,
        error: null,
        result_keys: [],
      });
      showFlash(`Run accepted & queued: ${res.run_id}`, 'success');
      await loadHistory();
      await pollRunStatus(res.project, res.run_id);
    } catch (err: any) {
      showFlash(`Execution rejected: ${err.message}`, 'error');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDownloadSummary = async () => {
    if (!selectedProject) return;
    setDownloadingSummary(true);
    try {
      const detail = await apiClient.getProjectSummary(selectedProject);
      downloadTextFile(
        `${selectedProject}_summary_report.json`,
        JSON.stringify(detail.summary, null, 2),
        'application/json',
      );
      showFlash(`Downloaded ${selectedProject}_summary_report.json`, 'success');
    } catch (err: any) {
      showFlash(`Could not download summary report: ${err.message}`, 'error');
    } finally {
      setDownloadingSummary(false);
    }
  };

  const formatDuration = (start?: string | null, end?: string | null): string => {
    if (!start || !end) return '—';
    try {
      const s = new Date(start).getTime();
      const e = new Date(end).getTime();
      const diffSec = Math.round((e - s) / 1000);
      if (diffSec < 0) return '—';
      if (diffSec < 60) return `${diffSec}s`;
      const min = Math.floor(diffSec / 60);
      const sec = diffSec % 60;
      return `${min}m ${sec}s`;
    } catch {
      return '—';
    }
  };

  useEffect(() => {
    // The interpretation recorded with the results: what the numbers below actually answer.
    setResultInterpretation(null);
    if (mode !== 'live' || !selectedProject) return;
    let cancelled = false;
    apiClient
      .getProjectSummary(selectedProject)
      .then((detail) => {
        const recorded = detail.summary?.interpretation;
        if (!cancelled && recorded && typeof recorded === 'object') setResultInterpretation(recorded as QuestionInterpretation);
      })
      .catch(() => {
        // No summary yet (nothing has run): there is nothing to show.
      });
    return () => {
      cancelled = true;
    };
  }, [apiClient, mode, selectedProject, monitoredRun?.status]);

  const hasResults =
    mode === 'live' &&
    ((currentOverview?.marts_count ?? 0) > 0 ||
      Boolean(currentOverview?.artifacts?.summary) ||
      monitoredRun?.status === 'COMPLETED' ||
      runs.some((r) => r.status === 'COMPLETED'));

  return (
    <div className="space-y-6">
      {/* Local Operations Workflow Banner & Inline CSV Project Wizard */}
      {mode === 'live' && (
        <div className="space-y-4">
          <div className="flex flex-col justify-between gap-3 rounded-3xl border border-sky-200 bg-sky-50/70 p-4 dark:border-sky-900/50 dark:bg-sky-950/30 sm:flex-row sm:items-center">
            <div className="space-y-1">
              <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-extrabold uppercase tracking-wider text-sky-900 dark:text-sky-300">
                <span>Local Operations Workflow:</span>
                <span className="rounded bg-white px-2 py-0.5 text-sky-800 shadow-2xs dark:bg-slate-900 dark:text-sky-300">
                  Import CSV
                </span>
                <span>→</span>
                <span className="rounded bg-white px-2 py-0.5 text-sky-800 shadow-2xs dark:bg-slate-900 dark:text-sky-300">
                  Configure Project
                </span>
                <span>→</span>
                <span className="rounded bg-sky-600 px-2 py-0.5 text-white shadow-2xs">
                  Run Python Pipeline
                </span>
                <span>→</span>
                <span className="rounded bg-white px-2 py-0.5 text-sky-800 shadow-2xs dark:bg-slate-900 dark:text-sky-300">
                  Explore Results
                </span>
              </div>
              <p className="text-xs text-slate-700 dark:text-slate-300">
                Upload a new CSV dataset and create a local project, or configure and execute the Python pipeline on <strong>{selectedProject}</strong>.
              </p>
            </div>

            <button
              type="button"
              data-testid="toggle-local-wizard-btn"
              onClick={() => setShowWizard((prev) => !prev)}
              className="inline-flex shrink-0 items-center space-x-1.5 rounded-2xl bg-emerald-600 px-4 py-2 text-xs font-bold text-white shadow-2xs transition hover:bg-emerald-500"
            >
              <FolderPlus className="h-4 w-4" />
              <span>{showWizard ? 'Hide CSV Project Setup' : 'New Project from CSV'}</span>
              {showWizard ? (
                <ChevronUp className="h-3.5 w-3.5" />
              ) : (
                <ChevronDown className="h-3.5 w-3.5" />
              )}
            </button>
          </div>

          {showWizard && (
            <LocalPipelineWizard
              compact
              onProjectCreated={(created, launchedRun, runAccepted) => {
                setQuestion(created.question);
                setMetricColumn(created.metric_column || '');
                setDimensionColumn(created.dimension_column || '');
                setShowWizard(false);
                if (launchedRun && runAccepted) {
                  setMonitoredRun({
                    run_id: runAccepted.run_id,
                    project: created.project,
                    status: runAccepted.status,
                    error: null,
                    result_keys: [],
                  });
                  pollRunStatus(created.project, runAccepted.run_id);
                }
              }}
            />
          )}
        </div>
      )}

      {/* Live Run Status Monitor */}
      {mode === 'live' && monitoredRun && (
        <div
          data-testid="live-run-status-banner"
          className={`flex flex-col justify-between gap-3 rounded-3xl border p-4 sm:flex-row sm:items-center ${
            monitoredRun.status === 'COMPLETED'
              ? 'border-emerald-200 bg-emerald-50/80 text-emerald-950 dark:border-emerald-900/60 dark:bg-emerald-950/40 dark:text-emerald-200'
              : monitoredRun.status === 'FAILED'
              ? 'border-rose-200 bg-rose-50/80 text-rose-950 dark:border-rose-900/60 dark:bg-rose-950/40 dark:text-rose-200'
              : 'border-sky-200 bg-sky-50/80 text-sky-950 dark:border-sky-900/60 dark:bg-sky-950/40 dark:text-sky-200'
          }`}
        >
          <div className="flex items-start space-x-3">
            {monitoredRun.status === 'COMPLETED' && (
              <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-600 dark:text-emerald-400" />
            )}
            {monitoredRun.status === 'FAILED' && (
              <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-rose-600 dark:text-rose-400" />
            )}
            {(monitoredRun.status === 'QUEUED' || monitoredRun.status === 'RUNNING') && (
              <Loader2 className="mt-0.5 h-5 w-5 shrink-0 animate-spin text-sky-600 dark:text-sky-400" />
            )}
            <div className="space-y-0.5 text-xs">
              <div className="font-extrabold">
                Run Status: <span data-testid="monitored-run-status">{monitoredRun.status}</span> ·{' '}
                <span className="font-mono">{monitoredRun.run_id}</span>
              </div>
              {monitoredRun.error ? (
                <div
                  data-testid="run-error"
                  className="whitespace-pre-line font-mono text-[11px] text-rose-700 dark:text-rose-300"
                >
                  {monitoredRun.error}
                </div>
              ) : (
                <div className="text-[11px] opacity-85">
                  {monitoredRun.status === 'COMPLETED'
                    ? 'Python pipeline completed. Marts, Data Quality scores, Anomaly reports, and Lineage are ready below.'
                    : 'Executing Python pipeline on local dataset...'}
                </div>
              )}
            </div>
          </div>

          <button
            type="button"
            data-testid="poll-run-status-btn"
            onClick={() => pollRunStatus(monitoredRun.project, monitoredRun.run_id)}
            className="inline-flex shrink-0 items-center space-x-1.5 rounded-xl border border-current/20 bg-white/80 px-3 py-1.5 text-xs font-bold shadow-2xs hover:bg-white dark:bg-slate-900/80"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            <span>Check Status</span>
          </button>
        </div>
      )}

      {/* Execution Form Card */}
      <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
        <div className="flex flex-col justify-between space-y-2 border-b border-slate-100 pb-4 dark:border-slate-800 sm:flex-row sm:items-center sm:space-y-0">
          <div>
            <h3 className="text-base font-bold text-slate-900 dark:text-white">
              Trigger Pipeline Run
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Transforms raw inputs into clean staging, calculates column DQ scores, frames anomalies, and builds analytical marts.
            </p>
          </div>
          {mode === 'demo' && (
            <span className="rounded-full bg-amber-100 px-3 py-1 text-xs font-bold text-amber-800 dark:bg-amber-950/80 dark:text-amber-300">
              Demo Sandbox (Read-Only)
            </span>
          )}
        </div>

        {mode === 'demo' && (
          <div className="mt-4 flex flex-col items-start justify-between rounded-2xl border border-amber-200 bg-amber-50/70 p-4 text-xs text-amber-900 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-200 sm:flex-row sm:items-center">
            <div className="flex items-center space-x-2.5">
              <Info className="h-5 w-5 shrink-0 text-amber-600 dark:text-amber-400" />
              <div>
                <span className="font-bold">Public Demo Sandbox Mode:</span>
                <span className="ml-1">
                  Full ETL pipeline execution requires the local Python environment. To analyze custom data without a backend, use the interactive browser profiler.
                </span>
              </div>
            </div>
            <button
              onClick={() => setActiveTab('import')}
              className="mt-2 flex shrink-0 items-center space-x-1 font-bold text-sky-700 underline hover:text-sky-800 dark:text-sky-300 sm:mt-0"
            >
              <span>Launch Browser Profiler</span>
              <ArrowRight className="h-3.5 w-3.5" />
            </button>
          </div>
        )}

        {validationErrors.length > 0 && (
          <div className="mt-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-800 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-300">
            <div className="font-semibold">Input Validation:</div>
            <ul className="mt-1 list-disc pl-4 space-y-0.5">
              {validationErrors.map((err, i) => (
                <li key={i}>{err}</li>
              ))}
            </ul>
          </div>
        )}

        <form onSubmit={handleStartRun} className="mt-5 space-y-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label htmlFor="run-project" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                Target Project
              </label>
              <input
                id="run-project"
                type="text"
                disabled
                value={selectedProject}
                className="mt-1 w-full rounded-xl border border-slate-200 bg-slate-50 px-3.5 py-2 text-xs font-mono text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
              />
            </div>
            <div>
              <label htmlFor="run-source-url" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                Source Dataset URL (Optional if project_config.json exists)
              </label>
              <input
                id="run-source-url"
                type="text"
                value={sourceUrl}
                onChange={(e) => setSourceUrl(e.target.value)}
                placeholder="https://example.com/dataset.csv"
                className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
              />
            </div>
          </div>

          {mode === 'live' && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <div>
                <label htmlFor="run-metric-column" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                  Metric Column Override (Optional, single-dataset projects)
                </label>
                <input
                  id="run-metric-column"
                  type="text"
                  value={metricColumn}
                  onChange={(e) => setMetricColumn(e.target.value)}
                  placeholder="Leave blank to use project_config.json or auto-infer"
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                />
              </div>
              <div>
                <label htmlFor="run-dimension-column" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                  Dimension Column Override (Optional, single-dataset projects)
                </label>
                <input
                  id="run-dimension-column"
                  type="text"
                  value={dimensionColumn}
                  onChange={(e) => setDimensionColumn(e.target.value)}
                  placeholder="Leave blank to use project_config.json or auto-infer"
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                />
              </div>
              <div>
                <label htmlFor="run-aggregation" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                  Aggregation (Optional)
                </label>
                <select
                  id="run-aggregation"
                  data-testid="run-aggregation-select"
                  value={aggregation}
                  onChange={(e) => setAggregation(e.target.value as Aggregation | '')}
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                >
                  <option value="">Read it from the question</option>
                  <option value="average">Average</option>
                  <option value="sum">Total (sum)</option>
                  <option value="count">Count of records</option>
                </select>
              </div>
            </div>
          )}

          <div>
            <div className="flex items-center justify-between">
              <label htmlFor="run-question" className="text-xs font-semibold text-slate-700 dark:text-slate-300">
                Business Analytics Question
              </label>
              <span className="text-[10px] text-slate-400">
                {question.length}/1000 characters
              </span>
            </div>
            <textarea
              id="run-question"
              rows={2}
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="e.g. Which release years generate the strongest average streams?"
              className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
            />
          </div>

          <div className="flex flex-col justify-between space-y-3 pt-2 sm:flex-row sm:items-center sm:space-y-0">
            <div className="text-[11px] text-slate-500 dark:text-slate-400">
              {mode === 'demo'
                ? 'Execution disabled in static demo. Run FastAPI locally to execute.'
                : 'Runs asynchronously via FastAPI BackgroundTasks.'}
            </div>

            <button
              type="submit"
              disabled={submitting}
              className={`flex items-center justify-center space-x-2 rounded-2xl px-5 py-2.5 text-xs font-bold text-white shadow-xs transition ${
                mode === 'demo'
                  ? 'cursor-not-allowed bg-slate-400 opacity-70 dark:bg-slate-700'
                  : 'bg-sky-600 hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400'
              }`}
            >
              {submitting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span>Submitting to FastAPI...</span>
                </>
              ) : (
                <>
                  <PlayCircle className="h-4 w-4" />
                  <span>{mode === 'demo' ? 'Run Pipeline (Demo Mode Disabled)' : 'Run Pipeline'}</span>
                </>
              )}
            </button>
          </div>
        </form>
      </div>

      {/* Step 4: Explore Generated Results & Download Artifacts */}
      {hasResults && (
        <div
          data-testid="local-results-panel"
          className="rounded-3xl border border-emerald-200 bg-white p-6 shadow-2xs dark:border-emerald-900/50 dark:bg-slate-900"
        >
          <div className="flex flex-col justify-between gap-2 border-b border-slate-100 pb-4 dark:border-slate-800 sm:flex-row sm:items-center">
            <div>
              <div className="flex items-center space-x-2">
                <CheckCircle2 className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                <h3 className="text-base font-bold text-slate-900 dark:text-white">
                  Explore Pipeline Results & Export Artifacts
                </h3>
              </div>
              <p className="text-xs text-slate-500 dark:text-slate-400">
                Inspect generated Parquet marts, column Data Quality scores, framed anomalies, DAG lineage, or download the summary report.
              </p>
            </div>
            <span className="rounded-full bg-emerald-100 px-3 py-1 text-xs font-bold text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
              {currentOverview?.marts_count ?? 0} Marts Generated
            </span>
          </div>

          {resultInterpretation && (
            <div className="mt-4" data-testid="result-interpretation">
              <div className="mb-1.5 text-[11px] font-bold uppercase tracking-wide text-slate-500 dark:text-slate-400">
                How these results were calculated
              </div>
              <InterpretationPanel interpretation={resultInterpretation} />
            </div>
          )}

          <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <button
              type="button"
              data-testid="goto-marts-btn"
              onClick={() => setActiveTab('marts')}
              className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50/70 p-4 text-left transition hover:border-sky-400 hover:bg-sky-50/40 dark:border-slate-800 dark:bg-slate-800/50 dark:hover:border-sky-700"
            >
              <div>
                <div className="flex items-center space-x-1.5 text-xs font-extrabold text-slate-900 dark:text-white">
                  <Table2 className="h-4 w-4 text-sky-600 dark:text-sky-400" />
                  <span>Explore Marts</span>
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Browse {currentOverview?.marts_count ?? 0} Parquet tables & export CSV
                </p>
              </div>
              <ArrowRight className="h-4 w-4 text-slate-400" />
            </button>

            <button
              type="button"
              data-testid="goto-quality-btn"
              onClick={() => setActiveTab('quality')}
              className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50/70 p-4 text-left transition hover:border-sky-400 hover:bg-sky-50/40 dark:border-slate-800 dark:bg-slate-800/50 dark:hover:border-sky-700"
            >
              <div>
                <div className="flex items-center space-x-1.5 text-xs font-extrabold text-slate-900 dark:text-white">
                  <ShieldAlert className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                  <span>Quality & Anomalies</span>
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Inspect DQ dimensions & v2 anomaly framing
                </p>
              </div>
              <ArrowRight className="h-4 w-4 text-slate-400" />
            </button>

            <button
              type="button"
              data-testid="goto-lineage-btn"
              onClick={() => setActiveTab('lineage')}
              className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50/70 p-4 text-left transition hover:border-sky-400 hover:bg-sky-50/40 dark:border-slate-800 dark:bg-slate-800/50 dark:hover:border-sky-700"
            >
              <div>
                <div className="flex items-center space-x-1.5 text-xs font-extrabold text-slate-900 dark:text-white">
                  <GitBranch className="h-4 w-4 text-indigo-600 dark:text-indigo-400" />
                  <span>Data Lineage DAG</span>
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Trace raw → staging → mart provenance
                </p>
              </div>
              <ArrowRight className="h-4 w-4 text-slate-400" />
            </button>

            <button
              type="button"
              data-testid="download-summary-btn"
              disabled={downloadingSummary}
              onClick={handleDownloadSummary}
              className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50/70 p-4 text-left transition hover:border-sky-400 hover:bg-sky-50/40 disabled:opacity-50 dark:border-slate-800 dark:bg-slate-800/50 dark:hover:border-sky-700"
            >
              <div>
                <div className="flex items-center space-x-1.5 text-xs font-extrabold text-slate-900 dark:text-white">
                  <Download className="h-4 w-4 text-amber-600 dark:text-amber-400" />
                  <span>Download Summary</span>
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Export final_summary_report.json
                </p>
              </div>
              <ArrowRight className="h-4 w-4 text-slate-400" />
            </button>
          </div>
        </div>
      )}

      {/* Run History Card */}
      <div className="rounded-3xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
        <div className="flex items-center justify-between border-b border-slate-200 p-5 dark:border-slate-800">
          <div>
            <h3 className="text-sm font-bold text-slate-900 dark:text-white">
              Recorded Execution History ({runs.length})
            </h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Persisted by Quaero's SQLite store (<span className="font-mono">db/store.py</span>) with state transitions.
            </p>
          </div>

          <button
            onClick={loadHistory}
            disabled={loadingHistory}
            className="flex items-center space-x-1.5 rounded-xl border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loadingHistory ? 'animate-spin' : ''}`} />
            <span>Refresh</span>
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead className="border-b border-slate-100 bg-slate-50/70 text-[11px] font-semibold text-slate-500 dark:border-slate-800 dark:bg-slate-800/40 dark:text-slate-400">
              <tr>
                <th className="px-5 py-3">Status</th>
                <th className="px-5 py-3">Run ID</th>
                <th className="px-5 py-3">Duration</th>
                <th className="px-5 py-3">Created</th>
                <th className="px-5 py-3">Question</th>
                <th className="px-5 py-3 text-right">Details</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
              {runs.map((r) => {
                const isCompleted = r.status === 'COMPLETED';
                const isFailed = r.status === 'FAILED';
                const isRunning = r.status === 'RUNNING' || r.status === 'QUEUED';

                return (
                  <tr key={r.run_id} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40">
                    <td className="px-5 py-3.5 font-sans">
                      <span
                        className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-[11px] font-bold ${
                          isCompleted
                            ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-400'
                            : isFailed
                            ? 'bg-rose-50 text-rose-700 dark:bg-rose-950/60 dark:text-rose-400'
                            : 'bg-sky-50 text-sky-700 dark:bg-sky-950/60 dark:text-sky-400'
                        }`}
                      >
                        {isCompleted && <CheckCircle2 className="mr-1 h-3 w-3" />}
                        {isFailed && <XCircle className="mr-1 h-3 w-3" />}
                        {isRunning && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
                        {r.status}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 text-slate-600 dark:text-slate-400 font-mono">
                      {r.run_id}
                    </td>
                    <td className="px-5 py-3.5 text-slate-700 dark:text-slate-300">
                      {formatDuration(r.started_at, r.completed_at)}
                    </td>
                    <td className="px-5 py-3.5 text-slate-500 dark:text-slate-400 font-sans">
                      {new Date(r.created_at).toLocaleDateString()} {new Date(r.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </td>
                    <td className="max-w-xs truncate px-5 py-3.5 text-slate-700 dark:text-slate-300 font-sans" title={r.question || ''}>
                      {r.question || '—'}
                    </td>
                    <td className="px-5 py-3.5 text-right font-sans">
                      {r.error ? (
                        <button
                          onClick={() => setSelectedError(r.error || 'Unknown error')}
                          className="rounded-lg border border-rose-200 px-2.5 py-1 text-[11px] font-semibold text-rose-700 hover:bg-rose-50 dark:border-rose-800 dark:text-rose-300 dark:hover:bg-rose-950/40"
                        >
                          View Error
                        </button>
                      ) : (
                        <span className="text-[11px] text-slate-400">Success</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Error modal */}
      {selectedError && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 p-4">
          <div className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-5 shadow-xl dark:border-slate-800 dark:bg-slate-900">
            <div className="flex items-center space-x-2 text-rose-600 dark:text-rose-400">
              <AlertTriangle className="h-5 w-5" />
              <h4 className="text-sm font-bold">Pipeline Error Diagnostics</h4>
            </div>
            <pre className="mt-3 overflow-x-auto rounded-xl bg-slate-950 p-3 font-mono text-xs text-rose-300">
              {selectedError}
            </pre>
            <div className="mt-4 flex justify-end">
              <button
                onClick={() => setSelectedError(null)}
                className="rounded-xl bg-slate-100 px-3.5 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
