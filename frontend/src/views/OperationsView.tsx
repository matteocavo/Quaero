import React, { useState, useEffect } from 'react';
import {
  PlayCircle,
  RefreshCw,
  Clock,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Loader2,
  Sparkles,
  ArrowRight,
  Info,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { RunHistoryItem } from '../types/api';

export const OperationsView: React.FC = () => {
  const {
    selectedProject,
    currentOverview,
    apiClient,
    mode,
    showFlash,
    setActiveTab,
  } = useApp();

  const [question, setQuestion] = useState('');
  const [sourceUrl, setSourceUrl] = useState('');
  const [runs, setRuns] = useState<RunHistoryItem[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [selectedError, setSelectedError] = useState<string | null>(null);

  // Load history
  const loadHistory = async () => {
    setLoadingHistory(true);
    try {
      const res = await apiClient.getRunHistory(selectedProject);
      setRuns(res.runs);
    } catch (err: any) {
      showFlash(`Could not load run history: ${err.message}`, 'error');
    } finally {
      setLoadingHistory(false);
    }
  };

  useEffect(() => {
    loadHistory();
    if (currentOverview?.question) {
      setQuestion(currentOverview.question);
    }
  }, [selectedProject, currentOverview]);

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
      );
      showFlash(`Run accepted & queued: ${res.run_id}`, 'success');
      loadHistory();
    } catch (err: any) {
      showFlash(`Execution rejected: ${err.message}`, 'error');
    } finally {
      setSubmitting(false);
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

  return (
    <div className="space-y-6">
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
