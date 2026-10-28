import React, { useState, useEffect, useMemo } from 'react';
import { adviseUploadSetup } from '../analysis/advisor';
import {
  Upload,
  FolderPlus,
  PlayCircle,
  CheckCircle2,
  FileSpreadsheet,
  Sparkles,
  ShieldCheck,
  Loader2,
  Database,
  Sliders,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import {
  ProjectCreateResponse,
  RunAcceptedResponse,
  UploadResponse,
} from '../types/api';

interface LocalPipelineWizardProps {
  onProjectCreated?: (
    created: ProjectCreateResponse,
    launchedRun: boolean,
    runAccepted?: RunAcceptedResponse,
  ) => void;
  compact?: boolean;
}

function toProjectSlug(raw: string): string {
  return raw
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 64);
}

export const LocalPipelineWizard: React.FC<LocalPipelineWizardProps> = ({
  onProjectCreated,
  compact = false,
}) => {
  const {
    apiClient,
    uploads,
    selectedUploadId,
    setSelectedUploadId,
    refreshUploads,
    refreshProjects,
    showFlash,
  } = useApp();

  const [uploading, setUploading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [projectName, setProjectName] = useState('');
  const [question, setQuestion] = useState('');
  const [metricColumn, setMetricColumn] = useState('');
  const [dimensionColumn, setDimensionColumn] = useState('');
  const [timeColumn, setTimeColumn] = useState('');
  const [formError, setFormError] = useState<string | null>(null);

  const selectedUpload: UploadResponse | undefined = useMemo(() => {
    if (!uploads.length) return undefined;
    if (selectedUploadId) {
      return uploads.find((u) => u.upload_id === selectedUploadId) || uploads[0];
    }
    return uploads[0];
  }, [uploads, selectedUploadId]);

  const applyUploadDefaults = (upload: UploadResponse) => {
    const stem = upload.original_filename.replace(/\.[^.]+$/, '');
    const defaultSlug = toProjectSlug(stem) || 'local_analysis';
    setProjectName(defaultSlug);

    // The browser rules pick a measure that varies (not the first numeric column, which may be all zeros).
    // They fall back to the server's own lists when the rules find nothing.
    const advice = adviseUploadSetup(upload);
    const defaultMetric = advice.metric || upload.numeric_columns[0] || '';
    const defaultDim =
      advice.dimension ||
      upload.candidate_dimensions.find((d) => d !== defaultMetric) ||
      upload.columns[0]?.normalized_name ||
      '';
    const defaultTime = advice.time || upload.candidate_time_columns[0] || '';

    setMetricColumn(defaultMetric);
    setDimensionColumn(defaultDim);
    setTimeColumn(defaultTime);

    if (defaultMetric && defaultDim) {
      setQuestion(
        `Which ${defaultDim.replace(/_/g, ' ')} generates the highest average ${defaultMetric.replace(/_/g, ' ')}?`,
      );
    } else {
      setQuestion(`What are the key patterns in ${stem.replace(/_/g, ' ')}?`);
    }
  };

  // Populate sensible defaults whenever a different upload is selected
  useEffect(() => {
    if (!selectedUpload) return;
    applyUploadDefaults(selectedUpload);
  }, [selectedUpload?.upload_id]);

  const setupAdvice = useMemo(() => (selectedUpload ? adviseUploadSetup(selectedUpload) : null), [selectedUpload]);

  const suggestedQuestions = useMemo(() => {
    if (!selectedUpload) return [];
    const m = metricColumn || selectedUpload.numeric_columns[0] || 'records';
    const d =
      dimensionColumn ||
      selectedUpload.candidate_dimensions[0] ||
      selectedUpload.columns[0]?.normalized_name ||
      'category';
    const mLabel = m.replace(/_/g, ' ');
    const dLabel = d.replace(/_/g, ' ');
    return [
      `Which ${dLabel} generates the highest average ${mLabel}?`,
      `What is the total ${mLabel} by ${dLabel}?`,
      `Which ${dLabel} has the highest ${mLabel} across the dataset?`,
    ];
  }, [selectedUpload, metricColumn, dimensionColumn]);

  const handleServerFileUpload = async (file: File) => {
    setFormError(null);
    setUploading(true);
    try {
      const uploaded = await apiClient.uploadCsv(file);
      applyUploadDefaults(uploaded);
      await refreshUploads();
      setSelectedUploadId(uploaded.upload_id);
      showFlash(
        `Uploaded ${uploaded.original_filename} (${uploaded.row_count} rows, ${uploaded.column_count} columns) as ${uploaded.upload_id}.`,
        'success',
      );
    } catch (err: any) {
      const msg = err?.message || 'Failed to upload CSV file.';
      setFormError(msg);
      showFlash(`CSV upload failed: ${msg}`, 'error');
    } finally {
      setUploading(false);
    }
  };

  const handleCreateProject = async (launchImmediately: boolean) => {
    setFormError(null);
    if (!selectedUpload) {
      setFormError('Upload or select a CSV dataset first.');
      return;
    }
    const slug = toProjectSlug(projectName);
    if (!slug) {
      setFormError('Enter a valid project name (letters, digits, underscores, or hyphens).');
      return;
    }
    if (!question.trim()) {
      setFormError('Enter an analytical question or objective for this project.');
      return;
    }

    setCreating(true);
    try {
      const created = await apiClient.createProject({
        project: projectName.trim(),
        upload_id: selectedUpload.upload_id,
        question: question.trim(),
        metric_column: metricColumn || null,
        dimension_column: dimensionColumn || null,
        time_column: timeColumn || null,
      });

      await refreshProjects(created.project);

      if (launchImmediately) {
        const runAccepted = await apiClient.startRun(
          created.project,
          created.question,
          undefined,
          created.metric_column || undefined,
          created.dimension_column || undefined,
        );
        showFlash(
          `Project '${created.project}' created and Python pipeline started (${runAccepted.run_id}).`,
          'success',
        );
        onProjectCreated?.(created, true, runAccepted);
      } else {
        showFlash(
          `Project '${created.project}' created from ${selectedUpload.original_filename}. Ready to run pipeline.`,
          'success',
        );
        onProjectCreated?.(created, false);
      }
    } catch (err: any) {
      const msg = err?.message || 'Could not create project.';
      setFormError(msg);
      showFlash(msg, 'error');
    } finally {
      setCreating(false);
    }
  };

  const normalizedPreview = toProjectSlug(projectName);

  return (
    <div
      data-testid="local-pipeline-wizard"
      className="rounded-3xl border border-emerald-200 bg-white p-6 shadow-2xs dark:border-emerald-900/60 dark:bg-slate-900"
    >
      {/* Header & Workflow Stepper */}
      <div className="flex flex-col justify-between gap-4 border-b border-slate-100 pb-4 dark:border-slate-800 lg:flex-row lg:items-center">
        <div>
          <div className="flex items-center space-x-2">
            <span className="rounded-full bg-emerald-100 px-2.5 py-0.5 text-[10px] font-extrabold uppercase tracking-wider text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
              Local Operations Pipeline
            </span>
            <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">
              FastAPI + Python ETL + Parquet Marts
            </span>
          </div>
          <h3 className="mt-1 text-base font-extrabold text-slate-900 dark:text-white sm:text-lg">
            {compact
              ? 'Upload CSV & Create Local Project'
              : 'Local Dataset Pipeline: Import → Configure → Run → Results'}
          </h3>
          <p className="text-xs text-slate-600 dark:text-slate-400">
            Upload a local CSV to the untracked <code className="font-mono">uploads/</code> workspace, create a reusable project config, and execute the Python pipeline.
          </p>
        </div>

        {/* 4-Step Visual Indicator */}
        <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-bold">
          <span className="inline-flex items-center rounded-lg bg-sky-100 px-2.5 py-1 text-sky-900 dark:bg-sky-950 dark:text-sky-300">
            1. Import CSV
          </span>
          <span className="text-slate-400">→</span>
          <span
            className={`inline-flex items-center rounded-lg px-2.5 py-1 ${
              selectedUpload
                ? 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-300'
                : 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
            }`}
          >
            2. Configure Project
          </span>
          <span className="text-slate-400">→</span>
          <span className="inline-flex items-center rounded-lg bg-slate-100 px-2.5 py-1 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
            3. Run Pipeline
          </span>
          <span className="text-slate-400">→</span>
          <span className="inline-flex items-center rounded-lg bg-slate-100 px-2.5 py-1 text-slate-600 dark:bg-slate-800 dark:text-slate-300">
            4. Explore Results
          </span>
        </div>
      </div>

      {formError && (
        <div
          role="alert"
          data-testid="local-wizard-error"
          className="mt-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-xs font-medium text-rose-800 dark:border-rose-900 dark:bg-rose-950/50 dark:text-rose-200"
        >
          {formError}
        </div>
      )}

      <div className="mt-5 grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* STEP 1: IMPORT CSV TO FASTAPI */}
        <div className="space-y-4 lg:col-span-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <Database className="h-4 w-4 text-sky-600 dark:text-sky-400" />
              <h4 className="text-xs font-extrabold uppercase tracking-wider text-slate-800 dark:text-slate-200">
                Step 1 · Upload CSV Dataset
              </h4>
            </div>
            {uploads.length > 0 && (
              <span className="text-[11px] font-semibold text-slate-500 dark:text-slate-400">
                {uploads.length} uploaded
              </span>
            )}
          </div>

          <label
            htmlFor="local-csv-upload-input"
            className="flex cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed border-sky-300 bg-sky-50/50 p-5 text-center transition hover:border-sky-500 hover:bg-sky-50 dark:border-sky-800 dark:bg-sky-950/20 dark:hover:border-sky-600"
          >
            {uploading ? (
              <Loader2 className="h-6 w-6 animate-spin text-sky-600 dark:text-sky-400" />
            ) : (
              <Upload className="h-6 w-6 text-sky-600 dark:text-sky-400" />
            )}
            <span className="mt-2 text-xs font-bold text-sky-800 dark:text-sky-300">
              {uploading ? 'Validating & storing CSV...' : 'Upload CSV to Local FastAPI Workspace'}
            </span>
            <span className="mt-0.5 text-[11px] text-slate-500 dark:text-slate-400">
              UTF-8 .csv, .tsv, or .txt · Stored in local untracked <code className="font-mono">uploads/</code>
            </span>
            <input
              id="local-csv-upload-input"
              data-testid="local-csv-upload-input"
              type="file"
              accept=".csv,.tsv,.txt,text/csv,text/plain"
              disabled={uploading}
              onChange={(e) => {
                if (e.target.files && e.target.files.length > 0) {
                  handleServerFileUpload(e.target.files[0]);
                  e.target.value = '';
                }
              }}
              className="sr-only"
            />
          </label>

          {uploads.length > 0 && (
            <div>
              <label
                htmlFor="local-upload-select"
                className="block text-xs font-semibold text-slate-700 dark:text-slate-300"
              >
                Selected Uploaded Dataset
              </label>
              <select
                id="local-upload-select"
                data-testid="local-upload-select"
                value={selectedUpload?.upload_id || ''}
                onChange={(e) => setSelectedUploadId(e.target.value)}
                className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
              >
                {uploads.map((u) => (
                  <option key={u.upload_id} value={u.upload_id}>
                    {u.original_filename} ({u.row_count.toLocaleString()} rows × {u.column_count} cols · {u.upload_id})
                  </option>
                ))}
              </select>
            </div>
          )}

          {selectedUpload && (
            <div
              data-testid="upload-metadata-card"
              className="rounded-2xl border border-slate-200 bg-slate-50/80 p-3.5 text-xs dark:border-slate-800 dark:bg-slate-800/50"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-1.5 font-bold text-slate-900 dark:text-white">
                  <FileSpreadsheet className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                  <span className="truncate">{selectedUpload.original_filename}</span>
                </div>
                <span className="rounded bg-emerald-100 px-2 py-0.5 font-mono text-[10px] font-bold text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
                  {selectedUpload.upload_id}
                </span>
              </div>

              <div className="mt-2 grid grid-cols-3 gap-2 text-[11px]">
                <div className="rounded-lg bg-white p-2 dark:bg-slate-900">
                  <div className="text-slate-400">Rows</div>
                  <div className="font-mono font-bold text-slate-800 dark:text-slate-200">
                    {selectedUpload.row_count.toLocaleString()}
                  </div>
                </div>
                <div className="rounded-lg bg-white p-2 dark:bg-slate-900">
                  <div className="text-slate-400">Columns</div>
                  <div className="font-mono font-bold text-slate-800 dark:text-slate-200">
                    {selectedUpload.column_count}
                  </div>
                </div>
                <div className="rounded-lg bg-white p-2 dark:bg-slate-900">
                  <div className="text-slate-400">Size</div>
                  <div className="font-mono font-bold text-slate-800 dark:text-slate-200">
                    {(selectedUpload.size_bytes / 1024).toFixed(1)} KB
                  </div>
                </div>
              </div>

              <div className="mt-2.5 flex flex-wrap gap-1">
                {selectedUpload.columns.slice(0, 8).map((col) => (
                  <span
                    key={col.normalized_name}
                    className="inline-flex items-center rounded-md border border-slate-200 bg-white px-2 py-0.5 font-mono text-[10px] text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300"
                    title={`Sample: ${col.sample_values.join(', ')}`}
                  >
                    <span className="font-bold">{col.normalized_name}</span>
                    <span className="ml-1 text-slate-400">({col.dtype})</span>
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* STEP 2: CONFIGURE PROJECT & LAUNCH PIPELINE */}
        <div className="space-y-4 lg:col-span-7">
          <div className="flex items-center space-x-2">
            <Sliders className="h-4 w-4 text-sky-600 dark:text-sky-400" />
            <h4 className="text-xs font-extrabold uppercase tracking-wider text-slate-800 dark:text-slate-200">
              Step 2 · Configure Project & Analysis Objective
            </h4>
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div>
              <label
                htmlFor="local-project-name-input"
                className="block text-xs font-semibold text-slate-700 dark:text-slate-300"
              >
                Project Name
              </label>
              <input
                id="local-project-name-input"
                data-testid="local-project-name-input"
                type="text"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                placeholder="e.g. regional_sales_2024"
                className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
              />
              {normalizedPreview && (
                <div className="mt-1 text-[10px] text-slate-500 dark:text-slate-400">
                  Project directory: <code className="font-mono">projects/{normalizedPreview}/</code>
                </div>
              )}
            </div>

            <div className="grid grid-cols-3 gap-2">
              <div>
                <label
                  htmlFor="local-metric-select"
                  className="block text-xs font-semibold text-slate-700 dark:text-slate-300"
                >
                  Metric (Optional)
                </label>
                <select
                  id="local-metric-select"
                  data-testid="local-metric-select"
                  value={metricColumn}
                  onChange={(e) => setMetricColumn(e.target.value)}
                  disabled={!selectedUpload}
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                >
                  <option value="">Auto-infer</option>
                  {selectedUpload?.numeric_columns.map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label
                  htmlFor="local-dimension-select"
                  className="block text-xs font-semibold text-slate-700 dark:text-slate-300"
                >
                  Dimension (Optional)
                </label>
                <select
                  id="local-dimension-select"
                  data-testid="local-dimension-select"
                  value={dimensionColumn}
                  onChange={(e) => setDimensionColumn(e.target.value)}
                  disabled={!selectedUpload}
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                >
                  <option value="">Auto-infer</option>
                  {selectedUpload?.columns.map((col) => (
                    <option key={col.normalized_name} value={col.normalized_name}>
                      {col.normalized_name}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label
                  htmlFor="local-time-select"
                  className="block text-xs font-semibold text-slate-700 dark:text-slate-300"
                >
                  Time Col (Optional)
                </label>
                <select
                  id="local-time-select"
                  data-testid="local-time-select"
                  value={timeColumn}
                  onChange={(e) => setTimeColumn(e.target.value)}
                  disabled={!selectedUpload}
                  className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
                >
                  <option value="">Auto-detect</option>
                  {selectedUpload?.columns.map((col) => (
                    <option key={col.normalized_name} value={col.normalized_name}>
                      {col.normalized_name}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {setupAdvice && selectedUpload && (setupAdvice.metric || setupAdvice.notes.length > 0) && (
            <div
              data-testid="setup-advice"
              className="rounded-xl border border-sky-200 bg-sky-50 p-3 text-[11px] text-sky-950 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-100"
            >
              <div className="font-bold">Suggested setup</div>
              <p className="mt-0.5">
                Chosen by fixed rules in your browser from the upload's columns, not by the Python pipeline:{' '}
                {setupAdvice.metric ? (
                  <>
                    measure <span className="font-mono font-semibold">{setupAdvice.metric}</span>
                  </>
                ) : (
                  'no numeric measure found'
                )}
                {setupAdvice.dimension && (
                  <>
                    , group by <span className="font-mono font-semibold">{setupAdvice.dimension}</span>
                  </>
                )}
                {setupAdvice.time && (
                  <>
                    , time <span className="font-mono font-semibold">{setupAdvice.time}</span>
                  </>
                )}
                .
              </p>
              {setupAdvice.notes.length > 0 && (
                <ul className="mt-1 list-disc pl-4">
                  {setupAdvice.notes.map((n) => (
                    <li key={`${n.column}-${n.text}`}>
                      <span className="font-mono font-semibold">{n.column}</span>: {n.text}
                    </li>
                  ))}
                </ul>
              )}
              <button
                type="button"
                data-testid="use-suggested-setup"
                onClick={() => {
                  setMetricColumn(setupAdvice.metric || '');
                  setDimensionColumn(setupAdvice.dimension || '');
                  setTimeColumn(setupAdvice.time || '');
                }}
                className="mt-2 rounded-lg border border-sky-300 bg-white px-2.5 py-1 font-semibold text-sky-900 hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-100"
              >
                Use suggested setup
              </button>
            </div>
          )}

          <div>
            <div className="flex items-center justify-between">
              <label
                htmlFor="local-project-question-input"
                className="text-xs font-semibold text-slate-700 dark:text-slate-300"
              >
                Analytical Question / Objective
              </label>
              <span className="text-[10px] text-slate-400">{question.length}/1000</span>
            </div>
            <textarea
              id="local-project-question-input"
              data-testid="local-project-question-input"
              rows={2}
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="e.g. Which region generates the highest average revenue?"
              className="mt-1 w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-900 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white"
            />
            {suggestedQuestions.length > 0 && (
              <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <span className="text-[10px] font-semibold text-slate-400">Suggestions:</span>
                {suggestedQuestions.map((q) => (
                  <button
                    key={q}
                    type="button"
                    onClick={() => setQuestion(q)}
                    className="inline-flex items-center space-x-1 rounded-lg border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-medium text-slate-700 hover:bg-sky-50 hover:text-sky-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
                  >
                    <Sparkles className="h-2.5 w-2.5 text-sky-600 dark:text-sky-400" />
                    <span>{q}</span>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Supported Pipeline Data Validation Settings */}
          <div className="rounded-2xl border border-slate-200 bg-slate-50/70 p-3 text-[11px] dark:border-slate-800 dark:bg-slate-800/40">
            <div className="flex items-center space-x-1.5 font-bold text-slate-800 dark:text-slate-200">
              <ShieldCheck className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />
              <span>Active Pipeline Validation & Quality Rules (Quaero Python Engine)</span>
            </div>
            <div className="mt-1.5 flex flex-wrap gap-1.5 text-[10px] text-slate-600 dark:text-slate-300">
              <span className="inline-flex items-center rounded-md bg-white px-2 py-0.5 font-medium dark:bg-slate-900">
                <CheckCircle2 className="mr-1 h-3 w-3 text-emerald-600" />
                UTF-8 & Structural CSV Check
              </span>
              <span className="inline-flex items-center rounded-md bg-white px-2 py-0.5 font-medium dark:bg-slate-900">
                <CheckCircle2 className="mr-1 h-3 w-3 text-emerald-600" />
                Duplicate Row Removal & Trimming
              </span>
              <span className="inline-flex items-center rounded-md bg-white px-2 py-0.5 font-medium dark:bg-slate-900">
                <CheckCircle2 className="mr-1 h-3 w-3 text-emerald-600" />
                Quality Flags (has_nulls, invalid_date_parse)
              </span>
              <span className="inline-flex items-center rounded-md bg-white px-2 py-0.5 font-medium dark:bg-slate-900">
                <CheckCircle2 className="mr-1 h-3 w-3 text-emerald-600" />
                DQ Scoring (40% Completeness / 20% Uniqueness / 40% Format)
              </span>
              <span className="inline-flex items-center rounded-md bg-white px-2 py-0.5 font-medium dark:bg-slate-900">
                <CheckCircle2 className="mr-1 h-3 w-3 text-emerald-600" />
                IQR & 3σ Z-Score Anomaly Framing (v2)
              </span>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-end gap-2.5 pt-1">
            <button
              type="button"
              data-testid="create-project-btn"
              disabled={creating || !selectedUpload}
              onClick={() => handleCreateProject(false)}
              className="inline-flex items-center space-x-1.5 rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-xs font-bold text-slate-800 shadow-2xs transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
            >
              {creating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <FolderPlus className="h-4 w-4 text-sky-600 dark:text-sky-400" />
              )}
              <span>Create Local Project</span>
            </button>

            <button
              type="button"
              data-testid="create-and-run-btn"
              disabled={creating || !selectedUpload}
              onClick={() => handleCreateProject(true)}
              className="inline-flex items-center space-x-1.5 rounded-2xl bg-sky-600 px-5 py-2.5 text-xs font-bold text-white shadow-xs transition hover:bg-sky-500 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-sky-500 dark:hover:bg-sky-400"
            >
              {creating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <PlayCircle className="h-4 w-4" />
              )}
              <span>Create Project & Run Python Pipeline</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
