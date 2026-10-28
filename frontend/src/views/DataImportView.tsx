import React, { useState, useMemo } from 'react';
import {
  Upload,
  FileSpreadsheet,
  CheckCircle2,
  AlertTriangle,
  Info,
  BarChart2,
  Download,
  Search,
  Sparkles,
  ArrowRight,
  ShieldCheck,
  Layers,
  PlayCircle,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { toCsv } from '../utils/csv';
import { downloadTextFile, fileStem } from '../utils/download';
import { parseCSV, DatasetProfile, ColumnProfile } from '../utils/csvProfiler';

export const DataImportView: React.FC = () => {
  const {
    activeDatasetProfile,
    setActiveDatasetProfile,
    loadSampleDataset,
    setActiveTab,
    showFlash,
  } = useApp();

  const [dragOver, setDragOver] = useState(false);
  const [selectedColumnName, setSelectedColumnName] = useState<string>('');
  const [previewPage, setPreviewPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(10);
  const [filterQuery, setFilterQuery] = useState<string>('');

  const profile = activeDatasetProfile;

  // Selected column for interactive distribution
  const selectedColumn: ColumnProfile | undefined = useMemo(() => {
    if (!profile) return undefined;
    if (selectedColumnName) {
      return profile.columns.find((c) => c.name === selectedColumnName) || profile.columns[0];
    }
    return profile.columns[0];
  }, [profile, selectedColumnName]);

  const handleFileUpload = (file: File) => {
    if (!file.name.endsWith('.csv') && file.type !== 'text/csv' && file.type !== 'text/plain') {
      showFlash('Please select a valid CSV file (.csv).', 'error');
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string;
        const newProfile = parseCSV(text, file.name);
        setActiveDatasetProfile(newProfile);
        if (newProfile.columns.length > 0) {
          setSelectedColumnName(newProfile.columns[0].name);
        }
        setPreviewPage(1);
        showFlash(`Successfully parsed ${file.name} in-browser (${newProfile.totalRows} rows, ${newProfile.totalColumns} columns).`, 'success');
      } catch (err: any) {
        showFlash(`CSV parsing failed: ${err.message}`, 'error');
      }
    };
    reader.onerror = () => {
      showFlash('Error reading file from disk.', 'error');
    };
    reader.readAsText(file);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      handleFileUpload(e.dataTransfer.files[0]);
    }
  };

  const filteredPreviewRows = useMemo(() => {
    if (!profile) return [];
    if (!filterQuery.trim()) return profile.allRows;
    const q = filterQuery.toLowerCase();
    return profile.allRows.filter((row) =>
      Object.values(row).some((val) => String(val ?? '').toLowerCase().includes(q)),
    );
  }, [profile, filterQuery]);

  const totalPages = profile ? Math.max(1, Math.ceil(filteredPreviewRows.length / pageSize)) : 1;
  const paginatedRows = useMemo(() => {
    const start = (previewPage - 1) * pageSize;
    return filteredPreviewRows.slice(start, start + pageSize);
  }, [filteredPreviewRows, previewPage, pageSize]);

  // Download parsed CSV
  const downloadCSV = () => {
    if (!profile) return;
    downloadTextFile(
      `cleaned_${profile.fileName}`,
      toCsv(profile.headers, profile.allRows),
      'text/csv;charset=utf-8;',
    );
  };

  // Download JSON audit report
  const downloadAuditJSON = () => {
    if (!profile) return;
    const audit = {
      file: profile.fileName,
      total_rows: profile.totalRows,
      total_columns: profile.totalColumns,
      duplicate_rows: profile.duplicateRows,
      overall_dq_score: profile.overallDqScore,
      columns: profile.columns,
      analysis_type: 'Browser-Side Local Profile (Not Full Python Pipeline)',
      generated_at: new Date().toISOString(),
      generator: 'Quaero Web v0.7.0 Browser Profiler',
    };
    downloadTextFile(
      `dq_audit_${fileStem(profile.fileName)}.json`,
      JSON.stringify(audit, null, 2),
      'application/json',
    );
  };

  return (
    <div className="space-y-6">
      {/* Header Bar */}
      <div className="flex flex-col justify-between space-y-3 sm:flex-row sm:items-center sm:space-y-0">
        <div>
          <div className="flex items-center space-x-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-sky-100 text-sky-700 dark:bg-sky-950 dark:text-sky-300">
              <FileSpreadsheet className="h-5 w-5" />
            </div>
            <h2 className="text-xl font-extrabold tracking-tight text-slate-900 dark:text-white sm:text-2xl">
              Data Import & Browser Profiler
            </h2>
          </div>
          <p className="mt-1 text-xs text-slate-600 dark:text-slate-300 sm:text-sm">
            Client-side data exploration, automated column datatype inference, and Quaero Data Quality audits.
          </p>
        </div>

        {/* Sample selector buttons */}
        <div className="flex flex-wrap items-center gap-2">
          <button
            onClick={() => loadSampleDataset('release_impact')}
            className="flex items-center space-x-1.5 rounded-xl border border-sky-300 bg-sky-50 px-3.5 py-2 text-xs font-bold text-sky-900 shadow-2xs transition hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950/60 dark:text-sky-300 dark:hover:bg-sky-900/60"
            title="Load music catalog streams sample"
          >
            <Sparkles className="h-3.5 w-3.5 text-sky-700 dark:text-sky-400" />
            <span>Sample: release_impact.csv</span>
          </button>
          <button
            onClick={() => loadSampleDataset('global_indicators')}
            className="flex items-center space-x-1.5 rounded-xl border border-slate-300 bg-white px-3.5 py-2 text-xs font-bold text-slate-800 shadow-2xs transition hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-750"
            title="Load an illustrative macro indicators sample (rounded values, not an official World Bank extract)"
          >
            <Sparkles className="h-3.5 w-3.5 text-slate-600 dark:text-slate-400" />
            <span>Sample: global_indicators.csv</span>
          </button>
        </div>
      </div>

      {/* Scope Disclaimer Banner with Strict WCAG Contrast */}
      <div className="flex items-start space-x-3 rounded-2xl border border-sky-200 bg-sky-50/90 p-4 text-xs text-sky-950 shadow-2xs dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-100">
        <Info className="mt-0.5 h-4 w-4 shrink-0 text-sky-700 dark:text-sky-400" />
        <div className="space-y-1">
          <div className="flex items-center space-x-2">
            <span className="font-bold text-sky-950 dark:text-sky-100">Workflow Scope:</span>
            <span className="rounded bg-sky-200/80 px-2 py-0.5 font-mono text-[10px] font-bold text-sky-950 dark:bg-sky-900 dark:text-sky-200">
              STAGE 01 — CLIENT-SIDE EXPLORATION
            </span>
          </div>
          <p className="text-xs leading-relaxed text-sky-900 dark:text-sky-200">
            This screen parses and profiles datasets <strong>in-memory inside your browser</strong>. Uploaded data is not submitted to FastAPI, nor does it generate backend Parquet marts. To run the full Python pipeline, save your dataset to <span className="rounded bg-sky-100 px-1 py-0.5 font-mono text-[11px] font-semibold text-sky-950 dark:bg-sky-900 dark:text-sky-100">sample_data/</span> or provide a source URL in Local Operations mode.
          </p>
        </div>
      </div>

      {/* Upload Dropzone */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        className={`relative flex flex-col items-center justify-center rounded-3xl border-2 border-dashed p-8 text-center transition ${
          dragOver
            ? 'border-sky-500 bg-sky-50/70 dark:border-sky-400 dark:bg-sky-950/30'
            : 'border-slate-300 bg-white hover:border-slate-400 hover:bg-slate-50/40 dark:border-slate-700 dark:bg-slate-900 dark:hover:border-slate-600'
        }`}
      >
        <div className="rounded-2xl bg-sky-50 p-3.5 text-sky-700 dark:bg-sky-950/70 dark:text-sky-400">
          <Upload className="h-7 w-7" />
        </div>
        <div className="mt-3">
          <label htmlFor="csv-input" className="cursor-pointer font-bold text-sky-700 hover:text-sky-800 dark:text-sky-400 dark:hover:text-sky-300">
            Click to upload your CSV
          </label>
          <span className="font-medium text-slate-700 dark:text-slate-300"> or drag and drop a file</span>
          <input
            id="csv-input"
            type="file"
            accept=".csv,text/csv"
            onChange={(e) => {
              if (e.target.files && e.target.files.length > 0) {
                handleFileUpload(e.target.files[0]);
              }
            }}
            className="sr-only"
          />
        </div>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Supports CSV with comma, semicolon, or tab delimiters · No server transmission
        </p>
      </div>

      {profile && (
        <>
          {/* Dataset Profiling Metrics */}
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">Dataset File</span>
              <div className="mt-1 truncate font-mono text-xs font-bold text-slate-900 dark:text-white" title={profile.fileName}>
                {profile.fileName}
              </div>
              <span className="text-[11px] font-mono text-slate-500 dark:text-slate-400">delimiter: '{profile.delimiter === '\t' ? '\\t' : profile.delimiter}'</span>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">Total Rows</span>
              <div className="mt-1 text-lg font-bold text-slate-900 dark:text-white">
                {profile.totalRows.toLocaleString()}
              </div>
              <span className="text-[11px] font-mono text-slate-500 dark:text-slate-400">{profile.totalColumns} columns detected</span>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">Duplicate Check</span>
              <div className="mt-1 text-lg font-bold text-slate-900 dark:text-white">
                {profile.duplicateRows}
              </div>
              <span className="text-[11px] text-slate-500 dark:text-slate-400">
                {profile.duplicateRows === 0 ? 'Zero duplicates' : 'Duplicate rows found'}
              </span>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">In-Browser DQ Score</span>
              <div className="mt-1 flex items-baseline space-x-1.5">
                <span className="text-lg font-bold text-sky-700 dark:text-sky-400">
                  {profile.overallDqScore}%
                </span>
                <span className="text-xs font-semibold text-emerald-700 dark:text-emerald-400">Reliable</span>
              </div>
              <div className="mt-1 h-1.5 w-full rounded-full bg-slate-100 dark:bg-slate-800">
                <div
                  className="h-full rounded-full bg-sky-600 dark:bg-sky-500"
                  style={{ width: `${profile.overallDqScore}%` }}
                />
              </div>
            </div>

            <div className="flex flex-col justify-between rounded-2xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <span className="text-xs font-semibold text-slate-500 dark:text-slate-400">Export Analysis</span>
              <div className="mt-1 flex space-x-1.5">
                <button
                  onClick={downloadCSV}
                  className="flex flex-1 items-center justify-center space-x-1 rounded-xl border border-slate-300 bg-slate-50 px-2 py-1.5 text-xs font-bold text-slate-800 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-750"
                  title="Export cleaned CSV"
                >
                  <Download className="h-3 w-3" />
                  <span>CSV</span>
                </button>
                <button
                  onClick={downloadAuditJSON}
                  className="flex flex-1 items-center justify-center space-x-1 rounded-xl border border-slate-300 bg-slate-50 px-2 py-1.5 text-xs font-bold text-slate-800 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-750"
                  title="Export JSON Quality Audit Report"
                >
                  <Download className="h-3 w-3" />
                  <span>Audit</span>
                </button>
              </div>
            </div>
          </div>

          {/* Column Profile Table & Distribution Visualizer */}
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
            {/* Column Audit Table (2 cols) */}
            <div className="rounded-3xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900 lg:col-span-2">
              <div className="flex items-center justify-between border-b border-slate-100 pb-3 dark:border-slate-800">
                <div>
                  <h3 className="text-xs font-bold uppercase tracking-wider text-slate-700 dark:text-slate-300">
                    Column-Level Schema & Data Quality Audit ({profile.columns.length})
                  </h3>
                  <p className="text-xs text-slate-500 dark:text-slate-400">
                    Computed using completeness (40%), uniqueness (20%), and formatting consistency (40%). Click a row to chart.
                  </p>
                </div>
              </div>

              <div className="mt-3 overflow-x-auto">
                <table className="w-full text-left text-xs font-mono">
                  <thead className="border-b border-slate-200 bg-slate-50 text-xs font-semibold text-slate-700 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-300">
                    <tr>
                      <th className="py-2.5 pl-3">Column</th>
                      <th className="py-2.5">Type</th>
                      <th className="py-2.5">Completeness</th>
                      <th className="py-2.5">Uniqueness</th>
                      <th className="py-2.5">DQ Score</th>
                      <th className="py-2.5 pr-3 text-right">Outliers</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                    {profile.columns.map((c) => {
                      const isSelected = selectedColumn?.name === c.name;
                      return (
                        <tr
                          key={c.name}
                          onClick={() => setSelectedColumnName(c.name)}
                          className={`cursor-pointer transition hover:bg-slate-50 dark:hover:bg-slate-800/40 ${
                            isSelected ? 'bg-sky-50/90 font-semibold dark:bg-sky-950/40' : ''
                          }`}
                        >
                          <td className="py-2.5 pl-3 font-bold text-slate-900 dark:text-white">
                            {c.name}
                          </td>
                          <td className="py-2.5">
                            <span className="rounded-md border border-slate-200 bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200">
                              {c.dtype}
                            </span>
                          </td>
                          <td className="py-2.5 font-sans">
                            <span className={c.completeness === 100 ? 'font-semibold text-emerald-700 dark:text-emerald-400' : 'font-semibold text-amber-700 dark:text-amber-400'}>
                              {c.completeness}%
                            </span>
                            {c.nullCount > 0 && (
                              <span className="ml-1 font-mono text-[10px] text-slate-500 dark:text-slate-400">({c.nullCount} nulls)</span>
                            )}
                          </td>
                          <td className="py-2.5 font-sans text-slate-700 dark:text-slate-300">
                            {c.uniqueness}%
                          </td>
                          <td className="py-2.5 font-sans font-bold text-sky-800 dark:text-sky-400">
                            {c.dqScore}%
                          </td>
                          <td className="py-2.5 pr-3 text-right">
                            {c.outlierCount > 0 ? (
                              <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-bold text-amber-900 dark:bg-amber-950 dark:text-amber-300">
                                {c.outlierCount} flagged
                              </span>
                            ) : (
                              <span className="font-sans text-[11px] text-slate-500 dark:text-slate-400">0</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Column Visualizer Card (1 col) */}
            <div className="flex flex-col justify-between rounded-3xl border border-slate-200 bg-white p-5 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <div>
                <div className="flex items-center space-x-2 border-b border-slate-100 pb-3 dark:border-slate-800">
                  <BarChart2 className="h-4 w-4 text-sky-700 dark:text-sky-400" />
                  <h3 className="text-xs font-bold uppercase tracking-wider text-slate-700 dark:text-slate-300">
                    Distribution: <span className="font-mono text-sky-700 dark:text-sky-400">{selectedColumn?.name}</span>
                  </h3>
                </div>

                {selectedColumn ? (
                  <div className="mt-4 space-y-4 text-xs">
                    <div className="flex items-center justify-between">
                      <span className="text-slate-600 dark:text-slate-400">Inferred Data Type:</span>
                      <span className="font-mono font-bold text-slate-900 dark:text-white">
                        {selectedColumn.dtype}
                      </span>
                    </div>

                    {selectedColumn.stats?.mean !== undefined && (
                      <div className="space-y-1.5 rounded-xl border border-slate-200 bg-slate-50 p-3.5 dark:border-slate-800 dark:bg-slate-800/50">
                        <div className="flex justify-between">
                          <span className="text-slate-600 dark:text-slate-400">Min → Max:</span>
                          <span className="font-mono font-semibold text-slate-900 dark:text-white">{selectedColumn.stats.min} → {selectedColumn.stats.max}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-slate-600 dark:text-slate-400">Arithmetic Mean:</span>
                          <span className="font-mono font-semibold text-slate-900 dark:text-white">{selectedColumn.stats.mean}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-slate-600 dark:text-slate-400">Median:</span>
                          <span className="font-mono font-semibold text-slate-900 dark:text-white">{selectedColumn.stats.median}</span>
                        </div>
                      </div>
                    )}

                    {/* Frequency bars */}
                    {selectedColumn.stats?.topValues && (
                      <div className="space-y-2">
                        <span className="text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">Top Frequencies:</span>
                        {selectedColumn.stats.topValues.map((item) => {
                          const pct = Math.round((item.count / profile.totalRows) * 100);
                          return (
                            <div key={item.value} className="space-y-1">
                              <div className="flex justify-between text-xs">
                                <span className="max-w-[140px] truncate font-mono font-semibold text-slate-800 dark:text-slate-200">{item.value || '(empty)'}</span>
                                <span className="font-mono text-slate-500 dark:text-slate-400">{item.count} ({pct}%)</span>
                              </div>
                              <div className="h-1.5 w-full rounded-full bg-slate-200 dark:bg-slate-800">
                                <div
                                  className="h-full rounded-full bg-sky-600 dark:bg-sky-500"
                                  style={{ width: `${pct}%` }}
                                />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="py-8 text-center text-xs text-slate-500 dark:text-slate-400">
                    Select a column to chart.
                  </div>
                )}
              </div>

              {/* Next Step Action Button */}
              <div className="mt-6 border-t border-slate-100 pt-4 dark:border-slate-800">
                <span className="text-[10px] font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400">
                  Recommended Next Action:
                </span>
                <button
                  onClick={() => setActiveTab('explorer')}
                  className="mt-2 flex w-full items-center justify-center space-x-1.5 rounded-xl bg-sky-600 py-2.5 text-xs font-bold text-white shadow-2xs hover:bg-sky-500 dark:bg-sky-500 dark:hover:bg-sky-400"
                >
                  <span>Formulate Business Question</span>
                  <ArrowRight className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
          </div>

          {/* Raw Records Preview Table */}
          <div className="rounded-3xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
            <div className="flex flex-col justify-between space-y-2 border-b border-slate-100 p-5 dark:border-slate-800 sm:flex-row sm:items-center sm:space-y-0">
              <div>
                <h3 className="text-xs font-bold uppercase tracking-wider text-slate-700 dark:text-slate-300">
                  In-Memory Records Preview ({filteredPreviewRows.length} rows)
                </h3>
                <p className="text-xs text-slate-500 dark:text-slate-400">
                  Client-side tabular viewer for uploaded or sample rows.
                </p>
              </div>

              <div className="flex items-center space-x-2">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-3 top-2.5 h-3.5 w-3.5 text-slate-400" />
                  <input
                    type="text"
                    value={filterQuery}
                    onChange={(e) => {
                      setFilterQuery(e.target.value);
                      setPreviewPage(1);
                    }}
                    placeholder="Search records..."
                    className="h-8.5 rounded-xl border border-slate-300 bg-white pl-8.5 pr-3 text-xs text-slate-900 placeholder:text-slate-400 focus:border-sky-500 focus:outline-none dark:border-slate-700 dark:bg-slate-800 dark:text-white dark:placeholder:text-slate-500"
                  />
                </div>
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs font-mono">
                <thead className="border-b border-slate-200 bg-slate-50 text-xs font-semibold text-slate-700 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-300">
                  <tr>
                    <th className="w-12 px-4 py-2.5 text-slate-500 dark:text-slate-400">#</th>
                    {profile.headers.map((h) => (
                      <th key={h} className="px-4 py-2.5 font-bold text-slate-800 dark:text-slate-200">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60">
                  {paginatedRows.map((row, idx) => (
                    <tr key={idx} className="hover:bg-slate-50 dark:hover:bg-slate-800/40">
                      <td className="px-4 py-2.5 text-xs text-slate-500 dark:text-slate-400">
                        {(previewPage - 1) * pageSize + idx + 1}
                      </td>
                      {profile.headers.map((h) => (
                        <td key={h} className="px-4 py-2.5 text-slate-800 dark:text-slate-200">
                          {String(row[h] ?? '—')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between border-t border-slate-100 p-4 text-xs text-slate-600 dark:border-slate-800 dark:text-slate-300">
              <span>
                Showing Page {previewPage} of {totalPages}
              </span>
              <div className="flex space-x-1.5">
                <button
                  onClick={() => setPreviewPage((p) => Math.max(1, p - 1))}
                  disabled={previewPage <= 1}
                  className="rounded-xl border border-slate-300 bg-white px-3 py-1 text-xs font-bold text-slate-700 shadow-2xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-750"
                >
                  Prev
                </button>
                <button
                  onClick={() => setPreviewPage((p) => Math.min(totalPages, p + 1))}
                  disabled={previewPage >= totalPages}
                  className="rounded-xl border border-slate-300 bg-white px-3 py-1 text-xs font-bold text-slate-700 shadow-2xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-750"
                >
                  Next
                </button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};
