import React, { useState, useEffect } from 'react';
import {
  ShieldCheck,
  AlertTriangle,
  Info,
  CheckCircle2,
  HelpCircle,
  FileSearch,
  Sparkles,
} from 'lucide-react';
import { useApp } from '../context/AppContext';
import { DataQualityResponse, AnomalyResponse } from '../types/api';

export const QualityView: React.FC = () => {
  const { selectedProject, apiClient, showFlash } = useApp();

  const [dqData, setDqData] = useState<DataQualityResponse | null>(null);
  const [anomalyData, setAnomalyData] = useState<AnomalyResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    let isMounted = true;
    const fetchData = async () => {
      setLoading(true);
      try {
        const [dq, anomalies] = await Promise.allSettled([
          apiClient.getDataQuality(selectedProject),
          apiClient.getAnomalies(selectedProject),
        ]);

        if (!isMounted) return;

        if (dq.status === 'fulfilled') {
          setDqData(dq.value);
        } else {
          setDqData(null);
        }

        if (anomalies.status === 'fulfilled') {
          setAnomalyData(anomalies.value);
        } else {
          setAnomalyData(null);
        }
      } catch (err: any) {
        showFlash(`Could not load quality metrics: ${err.message}`, 'error');
      } finally {
        if (isMounted) setLoading(false);
      }
    };

    fetchData();
    return () => {
      isMounted = false;
    };
  }, [selectedProject, apiClient, showFlash]);

  const report = dqData?.reports?.[0];

  return (
    <div className="space-y-8">
      {/* ------------------------------------------------------------------ */}
      {/* A. Data Quality Section */}
      {/* ------------------------------------------------------------------ */}
      <div className="space-y-4">
        <div className="flex flex-col justify-between space-y-1 sm:flex-row sm:items-center sm:space-y-0">
          <div>
            <div className="flex items-center space-x-2">
              <ShieldCheck className="h-5 w-5 text-sky-600 dark:text-sky-400" />
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                Data Quality Scoring
              </h3>
            </div>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Dimensional audit across completeness (40%), uniqueness (20%), and format consistency (40%).
            </p>
          </div>
          {report && (
            <span className="text-[11px] font-mono text-slate-400 dark:text-slate-500">
              Source: {report.source} · Generated: {report.generated_at ? new Date(report.generated_at).toLocaleDateString() : 'Active'}
            </span>
          )}
        </div>

        {report ? (
          <>
            {/* DQ KPI Cards */}
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Overall Quality Score
                </span>
                <div className="mt-2 flex items-baseline space-x-2">
                  <span className="text-3xl font-bold tracking-tight text-slate-900 dark:text-white">
                    {report.overall_score}%
                  </span>
                  <span className="text-xs font-semibold text-emerald-600 dark:text-emerald-400">
                    High Reliability
                  </span>
                </div>
                <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
                  <div
                    className="h-full bg-sky-500 rounded-full"
                    style={{ width: `${report.overall_score}%` }}
                  />
                </div>
              </div>

              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Columns Scored
                </span>
                <div className="mt-2 text-3xl font-bold tracking-tight text-slate-900 dark:text-white">
                  {report.column_count}
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Excludes technical quality flags
                </p>
              </div>

              <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-2xs dark:border-slate-800 dark:bg-slate-900">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Outlier Flags
                </span>
                <div className="mt-2 flex items-center space-x-2">
                  <span className="text-3xl font-bold tracking-tight text-slate-900 dark:text-white">
                    {report.columns.filter((c) => c.outlier_flag).length}
                  </span>
                  <span className="rounded bg-slate-100 px-2 py-0.5 text-[11px] font-mono text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                    &gt; 3 sigma
                  </span>
                </div>
                <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
                  Informational only; does not penalize score
                </p>
              </div>
            </div>

            {/* Per-Column Scores Table */}
            <div className="rounded-xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead className="border-b border-slate-100 bg-slate-50/70 font-medium text-slate-500 dark:border-slate-800 dark:bg-slate-800/40 dark:text-slate-400">
                    <tr>
                      <th className="px-4 py-3">Column</th>
                      <th className="px-4 py-3">Type</th>
                      <th className="px-4 py-3">Completeness (40%)</th>
                      <th className="px-4 py-3">Uniqueness (20%)</th>
                      <th className="px-4 py-3">Format Score (40%)</th>
                      <th className="px-4 py-3">DQ Score</th>
                      <th className="px-4 py-3 text-right">Outlier Flag</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                    {report.columns.map((c) => (
                      <tr key={c.column} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40">
                        <td className="px-4 py-3 font-semibold text-slate-900 dark:text-white">
                          {c.column}
                        </td>
                        <td className="px-4 py-3 text-slate-500 dark:text-slate-400">
                          {c.dtype || 'object'}
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center space-x-2">
                            <span className="w-10 text-slate-700 dark:text-slate-300">
                              {c.completeness}%
                            </span>
                            <div className="h-1.5 w-16 rounded-full bg-slate-100 dark:bg-slate-800">
                              <div
                                className="h-full rounded-full bg-emerald-500"
                                style={{ width: `${c.completeness}%` }}
                              />
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-3 text-slate-700 dark:text-slate-300">
                          {c.uniqueness}%
                        </td>
                        <td className="px-4 py-3 text-slate-700 dark:text-slate-300">
                          {c.format_score}%
                        </td>
                        <td className="px-4 py-3 font-semibold text-sky-700 dark:text-sky-400">
                          {c.dq_score}%
                        </td>
                        <td className="px-4 py-3 text-right">
                          {c.outlier_flag ? (
                            <span className="inline-flex items-center rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-700 dark:bg-amber-950/60 dark:text-amber-400">
                              Flagged
                            </span>
                          ) : (
                            <span className="text-[11px] text-slate-400 font-sans">Normal</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <div className="rounded-xl border border-slate-200 bg-white p-8 text-center dark:border-slate-800 dark:bg-slate-900">
            <Info className="mx-auto h-8 w-8 text-slate-400" />
            <h4 className="mt-2 text-sm font-semibold text-slate-800 dark:text-slate-200">
              No Data Quality Report Found
            </h4>
            <p className="mt-1 text-xs text-slate-500">
              Data quality reports are generated automatically for single-dataset pipeline runs.
            </p>
          </div>
        )}
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* B. Anomalies Section */}
      {/* ------------------------------------------------------------------ */}
      <div className="space-y-4 pt-4 border-t border-slate-200 dark:border-slate-800">
        <div className="flex flex-col justify-between space-y-1 sm:flex-row sm:items-center sm:space-y-0">
          <div>
            <div className="flex items-center space-x-2">
              <AlertTriangle className="h-5 w-5 text-amber-600 dark:text-amber-400" />
              <h3 className="text-base font-bold text-slate-900 dark:text-white">
                Anomaly Detection & Framing
              </h3>
            </div>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Identifies statistical outliers and translates raw detection signals into actionable business guidance.
            </p>
          </div>
          {anomalyData && (
            <span className="rounded-full bg-amber-50 px-2.5 py-1 text-xs font-semibold text-amber-800 dark:bg-amber-950/80 dark:text-amber-300">
              Report Version: {anomalyData.version}
            </span>
          )}
        </div>

        {anomalyData ? (
          <div className="space-y-4">
            {/* Business Summary Callout (v2 feature) */}
            {anomalyData.business_summary && (
              <div className="rounded-xl border border-sky-200 bg-sky-50/70 p-4 text-xs dark:border-sky-900/60 dark:bg-sky-950/30">
                <div className="flex items-start space-x-2.5">
                  <Sparkles className="mt-0.5 h-4 w-4 shrink-0 text-sky-600 dark:text-sky-400" />
                  <div>
                    <span className="font-semibold text-sky-900 dark:text-sky-200">
                      Framed Executive Summary:
                    </span>
                    <p className="mt-1 text-sky-800 dark:text-sky-300">
                      {anomalyData.business_summary}
                    </p>
                  </div>
                </div>
              </div>
            )}

            {/* Anomaly Table */}
            <div className="rounded-xl border border-slate-200 bg-white shadow-2xs dark:border-slate-800 dark:bg-slate-900">
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead className="border-b border-slate-100 bg-slate-50/70 font-medium text-slate-500 dark:border-slate-800 dark:bg-slate-800/40 dark:text-slate-400">
                    <tr>
                      <th className="px-4 py-3">Column</th>
                      <th className="px-4 py-3">Detection Method</th>
                      <th className="px-4 py-3">Threshold Tested</th>
                      <th className="px-4 py-3">Flagged Count</th>
                      <th className="px-4 py-3">Example Values</th>
                      {anomalyData.version === 'v2' && (
                        <>
                          <th className="px-4 py-3">Impact</th>
                          <th className="px-4 py-3">Suggested Deterministic Action</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100 dark:divide-slate-800/60 font-mono">
                    {anomalyData.anomalies.map((item, idx) => {
                      const isHigh = item.impact === 'high';
                      const isMedium = item.impact === 'medium';

                      return (
                        <tr key={idx} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/40">
                          <td className="px-4 py-3 font-semibold text-slate-900 dark:text-white">
                            {item.column}
                          </td>
                          <td className="px-4 py-3 text-slate-600 dark:text-slate-300">
                            {item.method}
                          </td>
                          <td className="px-4 py-3 text-slate-500 dark:text-slate-400">
                            {item.threshold || '—'}
                          </td>
                          <td className="px-4 py-3 font-semibold text-slate-800 dark:text-slate-200">
                            {item.anomaly_count}
                          </td>
                          <td className="px-4 py-3 text-slate-600 dark:text-slate-300">
                            {item.example_values.length > 0
                              ? item.example_values.join(', ')
                              : 'None'}
                          </td>
                          {anomalyData.version === 'v2' && (
                            <>
                              <td className="px-4 py-3 font-sans">
                                {item.impact ? (
                                  <span
                                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider ${
                                      isHigh
                                        ? 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300'
                                        : isMedium
                                        ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
                                        : 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300'
                                    }`}
                                  >
                                    {item.impact}
                                  </span>
                                ) : (
                                  '—'
                                )}
                              </td>
                              <td className="px-4 py-3 font-sans text-slate-700 dark:text-slate-300 max-w-sm">
                                {item.suggested_action || '—'}
                              </td>
                            </>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        ) : (
          <div className="rounded-xl border border-slate-200 bg-white p-8 text-center dark:border-slate-800 dark:bg-slate-900">
            <Info className="mx-auto h-8 w-8 text-slate-400" />
            <h4 className="mt-2 text-sm font-semibold text-slate-800 dark:text-slate-200">
              No Anomaly Report Generated
            </h4>
            <p className="mt-1 text-xs text-slate-500">
              Execute a pipeline run to generate statistical anomaly framing.
            </p>
          </div>
        )}
      </div>
    </div>
  );
};
