import React from 'react';
import { useApp } from '../../context/AppContext';
import { Card, Notice, Stat, secondaryButton } from './ui';

/** The same Data Quality numbers as the profiling screen, next to the analysis and with plain findings. */
export const QualityTab: React.FC = () => {
  const { activeDatasetProfile: profile, activeDataset: dataset, setActiveTab } = useApp();
  if (!profile || !dataset) return null;

  const missingCells = profile.columns.reduce((sum, c) => sum + c.nullCount, 0);
  const unreadable = dataset.columns.reduce((sum, c) => sum + c.invalidCount, 0);

  const findings: string[] = [];
  for (const c of profile.columns) {
    if (c.nullCount > 0) {
      const share = ((c.nullCount / Math.max(1, profile.totalRows)) * 100).toFixed(1);
      findings.push(`'${c.name}' has ${c.nullCount} missing value${c.nullCount === 1 ? '' : 's'} (${share}% of rows).`);
    }
  }
  for (const c of dataset.columns) {
    if (c.invalidCount > 0) {
      findings.push(`'${c.name}' has ${c.invalidCount} value${c.invalidCount === 1 ? '' : 's'} that could not be read as ${c.kind}; the analysis treats them as missing.`);
    }
  }
  if (profile.duplicateRows > 0) {
    findings.push(`${profile.duplicateRows} row${profile.duplicateRows === 1 ? ' is' : 's are'} an exact duplicate of an earlier row.`);
  }
  for (const c of profile.columns) {
    if (c.outlierFlag) {
      findings.push(`'${c.name}' has ${c.outlierCount} outlier${c.outlierCount === 1 ? '' : 's'} (outside 1.5 x IQR); averages and sums may be skewed.`);
    }
  }
  for (const c of profile.columns) {
    if (c.dtype === 'string' && profile.totalRows > 1 && c.distinctCount === profile.totalRows - c.nullCount && c.distinctCount > 0) {
      findings.push(`'${c.name}' is different in every row; it is probably an identifier and not useful to group by.`);
    }
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat label="Overall Data Quality score" value={profile.overallDqScore} hint="mean of column scores, 0-100" />
        <Stat label="Missing cells" value={missingCells.toLocaleString('en-US')} />
        <Stat label="Duplicate rows" value={profile.duplicateRows.toLocaleString('en-US')} />
        <Stat label="Unreadable values" value={unreadable.toLocaleString('en-US')} />
      </div>

      <Card title="What to know before analyzing" subtitle="Derived from your data; no thresholds are applied.">
        {findings.length === 0 ? (
          <Notice>No missing values, duplicates, unreadable values or outliers were found.</Notice>
        ) : (
          <ul className="list-disc space-y-1 pl-5 text-xs text-slate-700 dark:text-slate-200">
            {findings.map((text) => (
              <li key={text}>{text}</li>
            ))}
          </ul>
        )}
      </Card>

      <Card
        title="Column scores"
        subtitle="Score = 40% completeness + 20% uniqueness + 40% format consistency, as in the Quaero scorer."
      >
        <div className="overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-800">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 font-semibold text-slate-600 dark:bg-slate-800/60 dark:text-slate-300">
              <tr>
                <th className="px-3 py-2">Column</th>
                <th className="px-3 py-2">Type</th>
                <th className="px-3 py-2">Completeness</th>
                <th className="px-3 py-2">Uniqueness</th>
                <th className="px-3 py-2">Outliers</th>
                <th className="px-3 py-2">Score</th>
              </tr>
            </thead>
            <tbody>
              {profile.columns.map((c) => (
                <tr key={c.name} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="px-3 py-1.5 font-mono font-semibold text-slate-900 dark:text-slate-100">{c.name}</td>
                  <td className="px-3 py-1.5">{c.dtype}</td>
                  <td className="px-3 py-1.5">{c.completeness}%</td>
                  <td className="px-3 py-1.5">{c.uniqueness}%</td>
                  <td className="px-3 py-1.5">{c.outlierFlag ? `${c.outlierCount} flagged` : 'none flagged'}</td>
                  <td className="px-3 py-1.5 font-bold">{c.dqScore}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3">
          <button type="button" className={secondaryButton} onClick={() => setActiveTab('import')}>
            Open the detailed profile (distributions, audit report)
          </button>
        </div>
      </Card>
    </div>
  );
};
