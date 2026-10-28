import type { DatasetProfile } from '../utils/csvProfiler';
import { topValues } from './query';
import { MISSING_LABEL, type ColumnKind, type Dataset } from './types';

export interface ColumnSummary {
  name: string;
  kind: ColumnKind;
  missing: number;
  /** Share of rows with no value, 0-100, one decimal. */
  missingPct: number;
  distinct: number;
  min?: number | string;
  max?: number | string;
  /** Numeric columns only. */
  mean?: number;
  /** Categorical and boolean columns only: the most frequent values. */
  top?: { value: string; count: number }[];
}

export interface DatasetSummary {
  rows: number;
  columns: number;
  cells: number;
  kindCounts: Record<ColumnKind, number>;
  missingCells: number;
  completenessPct: number;
  duplicateRows: number;
  unreadable: number;
  columnSummaries: ColumnSummary[];
}

const round1 = (n: number) => Math.round(n * 10) / 10;

/** Everything here is computed from the loaded rows; nothing is looked up or assumed. */
export function summarizeDataset(dataset: Dataset, profile: DatasetProfile): DatasetSummary {
  const rows = dataset.rows.length;
  const kindCounts: Record<ColumnKind, number> = { numeric: 0, categorical: 0, date: 0, boolean: 0 };
  let missingCells = 0;
  let unreadable = 0;

  const columnSummaries = dataset.columns.map((column): ColumnSummary => {
    kindCounts[column.kind] += 1;
    // nullCount already includes the unreadable values (they are read as missing), so do not add them again.
    missingCells += column.nullCount;
    unreadable += column.invalidCount;
    const missing = column.nullCount;
    const summary: ColumnSummary = {
      name: column.name,
      kind: column.kind,
      missing,
      missingPct: rows ? round1((missing / rows) * 100) : 0,
      distinct: column.distinctCount,
      min: column.min,
      max: column.max,
    };
    if (column.kind === 'numeric') {
      let total = 0;
      let count = 0;
      for (const row of dataset.rows) {
        const cell = row[column.name];
        if (typeof cell === 'number') {
          total += cell;
          count += 1;
        }
      }
      if (count > 0) summary.mean = Number((total / count).toPrecision(12));
    } else if (column.kind === 'categorical' || column.kind === 'boolean') {
      summary.top = topValues(dataset.rows, column.name, 4)
        .filter((t) => t.value !== MISSING_LABEL)
        .slice(0, 3);
    }
    return summary;
  });

  const cells = rows * dataset.columns.length;
  return {
    rows,
    columns: dataset.columns.length,
    cells,
    kindCounts,
    missingCells,
    completenessPct: cells ? round1(((cells - missingCells) / cells) * 100) : 100,
    duplicateRows: profile.duplicateRows,
    unreadable,
    columnSummaries,
  };
}

/** Plain-language findings about the data, shared by the Data Quality tab and the Overview. */
export function qualityFindings(dataset: Dataset, profile: DatasetProfile): string[] {
  const findings: string[] = [];
  for (const c of profile.columns) {
    if (c.nullCount > 0) {
      const share = ((c.nullCount / Math.max(1, profile.totalRows)) * 100).toFixed(1);
      findings.push(`'${c.name}' has ${c.nullCount} missing value${c.nullCount === 1 ? '' : 's'} (${share}% of rows).`);
    }
  }
  for (const c of dataset.columns) {
    if (c.invalidCount > 0) {
      findings.push(
        `'${c.name}' has ${c.invalidCount} value${c.invalidCount === 1 ? '' : 's'} that could not be read as ${c.kind}; the analysis treats them as missing.`,
      );
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
  return findings;
}
