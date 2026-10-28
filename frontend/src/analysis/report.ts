import { toCsv } from '../utils/csv';
import type { DatasetProfile } from '../utils/csvProfiler';
import { describeFilter, dimensionKey, measureKey, specToSentence, filterIndices } from './query';
import type { AnalysisResult, AnalysisSpec, Dataset, Filter } from './types';

export const REPORT_ROW_LIMIT = 5_000;

export const REPORT_NOTES = [
  'Calculated in the browser from the rows you loaded; nothing was uploaded.',
  'This is not the Quaero Python pipeline: it does not run the cleaning, mart, lineage or anomaly stages.',
  'Missing values are ignored by sum, average, minimum, maximum and the counts of a column.',
];

/** A JSON description of the analysis: what was asked, on which data, and what came out. */
export function buildReport(args: {
  dataset: Dataset;
  profile: DatasetProfile;
  spec: AnalysisSpec;
  result: AnalysisResult;
  generatedAt?: Date;
}) {
  const { dataset, profile, spec, result } = args;
  const included = result.rows.slice(0, REPORT_ROW_LIMIT);
  return {
    report_type: 'Quaero Web browser analysis',
    generated_at: (args.generatedAt ?? new Date()).toISOString(),
    notes: REPORT_NOTES,
    dataset: {
      file_name: dataset.fileName,
      origin: dataset.origin,
      rows: dataset.rows.length,
      columns: dataset.columns.map((c) => ({
        name: c.name,
        kind: c.kind,
        missing_values: c.nullCount,
        unreadable_values: c.invalidCount,
        distinct_values: c.distinctCount,
        ...(c.min !== undefined ? { min: c.min, max: c.max } : {}),
      })),
    },
    data_quality: {
      overall_score: profile.overallDqScore,
      duplicate_rows: profile.duplicateRows,
      columns: profile.columns.map((c) => ({
        name: c.name,
        dtype: c.dtype,
        completeness: c.completeness,
        uniqueness: c.uniqueness,
        dq_score: c.dqScore,
        outlier_flag: c.outlierFlag,
      })),
    },
    question: spec.question?.trim() || null,
    analysis: {
      description: specToSentence(spec),
      filters: spec.filters.map((filter) => ({ ...filter, description: describeFilter(filter) })),
      group_by: spec.dimensions.map((d) => ({ column: d.column, bin: d.bin ?? null, key: dimensionKey(d) })),
      measures: spec.measures.map((m) => ({ fn: m.fn, column: m.column ?? null, key: measureKey(m) })),
      sort: spec.sort ?? null,
      limit: spec.limit ?? null,
    },
    result: {
      rows_in_dataset: result.inputRows,
      rows_after_filters: result.filteredRows,
      groups: result.totalGroups,
      groups_returned: result.rows.length,
      limited_by_top_n: result.truncated,
      warnings: result.warnings,
      columns: result.columns.map((c) => ({ key: c.key, label: c.label, role: c.role })),
      rows_included: included.length,
      rows_omitted: result.rows.length - included.length,
      rows: included,
    },
  };
}

export type AnalysisReport = ReturnType<typeof buildReport>;

/** The aggregated result as CSV (one column per grouping column and measure). */
export function resultToCsv(result: AnalysisResult): string {
  return toCsv(
    result.columns.map((c) => c.key),
    result.rows,
  );
}

/**
 * The rows that pass the filters, exactly as they were in the file (original text, original column
 * order), so exporting never changes the data.
 */
export function filteredDatasetToCsv(dataset: Dataset, profile: DatasetProfile, filters: Filter[]): {
  csv: string;
  rows: number;
} {
  const indices = filterIndices(dataset.rows, filters);
  const raw = indices.map((index) => profile.allRows[index]);
  return { csv: toCsv(profile.headers, raw), rows: indices.length };
}
