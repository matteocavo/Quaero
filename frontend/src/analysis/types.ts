/**
 * Types of the in-browser analysis engine.
 *
 * Everything here describes data the user loaded into the page; nothing is predefined and nothing
 * leaves the browser.
 */

export type Cell = string | number | boolean | null;
export type Row = Record<string, Cell>;

/** categorical = text-like values, date values are normalised to ISO strings (YYYY-MM-DD[THH:mm:ss]). */
export type ColumnKind = 'numeric' | 'categorical' | 'date' | 'boolean';

export interface DatasetColumn {
  name: string;
  kind: ColumnKind;
  /** Rows with no value (blank or a null token such as NA). */
  nullCount: number;
  /** Values that exist but could not be read as the column's type (counted as missing). */
  invalidCount: number;
  distinctCount: number;
  /** Smallest/largest value for numeric and date columns. */
  min?: number | string;
  max?: number | string;
}

export type DatasetOrigin = 'upload' | 'sample';

export interface Dataset {
  /** Changes every time a different dataset is loaded, so dependent state can be reset. */
  id: number;
  fileName: string;
  origin: DatasetOrigin;
  columns: DatasetColumn[];
  rows: Row[];
}

// ----------------------------------------------------------------------------- filters

export type Filter =
  | { column: string; op: 'in'; values: string[] }
  | { column: string; op: 'range'; min?: number; max?: number }
  | { column: string; op: 'dateRange'; from?: string; to?: string }
  | { column: string; op: 'contains'; text: string }
  | { column: string; op: 'missing' }
  | { column: string; op: 'present' };

// ---------------------------------------------------------------------------- analysis

export type AggregateFn = 'count' | 'count_distinct' | 'sum' | 'avg' | 'min' | 'max';
export type DateBin = 'year' | 'month' | 'day';

export interface Measure {
  fn: AggregateFn;
  /** Optional only for `count` (counts rows). */
  column?: string;
}

export interface Dimension {
  column: string;
  /** Only for date columns: group by year, month or day. */
  bin?: DateBin;
}

export type SortDirection = 'asc' | 'desc';

export interface AnalysisSpec {
  /** The user's own wording, kept in the report. It is never interpreted. */
  question?: string;
  filters: Filter[];
  dimensions: Dimension[];
  measures: Measure[];
  sort?: { key: string; direction: SortDirection };
  limit?: number;
}

export interface ResultColumn {
  key: string;
  label: string;
  role: 'dimension' | 'measure';
  kind: ColumnKind;
}

export interface AnalysisResult {
  columns: ResultColumn[];
  rows: Row[];
  /** Rows in the dataset, rows left after the filters, groups before the limit was applied. */
  inputRows: number;
  filteredRows: number;
  totalGroups: number;
  truncated: boolean;
  warnings: string[];
}

export const MISSING_LABEL = '(missing)';
