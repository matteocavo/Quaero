import { findColumn } from './dataset';
import {
  MAX_DIMENSIONS,
  MAX_FILTERS,
  MAX_GROUPS,
  MAX_MEASURES,
  MAX_RESULT_ROWS,
} from './limits';
import {
  MISSING_LABEL,
  type AggregateFn,
  type AnalysisResult,
  type AnalysisSpec,
  type Cell,
  type Dataset,
  type DatasetColumn,
  type DateBin,
  type Dimension,
  type Filter,
  type Measure,
  type ResultColumn,
  type Row,
} from './types';

export class AnalysisError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AnalysisError';
  }
}

const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;

// ------------------------------------------------------------------------------ labels

export function measureKey(measure: Measure): string {
  return measure.column ? `${measure.fn}(${measure.column})` : measure.fn;
}

export function dimensionKey(dimension: Dimension): string {
  return dimension.bin ? `${dimension.column} (${dimension.bin})` : dimension.column;
}

const MEASURE_WORDS: Record<AggregateFn, string> = {
  count: 'Number of rows',
  count_distinct: 'Distinct values of',
  sum: 'Sum of',
  avg: 'Average of',
  min: 'Minimum of',
  max: 'Maximum of',
};

export function measureLabel(measure: Measure): string {
  if (measure.fn === 'count') {
    return measure.column ? `Rows with a value in ${measure.column}` : MEASURE_WORDS.count;
  }
  return `${MEASURE_WORDS[measure.fn]} ${measure.column ?? '?'}`;
}

export function describeFilter(filter: Filter): string {
  switch (filter.op) {
    case 'in': {
      const shown = filter.values.slice(0, 4).join(', ');
      const more = filter.values.length > 4 ? ` +${filter.values.length - 4} more` : '';
      return `${filter.column} is ${filter.values.length === 1 ? '' : 'one of '}${shown}${more}`;
    }
    case 'range':
      if (filter.min !== undefined && filter.max !== undefined) {
        return `${filter.column} is between ${filter.min} and ${filter.max}`;
      }
      return filter.min !== undefined
        ? `${filter.column} is at least ${filter.min}`
        : `${filter.column} is at most ${filter.max}`;
    case 'dateRange':
      if (filter.from && filter.to) return `${filter.column} is between ${filter.from} and ${filter.to}`;
      return filter.from
        ? `${filter.column} is on or after ${filter.from}`
        : `${filter.column} is on or before ${filter.to}`;
    case 'contains':
      return `${filter.column} contains "${filter.text}"`;
    case 'missing':
      return `${filter.column} is missing`;
    case 'present':
      return `${filter.column} has a value`;
  }
}

// ----------------------------------------------------------------------------- filters

export function matchesFilter(row: Row, filter: Filter): boolean {
  const cell = row[filter.column];
  switch (filter.op) {
    case 'in':
      return cell === null ? filter.values.includes(MISSING_LABEL) : filter.values.includes(String(cell));
    case 'range':
      if (typeof cell !== 'number') return false;
      return (
        (filter.min === undefined || cell >= filter.min) &&
        (filter.max === undefined || cell <= filter.max)
      );
    case 'dateRange': {
      if (typeof cell !== 'string') return false;
      const day = cell.slice(0, 10);
      return (!filter.from || day >= filter.from) && (!filter.to || day <= filter.to);
    }
    case 'contains':
      return cell !== null && String(cell).toLowerCase().includes(filter.text.toLowerCase());
    case 'missing':
      return cell === null;
    case 'present':
      return cell !== null;
  }
}

/** Rows that satisfy every filter (AND). */
export function applyFilters(rows: Row[], filters: Filter[]): Row[] {
  if (filters.length === 0) return rows;
  return rows.filter((row) => filters.every((filter) => matchesFilter(row, filter)));
}

/** Positions (in the original row order) of the rows that satisfy every filter. */
export function filterIndices(rows: Row[], filters: Filter[]): number[] {
  const indices: number[] = [];
  rows.forEach((row, index) => {
    if (filters.every((filter) => matchesFilter(row, filter))) indices.push(index);
  });
  return indices;
}

// -------------------------------------------------------------------------- validation

export function validateFilter(dataset: Dataset, filter: Filter): string[] {
  const column = findColumn(dataset, filter.column);
  if (!column) return [`Filter column '${filter.column}' does not exist in this dataset.`];
  const where = `Filter on '${column.name}'`;
  switch (filter.op) {
    case 'in':
      return filter.values.length === 0 ? [`${where}: choose at least one value.`] : [];
    case 'range': {
      if (column.kind !== 'numeric') return [`${where}: a numeric range needs a numeric column.`];
      const { min, max } = filter;
      if (min === undefined && max === undefined) return [`${where}: enter a minimum or a maximum.`];
      if ((min !== undefined && !Number.isFinite(min)) || (max !== undefined && !Number.isFinite(max))) {
        return [`${where}: the range must use numbers.`];
      }
      if (min !== undefined && max !== undefined && min > max) {
        return [`${where}: the minimum is greater than the maximum.`];
      }
      return [];
    }
    case 'dateRange': {
      if (column.kind !== 'date') return [`${where}: a date range needs a date column.`];
      const { from, to } = filter;
      if (!from && !to) return [`${where}: enter a start or an end date.`];
      if ((from && !ISO_DAY.test(from)) || (to && !ISO_DAY.test(to))) {
        return [`${where}: dates must look like 2024-01-31.`];
      }
      if (from && to && from > to) return [`${where}: the start date is after the end date.`];
      return [];
    }
    case 'contains':
      return filter.text.trim() === '' ? [`${where}: enter the text to look for.`] : [];
    default:
      return [];
  }
}

function validateMeasure(dataset: Dataset, measure: Measure): string[] {
  const needsColumn = measure.fn !== 'count';
  if (needsColumn && !measure.column) {
    return [`${MEASURE_WORDS[measure.fn].replace(/ of$/, '')} needs a column.`];
  }
  if (!measure.column) return [];
  const column = findColumn(dataset, measure.column);
  if (!column) return [`Measure column '${measure.column}' does not exist in this dataset.`];
  if ((measure.fn === 'sum' || measure.fn === 'avg') && column.kind !== 'numeric') {
    return [`'${column.name}' is not numeric, so ${measure.fn === 'sum' ? 'a sum' : 'an average'} is not possible.`];
  }
  if ((measure.fn === 'min' || measure.fn === 'max') && column.kind !== 'numeric' && column.kind !== 'date') {
    return [`'${column.name}' is not numeric or a date, so a ${measure.fn === 'min' ? 'minimum' : 'maximum'} is not possible.`];
  }
  return [];
}

/** Every problem that would make the analysis wrong or impossible, in plain words. */
export function validateSpec(dataset: Dataset, spec: AnalysisSpec): string[] {
  const errors: string[] = [];

  if (spec.filters.length > MAX_FILTERS) errors.push(`Use at most ${MAX_FILTERS} filters.`);
  spec.filters.forEach((filter) => errors.push(...validateFilter(dataset, filter)));

  if (spec.dimensions.length > MAX_DIMENSIONS) {
    errors.push(`Group by at most ${MAX_DIMENSIONS} columns.`);
  }
  const seen = new Set<string>();
  for (const dimension of spec.dimensions) {
    const column = findColumn(dataset, dimension.column);
    if (!column) {
      errors.push(`Group-by column '${dimension.column}' does not exist in this dataset.`);
      continue;
    }
    if (dimension.bin && column.kind !== 'date') {
      errors.push(`'${column.name}' is not a date column, so it cannot be grouped by ${dimension.bin}.`);
    }
    const key = dimensionKey(dimension);
    if (seen.has(key)) errors.push(`'${key}' is used twice in the grouping.`);
    seen.add(key);
  }

  if (spec.measures.length === 0) errors.push('Choose at least one measure to calculate.');
  if (spec.measures.length > MAX_MEASURES) errors.push(`Calculate at most ${MAX_MEASURES} measures.`);
  const measureKeys = new Set<string>();
  for (const measure of spec.measures) {
    errors.push(...validateMeasure(dataset, measure));
    const key = measureKey(measure);
    if (measureKeys.has(key)) errors.push(`'${key}' is calculated twice.`);
    measureKeys.add(key);
  }

  if (spec.limit !== undefined && (!Number.isInteger(spec.limit) || spec.limit < 1 || spec.limit > MAX_RESULT_ROWS)) {
    errors.push(`The limit must be a whole number between 1 and ${MAX_RESULT_ROWS}.`);
  }
  if (spec.sort) {
    const keys = new Set([...spec.dimensions.map(dimensionKey), ...spec.measures.map(measureKey)]);
    if (!keys.has(spec.sort.key)) errors.push(`Cannot sort by '${spec.sort.key}': it is not in the result.`);
  }
  return errors;
}

// --------------------------------------------------------------------------- aggregation

interface Accumulator {
  rows: number;
  nonNull: number;
  sum: number;
  min: Cell;
  max: Cell;
  distinct?: Set<string | number | boolean>;
}

function newAccumulator(measure: Measure): Accumulator {
  return {
    rows: 0,
    nonNull: 0,
    sum: 0,
    min: null,
    max: null,
    ...(measure.fn === 'count_distinct' ? { distinct: new Set() } : {}),
  };
}

function update(acc: Accumulator, measure: Measure, row: Row): void {
  acc.rows += 1;
  if (!measure.column) return;
  const cell = row[measure.column];
  if (cell === null) return;
  acc.nonNull += 1;
  if (typeof cell === 'number') acc.sum += cell;
  if (acc.min === null || cell < acc.min) acc.min = cell;
  if (acc.max === null || cell > acc.max) acc.max = cell;
  acc.distinct?.add(cell);
}

/** Drops binary floating-point noise (58767.070000000014 -> 58767.07) while keeping 12 significant digits. */
function tidy(value: number): number {
  return Number(value.toPrecision(12));
}

function finish(acc: Accumulator, measure: Measure): Cell {
  switch (measure.fn) {
    case 'count':
      return measure.column ? acc.nonNull : acc.rows;
    case 'count_distinct':
      return acc.distinct?.size ?? 0;
    case 'sum':
      return acc.nonNull > 0 ? tidy(acc.sum) : null;
    case 'avg':
      return acc.nonNull > 0 ? tidy(acc.sum / acc.nonNull) : null;
    case 'min':
      return acc.min;
    case 'max':
      return acc.max;
  }
}

function binDate(cell: Cell, bin: DateBin | undefined): Cell {
  if (cell === null || typeof cell !== 'string' || !bin) return cell;
  return cell.slice(0, bin === 'year' ? 4 : bin === 'month' ? 7 : 10);
}

function compareCells(a: Cell, b: Cell): number {
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  if (typeof a === 'boolean' && typeof b === 'boolean') return Number(a) - Number(b);
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
}

function resultColumns(dataset: Dataset, spec: AnalysisSpec): ResultColumn[] {
  const dimensions = spec.dimensions.map((d): ResultColumn => ({
    key: dimensionKey(d),
    label: dimensionKey(d),
    role: 'dimension',
    kind: findColumn(dataset, d.column)?.kind ?? 'categorical',
  }));
  const measures = spec.measures.map((m): ResultColumn => {
    const source = m.column ? findColumn(dataset, m.column) : undefined;
    const keepsKind = (m.fn === 'min' || m.fn === 'max') && source;
    return {
      key: measureKey(m),
      label: measureLabel(m),
      role: 'measure',
      kind: keepsKind ? source.kind : 'numeric',
    };
  });
  return [...dimensions, ...measures];
}

function ignoredWarnings(dataset: Dataset, spec: AnalysisSpec, rows: Row[]): string[] {
  const warnings: string[] = [];
  const missingIn = (column: DatasetColumn) => rows.filter((r) => r[column.name] === null).length;

  for (const dimension of spec.dimensions) {
    const column = findColumn(dataset, dimension.column);
    const missing = column ? missingIn(column) : 0;
    if (missing > 0) {
      warnings.push(`${missing} of ${rows.length} rows have no value in '${dimension.column}' and are grouped as ${MISSING_LABEL}.`);
    }
  }
  for (const measure of spec.measures) {
    const column = measure.column ? findColumn(dataset, measure.column) : undefined;
    const missing = column ? missingIn(column) : 0;
    if (column && measure.fn !== 'count' && missing > 0) {
      warnings.push(`${missing} of ${rows.length} rows have no value in '${column.name}' and are ignored by ${measureKey(measure)}.`);
    }
    if (column && measure.fn === 'count' && missing > 0) {
      warnings.push(`${missing} of ${rows.length} rows have no value in '${column.name}' and are not counted.`);
    }
  }
  const invalid = spec.measures
    .map((m) => (m.column ? findColumn(dataset, m.column) : undefined))
    .filter((c): c is DatasetColumn => !!c && c.invalidCount > 0);
  for (const column of new Set(invalid)) {
    warnings.push(`'${column.name}' has ${column.invalidCount} values that could not be read as ${column.kind}; they are treated as missing.`);
  }
  return warnings;
}

function defaultSort(spec: AnalysisSpec, dataset: Dataset) {
  if (spec.sort || spec.dimensions.length === 0) return spec.sort;
  const first = spec.dimensions[0];
  if (findColumn(dataset, first.column)?.kind === 'date') {
    return { key: dimensionKey(first), direction: 'asc' as const };
  }
  return { key: measureKey(spec.measures[0]), direction: 'desc' as const };
}

/** Run an analysis on the loaded rows. Throws AnalysisError when the specification is invalid. */
export function runAnalysis(dataset: Dataset, spec: AnalysisSpec): AnalysisResult {
  const errors = validateSpec(dataset, spec);
  if (errors.length > 0) throw new AnalysisError(errors.join(' '));

  const filtered = applyFilters(dataset.rows, spec.filters);
  const columns = resultColumns(dataset, spec);
  const warnings: string[] = [];
  if (filtered.length === 0 && dataset.rows.length > 0) {
    warnings.push('No rows match the filters, so there is nothing to calculate.');
  }
  warnings.push(...ignoredWarnings(dataset, spec, filtered));

  type Group = { dims: Cell[]; accumulators: Accumulator[] };
  const groups = new Map<string, Group>();
  const group = (dims: Cell[], key: string): Group => {
    let existing = groups.get(key);
    if (!existing) {
      if (groups.size >= MAX_GROUPS) {
        throw new AnalysisError(
          `This grouping produces more than ${MAX_GROUPS.toLocaleString('en-US')} groups. ` +
            'Group by a column with fewer distinct values, or by a date bin.',
        );
      }
      existing = { dims, accumulators: spec.measures.map(newAccumulator) };
      groups.set(key, existing);
    }
    return existing;
  };

  if (spec.dimensions.length === 0) group([], '');
  for (const row of filtered) {
    const dims = spec.dimensions.map((d) => binDate(row[d.column], d.bin));
    const key = dims.map((v) => (v === null ? '\u0000' : String(v))).join('\u0001');
    const target = group(dims, key);
    spec.measures.forEach((measure, i) => update(target.accumulators[i], measure, row));
  }

  let rows: Row[] = [...groups.values()].map((g) => {
    const row: Row = {};
    spec.dimensions.forEach((d, i) => {
      row[dimensionKey(d)] = g.dims[i];
    });
    spec.measures.forEach((m, i) => {
      row[measureKey(m)] = finish(g.accumulators[i], m);
    });
    return row;
  });

  const sort = defaultSort(spec, dataset);
  if (sort) {
    const direction = sort.direction === 'asc' ? 1 : -1;
    const indexed = rows.map((row, index) => ({ row, index }));
    indexed.sort((a, b) => {
      const left = a.row[sort.key];
      const right = b.row[sort.key];
      if (left === null && right === null) return a.index - b.index;
      if (left === null) return 1;
      if (right === null) return -1;
      return compareCells(left, right) * direction || a.index - b.index;
    });
    rows = indexed.map((entry) => entry.row);
  }

  const totalGroups = rows.length;
  const truncated = spec.limit !== undefined && totalGroups > spec.limit;
  if (spec.limit !== undefined) rows = rows.slice(0, spec.limit);

  return {
    columns,
    rows,
    inputRows: dataset.rows.length,
    filteredRows: filtered.length,
    totalGroups,
    truncated,
    warnings,
  };
}

// ---------------------------------------------------------------------------- sentences

/** A plain-language description of what the specification calculates. */
export function specToSentence(spec: AnalysisSpec): string {
  const what = spec.measures.map(measureLabel).join(' and ') || 'Nothing selected yet';
  const by = spec.dimensions.length
    ? ` by ${spec.dimensions.map((d) => (d.bin ? `${d.column} (per ${d.bin})` : d.column)).join(' and ')}`
    : '';
  const where = spec.filters.length ? `, where ${spec.filters.map(describeFilter).join(' and ')}` : '';
  const sort = spec.sort
    ? `, sorted by ${spec.sort.key} (${spec.sort.direction === 'desc' ? 'highest first' : 'lowest first'})`
    : '';
  const limit = spec.limit ? `, top ${spec.limit}` : '';
  return `${what}${by}${where}${sort}${limit}.`;
}

// ------------------------------------------------------------------------ distributions

export interface ValueCount {
  value: string;
  count: number;
}

/** The most frequent values of a column (missing values are counted as MISSING_LABEL). */
export function topValues(rows: Row[], column: string, limit = 12): ValueCount[] {
  const counts = new Map<string, number>();
  for (const row of rows) {
    const cell = row[column];
    const key = cell === null ? MISSING_LABEL : String(cell);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value))
    .slice(0, limit);
}

export interface HistogramBin {
  from: number;
  to: number;
  count: number;
}

/** Equal-width bins over the non-missing numeric values of a column. */
export function histogram(rows: Row[], column: string, bins = 10): { bins: HistogramBin[]; missing: number } {
  const values: number[] = [];
  let missing = 0;
  for (const row of rows) {
    const cell = row[column];
    if (typeof cell === 'number') values.push(cell);
    else missing += 1;
  }
  if (values.length === 0) return { bins: [], missing };
  let min = values[0];
  let max = values[0];
  for (const value of values) {
    if (value < min) min = value;
    if (value > max) max = value;
  }
  if (min === max) return { bins: [{ from: min, to: max, count: values.length }], missing };
  const width = (max - min) / bins;
  const result: HistogramBin[] = Array.from({ length: bins }, (_, i) => ({
    from: min + i * width,
    to: i === bins - 1 ? max : min + (i + 1) * width,
    count: 0,
  }));
  for (const value of values) {
    const index = Math.min(bins - 1, Math.floor((value - min) / width));
    result[index].count += 1;
  }
  return { bins: result, missing };
}
