import { formatNumber } from './format';
import { specToSentence, topValues } from './query';
import {
  MISSING_LABEL,
  type AnalysisSpec,
  type ColumnKind,
  type Dataset,
  type DatasetColumn,
  type DateBin,
  type Row,
} from './types';

/**
 * Deterministic analysis suggestions. Every suggestion comes from the rules below applied to the columns
 * of the loaded data: no model, no scores, no guessed relationships. The same input always gives the same
 * output, and each suggestion carries the facts that led to it, the exact calculation, and its limits.
 *
 * This runs in the browser on the rows the user loaded. It is separate from the Python pipeline.
 */
export const ADVISOR_RULES = {
  /** A column with at least this share of missing values is not suggested. */
  mostlyMissingShare: 0.5,
  /** A text column whose most frequent value is at least this share of its values is "almost constant". */
  almostConstantShare: 0.95,
  /** Fewer rows than this and "almost constant" is not judged: a handful of rows proves nothing. */
  minRowsForDominance: 20,
  /** A text column with more distinct values than this is a label (for rankings), not a grouping. */
  groupMaxDistinct: 30,
  /** Size of "top N" rankings. */
  topN: 10,
  /** At most this many numeric columns get suggestions, in file order. */
  maxMetrics: 3,
  maxSuggestions: 8,
  /** The same 1.5 x IQR rule as the Data Quality profile, so the counts agree. */
  iqrFactor: 1.5,
} as const;

/** A numeric column whose name looks like a key and whose values are all different is an identifier. */
const ID_NAME = /^(id|index|idx|key|row_?(id|num|number)?|.*_(id|key|code|uuid|guid)|(id|key|code)_.*)$/i;

export type ColumnRole = 'metric' | 'category' | 'label' | 'time' | 'identifier' | 'unused';
export type ConcernCode =
  | 'empty'
  | 'constant'
  | 'mostly_missing'
  | 'almost_constant'
  | 'identifier'
  | 'some_missing'
  | 'unreadable';

export interface Concern {
  code: ConcernCode;
  /** `warning`: the column is left out of the suggestions. `info`: worth knowing, still used. */
  severity: 'warning' | 'info';
  text: string;
}

/** What the rules need to know about a column, wherever it comes from (the loaded rows or an upload's schema). */
export interface ColumnFacts {
  name: string;
  kind: ColumnKind;
  rows: number;
  /** Rows with no usable value (blank, a null word, or unreadable as the column's type). */
  missing: number;
  /** Rows whose value could not be read as the column's type; part of `missing`. */
  unreadable?: number;
  distinct: number;
  min?: number | string;
  max?: number | string;
  /** Most frequent value, when known. */
  top?: { value: string; count: number };
  /** Some value of the column, used to say what a constant column contains. */
  example?: string;
  integerLike?: boolean;
}

export interface ColumnAdvice {
  name: string;
  kind: ColumnKind;
  role: ColumnRole;
  concerns: Concern[];
}

const percent = (part: number, whole: number) => (whole > 0 ? Math.round((part / whole) * 1000) / 10 : 0);
const plural = (n: number, word: string) => `${n.toLocaleString('en-US')} ${word}${n === 1 ? '' : 's'}`;

/** Applies the column rules to one column. */
export function classifyColumn(f: ColumnFacts): ColumnAdvice {
  const nonNull = f.rows - f.missing;
  const concerns: Concern[] = [];
  let role: ColumnRole = 'unused';

  if (f.unreadable && f.unreadable > 0) {
    concerns.push({
      code: 'unreadable',
      severity: 'info',
      text: `${plural(f.unreadable, 'value')} could not be read as ${f.kind}; they are treated as missing.`,
    });
  }

  if (nonNull <= 0) {
    concerns.push({ code: 'empty', severity: 'warning', text: 'No row has a value. Not used for suggestions.' });
    return { name: f.name, kind: f.kind, role, concerns };
  }
  if (f.distinct <= 1) {
    const value = f.kind === 'numeric' && f.min !== undefined ? String(f.min) : (f.top?.value ?? f.example);
    const what = value === undefined ? 'the same value' : f.kind === 'numeric' ? `${formatNumber(Number(value))}` : `“${value}”`;
    concerns.push({
      code: 'constant',
      severity: 'warning',
      text: `Every value is ${what}, so it cannot tell rows apart. Not used for suggestions.`,
    });
    return { name: f.name, kind: f.kind, role, concerns };
  }
  const missingShare = f.missing / Math.max(1, f.rows);
  if (missingShare >= ADVISOR_RULES.mostlyMissingShare) {
    concerns.push({
      code: 'mostly_missing',
      severity: 'warning',
      text: `${plural(f.missing, 'row')} (${percent(f.missing, f.rows)}%) have no value. Not used for suggestions.`,
    });
    return { name: f.name, kind: f.kind, role, concerns };
  }
  if (f.missing > 0) {
    concerns.push({
      code: 'some_missing',
      severity: 'info',
      text: `${plural(f.missing, 'row')} (${percent(f.missing, f.rows)}%) have no value; sums and averages leave them out.`,
    });
  }

  if (f.kind === 'date') {
    role = 'time';
  } else if (f.kind === 'numeric') {
    const allDifferent = nonNull >= 2 && f.distinct === nonNull;
    if (allDifferent && f.integerLike !== false && ID_NAME.test(f.name)) {
      concerns.push({
        code: 'identifier',
        severity: 'warning',
        text: 'The name looks like a key and every value is different, so it is probably an identifier, not a measure. Not used for suggestions.',
      });
      role = 'identifier';
    } else {
      role = 'metric';
    }
  } else {
    const dominant = f.top && nonNull >= ADVISOR_RULES.minRowsForDominance ? f.top.count / nonNull : 0;
    if (dominant >= ADVISOR_RULES.almostConstantShare) {
      concerns.push({
        code: 'almost_constant',
        severity: 'warning',
        text: `“${f.top!.value}” is ${percent(f.top!.count, nonNull)}% of its values, so grouping by it shows almost nothing. Not used for suggestions.`,
      });
      role = 'unused';
    } else if ((nonNull >= 2 && f.distinct === nonNull) || f.distinct > ADVISOR_RULES.groupMaxDistinct) {
      role = 'label'; // many different values: good for naming a ranking, not for grouping
    } else {
      role = 'category';
    }
  }
  return { name: f.name, kind: f.kind, role, concerns };
}

// ----------------------------------------------------------------------------------- from the loaded rows

function factsOf(dataset: Dataset, column: DatasetColumn): ColumnFacts {
  const facts: ColumnFacts = {
    name: column.name,
    kind: column.kind,
    rows: dataset.rows.length,
    missing: column.nullCount, // already includes the unreadable values
    unreadable: column.invalidCount,
    distinct: column.distinctCount,
    min: column.min,
    max: column.max,
  };
  if (column.kind === 'categorical' || column.kind === 'boolean') {
    const top = topValues(dataset.rows, column.name, 2).find((t) => t.value !== MISSING_LABEL);
    if (top) facts.top = top;
  }
  if (column.kind === 'numeric') {
    facts.integerLike = dataset.rows.every((row) => {
      const cell = row[column.name];
      return cell === null || Number.isInteger(cell);
    });
  }
  return facts;
}

export function adviseColumns(dataset: Dataset): ColumnAdvice[] {
  return dataset.columns.map((column) => classifyColumn(factsOf(dataset, column)));
}

/** A numeric column that is worth summing or averaging (not constant, empty, mostly missing or a key). */
export function usableAsMetric(dataset: Dataset, column: DatasetColumn): boolean {
  return column.kind === 'numeric' && classifyColumn(factsOf(dataset, column)).role === 'metric';
}

// ------------------------------------------------------------------------------------------- statistics

export interface NumericFacts {
  count: number;
  min: number;
  max: number;
  mean: number;
  q1: number;
  q3: number;
  lowerFence: number;
  upperFence: number;
  below: number;
  above: number;
}

/** Quartiles by the same rule as the Data Quality profile (sorted values, positions n*0.25 and n*0.75). */
export function numericFacts(rows: Row[], column: string): NumericFacts | null {
  const values: number[] = [];
  for (const row of rows) {
    const cell = row[column];
    if (typeof cell === 'number') values.push(cell);
  }
  if (values.length < 4) return null;
  const sorted = [...values].sort((a, b) => a - b);
  let total = 0;
  for (const v of sorted) total += v;
  const q1 = sorted[Math.floor(sorted.length * 0.25)];
  const q3 = sorted[Math.floor(sorted.length * 0.75)];
  const lowerFence = q1 - ADVISOR_RULES.iqrFactor * (q3 - q1);
  const upperFence = q3 + ADVISOR_RULES.iqrFactor * (q3 - q1);
  let below = 0;
  let above = 0;
  for (const v of sorted) {
    if (v < lowerFence) below += 1;
    else if (v > upperFence) above += 1;
  }
  return {
    count: sorted.length,
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: total / sorted.length,
    q1,
    q3,
    lowerFence,
    upperFence,
    below,
    above,
  };
}

/** Year, month or day, whichever gives a readable number of periods for dates between `min` and `max`. */
export function chooseBin(min: unknown, max: unknown): DateBin {
  const a = String(min ?? '');
  const b = String(max ?? '');
  const years = Number(b.slice(0, 4)) - Number(a.slice(0, 4));
  if (years >= 3) return 'year';
  const months = years * 12 + Number(b.slice(5, 7)) - Number(a.slice(5, 7));
  return months >= 2 ? 'month' : 'day';
}

// -------------------------------------------------------------------------------------------- suggestions

export interface Suggestion {
  id: string;
  /** `query` fills the Build Analysis form and runs it; the others open the column in Explore & Filter. */
  kind: 'query' | 'distribution' | 'outliers';
  title: string;
  /** Facts from the data that led to it. */
  because: string[];
  /** Exactly what will be calculated. */
  calculation: string;
  /** What the result will not tell, with the real counts. */
  limits: string[];
  chart: 'bar' | 'line' | 'histogram' | 'table';
  spec?: AnalysisSpec;
  column?: string;
}

export interface Advice {
  columns: ColumnAdvice[];
  suggestions: Suggestion[];
  /** Suggestions that were possible but not shown because of the cap. */
  hidden: number;
  /** Why there is nothing to suggest, when that is the case. */
  nothingBecause: string | null;
}

const measure = (fn: 'sum' | 'avg' | 'count', column?: string) => (column ? { fn, column } : { fn });

function leftOut(rows: Row[], columns: string[]): number {
  let n = 0;
  for (const row of rows) if (columns.some((c) => row[c] === null)) n += 1;
  return n;
}

function describeNumeric(f: NumericFacts): string {
  return `${plural(f.count, 'value')}, from ${formatNumber(f.min)} to ${formatNumber(f.max)}, average ${formatNumber(Number(f.mean.toPrecision(6)))}`;
}

export function adviseDataset(dataset: Dataset): Advice {
  const columns = adviseColumns(dataset);
  const byRole = (role: ColumnRole) =>
    columns.filter((c) => c.role === role).map((c) => dataset.columns.find((d) => d.name === c.name)!);
  const metrics = byRole('metric').slice(0, ADVISOR_RULES.maxMetrics);
  const time = byRole('time')[0];
  const label = byRole('label')[0];
  const category = byRole('category')[0];
  const rows = dataset.rows;
  const all: Suggestion[] = [];

  const timeBin = time ? chooseBin(time.min, time.max) : undefined;
  const binWord = timeBin === 'year' ? 'year' : timeBin === 'month' ? 'month' : 'day';
  const timeFact = time ? `${time.name} is a date column from ${time.min} to ${time.max}` : '';

  for (const metric of metrics) {
    const facts = numericFacts(rows, metric.name);
    const metricFact = facts
      ? `${metric.name} is a numeric column: ${describeNumeric(facts)}`
      : `${metric.name} is a numeric column with ${plural(rows.length - metric.nullCount, 'value')}`;

    if (time && timeBin) {
      const spec: AnalysisSpec = {
        filters: [],
        dimensions: [{ column: time.name, bin: timeBin }],
        measures: [measure('sum', metric.name)],
      };
      const skipped = leftOut(rows, [time.name, metric.name]);
      all.push({
        id: `trend:${metric.name}:${time.name}`,
        kind: 'query',
        title: `${metric.name} per ${binWord} of ${time.name}`,
        because: [timeFact, metricFact],
        calculation: specToSentence(spec),
        limits: [
          ...(skipped > 0 ? [`${plural(skipped, 'row')} with no ${time.name} or no ${metric.name} are left out.`] : []),
          'A period with more rows usually has a larger sum; this shows totals, not the typical row.',
        ],
        chart: 'line',
        spec,
      });
    }

    if (label) {
      const key = `sum(${metric.name})`;
      const spec: AnalysisSpec = {
        filters: [],
        dimensions: [{ column: label.name }],
        measures: [measure('sum', metric.name)],
        sort: { key, direction: 'desc' },
        limit: ADVISOR_RULES.topN,
      };
      const skipped = leftOut(rows, [label.name, metric.name]);
      all.push({
        id: `top:${metric.name}:${label.name}`,
        kind: 'query',
        title: `Top ${ADVISOR_RULES.topN} ${label.name} by ${metric.name}`,
        because: [
          `${label.name} has ${plural(label.distinctCount, 'different value')} (${label.distinctCount === rows.length - label.nullCount ? 'one per row' : 'many'}), so it names items in a ranking`,
          metricFact,
        ],
        calculation: specToSentence(spec),
        limits: [
          ...(skipped > 0 ? [`${plural(skipped, 'row')} with no ${label.name} or no ${metric.name} are left out.`] : []),
          `Only the ${ADVISOR_RULES.topN} largest totals are shown; the CSV export of the result has the same ${ADVISOR_RULES.topN}.`,
        ],
        chart: 'bar',
        spec,
      });
    }

    if (category) {
      const spec: AnalysisSpec = {
        filters: [],
        dimensions: [{ column: category.name }],
        measures: [measure('sum', metric.name), measure('avg', metric.name)],
        sort: { key: `sum(${metric.name})`, direction: 'desc' },
      };
      const skipped = leftOut(rows, [category.name, metric.name]);
      all.push({
        id: `compare:${metric.name}:${category.name}`,
        kind: 'query',
        title: `${metric.name} by ${category.name}`,
        because: [`${category.name} has ${plural(category.distinctCount, 'distinct value')}, few enough to compare`, metricFact],
        calculation: specToSentence(spec),
        limits: [
          ...(skipped > 0 ? [`${plural(skipped, 'row')} with no ${category.name} or no ${metric.name} are left out.`] : []),
          'Groups with more rows have larger sums; the average shows the typical row of each group.',
        ],
        chart: 'bar',
        spec,
      });
    }

    if (facts) {
      all.push({
        id: `distribution:${metric.name}`,
        kind: 'distribution',
        title: `Distribution of ${metric.name}`,
        because: [metricFact],
        calculation: `Rows are counted in 10 equal-width ranges between ${formatNumber(facts.min)} and ${formatNumber(facts.max)}.`,
        limits: [
          ...(rows.length - facts.count > 0 ? [`${plural(rows.length - facts.count, 'row')} with no ${metric.name} are not shown.`] : []),
          'Equal-width ranges: a few extreme values can leave most rows in one bar.',
        ],
        chart: 'histogram',
        column: metric.name,
      });
      if (facts.below + facts.above > 0) {
        all.push({
          id: `outliers:${metric.name}`,
          kind: 'outliers',
          title: `Unusual values in ${metric.name}`,
          because: [
            `${plural(facts.below + facts.above, 'value')} (${percent(facts.below + facts.above, facts.count)}%) fall outside ${formatNumber(Number(facts.lowerFence.toPrecision(6)))} to ${formatNumber(Number(facts.upperFence.toPrecision(6)))}`,
          ],
          calculation: `Values below ${formatNumber(Number(facts.lowerFence.toPrecision(6)))} or above ${formatNumber(Number(facts.upperFence.toPrecision(6)))} (1.5 x the interquartile range from ${formatNumber(facts.q1)} to ${formatNumber(facts.q3)}) are counted: ${facts.below} below, ${facts.above} above. Nothing is removed from your data.`,
          limits: ['Unusual is not the same as wrong: it only means far from most values.'],
          chart: 'histogram',
          column: metric.name,
        });
      }
    }
  }

  if (metrics.length === 0) {
    if (category) {
      const spec: AnalysisSpec = { filters: [], dimensions: [{ column: category.name }], measures: [{ fn: 'count' }], sort: { key: 'count', direction: 'desc' } };
      all.push({
        id: `count:${category.name}`,
        kind: 'query',
        title: `Rows per ${category.name}`,
        because: [`${category.name} has ${plural(category.distinctCount, 'distinct value')}`, 'There is no numeric column to sum or average'],
        calculation: specToSentence(spec),
        limits: leftOut(rows, [category.name]) > 0 ? [`${plural(leftOut(rows, [category.name]), 'row')} with no ${category.name} are left out.`] : [],
        chart: 'bar',
        spec,
      });
    }
    if (time && timeBin) {
      const spec: AnalysisSpec = { filters: [], dimensions: [{ column: time.name, bin: timeBin }], measures: [{ fn: 'count' }] };
      all.push({
        id: `count:${time.name}`,
        kind: 'query',
        title: `Rows per ${binWord} of ${time.name}`,
        because: [timeFact, 'There is no numeric column to sum or average'],
        calculation: specToSentence(spec),
        limits: leftOut(rows, [time.name]) > 0 ? [`${plural(leftOut(rows, [time.name]), 'row')} with no ${time.name} are left out.`] : [],
        chart: 'line',
        spec,
      });
    }
  }

  const suggestions = all.slice(0, ADVISOR_RULES.maxSuggestions);
  const usable = columns.some((c) => c.role !== 'unused' && c.role !== 'identifier');
  return {
    columns,
    suggestions,
    hidden: all.length - suggestions.length,
    nothingBecause:
      suggestions.length > 0
        ? null
        : usable
          ? 'No column combination here has a numeric column to measure or a column to group by.'
          : 'Every column is empty, constant, mostly missing or a key, so there is nothing to analyze.',
  };
}

// ------------------------------------------------------------------------- from an upload's server schema

/** The columns of an upload as the Quaero API describes them (see `UploadResponse`). */
export interface UploadSchema {
  row_count: number;
  /** Numeric columns the server considers measures (it leaves out year and *_id columns). */
  numeric_columns: string[];
  /** Columns the server recognised as time (a date, or a year/month/period column even when stored as a number). */
  candidate_time_columns: string[];
  columns: {
    name: string;
    normalized_name: string;
    dtype: string;
    null_count: number;
    unique_count: number;
    sample_values: string[];
  }[];
}

export interface UploadSetup {
  metric: string | null;
  dimension: string | null;
  time: string | null;
  /** One note per column that the rules flag, in file order. */
  notes: { column: string; text: string }[];
}

const KIND_OF_DTYPE: Record<string, ColumnKind> = {
  integer: 'numeric',
  float: 'numeric',
  datetime: 'date',
  boolean: 'boolean',
};

/**
 * Picks the metric, grouping and time column for a Python pipeline run from an upload's schema, with the
 * same rules as above. Only what the schema carries is used (no statistics), so this is a smaller view
 * of the same rules, shown as a suggestion the user can change.
 */
export function adviseUploadSetup(upload: UploadSchema): UploadSetup {
  const advice = upload.columns.map((c) => {
    const isTime = upload.candidate_time_columns.includes(c.normalized_name);
    const isNumeric = c.dtype === 'integer' || c.dtype === 'float';
    const result = classifyColumn({
      name: c.normalized_name,
      kind: isTime ? 'date' : (KIND_OF_DTYPE[c.dtype] ?? 'categorical'),
      rows: upload.row_count,
      missing: c.null_count,
      distinct: c.unique_count,
      example: c.sample_values[0],
      integerLike: c.dtype === 'integer' ? true : c.dtype === 'float' ? false : undefined,
    });
    // The server keeps year and *_id columns out of its list of measures; follow it.
    if (isNumeric && !isTime && result.role === 'metric' && !upload.numeric_columns.includes(c.normalized_name)) {
      return { ...result, role: 'unused' as ColumnRole };
    }
    return result;
  });
  const first = (role: ColumnRole) => advice.find((a) => a.role === role)?.name ?? null;
  return {
    metric: first('metric'),
    dimension: first('category') ?? first('label'),
    time: first('time'),
    notes: advice.flatMap((a) =>
      a.concerns.filter((c) => c.severity === 'warning').map((c) => ({ column: a.name, text: c.text })),
    ),
  };
}
