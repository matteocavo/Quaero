import { describe, expect, it } from 'vitest';
import { parseCsvText } from '../utils/csv';
import { parseCSV } from '../utils/csvProfiler';
import { buildDataset, normalizeDate, parseBoolean, parseNumber } from './dataset';
import {
  MAX_COLUMNS,
  MAX_FILE_BYTES,
  MAX_GROUPS,
  MAX_ROWS,
  checkFileSize,
  checkParsedSize,
} from './limits';
import {
  AnalysisError,
  applyFilters,
  filterIndices,
  histogram,
  runAnalysis,
  specToSentence,
  topValues,
  validateSpec,
} from './query';
import { REPORT_ROW_LIMIT, buildReport, filteredDatasetToCsv, resultToCsv } from './report';
import type { AnalysisSpec, Dataset, Row } from './types';

const SALES = [
  'order_id,region,product,units,price,order_date,returned',
  '1,North,Widget,3,9.99,2024-01-05,no',
  '2,South,Gadget,1,24.50,2024-01-06,yes',
  '3,North,Widget,,9.99,2024-02-11,no',
  '4,East,Gizmo,7,3.25,2024-02-12,no',
  '5,South,Widget,2,9.99,2024-03-01,yes',
  '6,North,Gizmo,5,3.25,2023-12-30,no',
].join('\n');

function load(csv: string, id = 1) {
  const profile = parseCSV(csv, 'data.csv');
  return { profile, dataset: buildDataset(profile, 'upload', id) };
}

const spec = (overrides: Partial<AnalysisSpec> = {}): AnalysisSpec => ({
  filters: [],
  dimensions: [],
  measures: [{ fn: 'count' }],
  ...overrides,
});

describe('typed dataset', () => {
  const { dataset } = load(SALES);
  const column = (name: string) => dataset.columns.find((c) => c.name === name)!;

  it('infers numeric, categorical, date and boolean columns from the file', () => {
    expect(column('units').kind).toBe('numeric');
    expect(column('price').kind).toBe('numeric');
    expect(column('region').kind).toBe('categorical');
    expect(column('order_date').kind).toBe('date');
    expect(column('returned').kind).toBe('boolean');
  });

  it('reads values with their real types and keeps blanks as missing', () => {
    expect(dataset.rows[0]).toEqual({
      order_id: 1,
      region: 'North',
      product: 'Widget',
      units: 3,
      price: 9.99,
      order_date: '2024-01-05',
      returned: false,
    });
    expect(dataset.rows[2].units).toBeNull();
    expect(column('units').nullCount).toBe(1);
    expect(dataset.rows.length).toBe(6);
  });

  it('records ranges and distinct counts', () => {
    expect(column('units').min).toBe(1);
    expect(column('units').max).toBe(7);
    expect(column('order_date').min).toBe('2023-12-30');
    expect(column('order_date').max).toBe('2024-03-01');
    expect(column('region').distinctCount).toBe(3);
  });

  it('treats NA, null, none and nan as missing, like the profiler', () => {
    const { dataset: d } = load('a,b\n1,NA\n2,null\n3,none\n4,NaN\n5,7\n');
    expect(d.columns[1].nullCount).toBe(4);
    expect(d.rows.map((r) => r.b)).toEqual([null, null, null, null, 7]);
  });

  it('counts values that do not fit the column type as unreadable, and treats them as missing', () => {
    const { dataset: d } = load(
      'day\n2024-01-01\n2024-01-02\n2024-01-03\n2024-01-04\n2024-01-05\n2024-13-45\n',
    );
    expect(d.columns[0].kind).toBe('date');
    expect(d.columns[0].invalidCount).toBe(1);
    expect(d.columns[0].nullCount).toBe(1);
  });

  it('parses numbers strictly', () => {
    expect(parseNumber('12.5')).toBe(12.5);
    expect(parseNumber(' -3 ')).toBe(-3);
    expect(parseNumber('1e3')).toBe(1000);
    expect(parseNumber('.5')).toBe(0.5);
    for (const bad of ['0x10', 'Infinity', '1,000', '12abc', '', '--1']) {
      expect(parseNumber(bad), bad).toBeNull();
    }
  });

  it('normalises dates and rejects impossible ones', () => {
    expect(normalizeDate('2024-02-29')).toBe('2024-02-29');
    expect(normalizeDate('2024/3/5')).toBe('2024-03-05');
    expect(normalizeDate('2024-03-05 14:30')).toBe('2024-03-05T14:30:00');
    expect(normalizeDate('2023-02-29')).toBeNull();
    expect(normalizeDate('2024-13-01')).toBeNull();
    expect(normalizeDate('2024-01-01T25:00')).toBeNull();
    expect(normalizeDate('not a date')).toBeNull();
  });

  it('parses booleans', () => {
    expect(parseBoolean('Yes')).toBe(true);
    expect(parseBoolean('0')).toBe(false);
    expect(parseBoolean('maybe')).toBeNull();
  });
});

describe('filters', () => {
  const { dataset } = load(SALES);
  const ids = (rows: Row[]) => rows.map((r) => r.order_id);

  it('matches any of the selected values', () => {
    const rows = applyFilters(dataset.rows, [{ column: 'region', op: 'in', values: ['South', 'East'] }]);
    expect(ids(rows)).toEqual([2, 4, 5]);
  });

  it('can select missing values', () => {
    const rows = applyFilters(dataset.rows, [{ column: 'units', op: 'in', values: ['(missing)'] }]);
    expect(ids(rows)).toEqual([3]);
  });

  it('numeric ranges are inclusive and exclude missing values', () => {
    expect(ids(applyFilters(dataset.rows, [{ column: 'units', op: 'range', min: 3, max: 5 }]))).toEqual([1, 6]);
    expect(ids(applyFilters(dataset.rows, [{ column: 'units', op: 'range', min: 5 }]))).toEqual([4, 6]);
    expect(ids(applyFilters(dataset.rows, [{ column: 'units', op: 'range', max: 1 }]))).toEqual([2]);
  });

  it('date ranges are inclusive', () => {
    const rows = applyFilters(dataset.rows, [
      { column: 'order_date', op: 'dateRange', from: '2024-01-06', to: '2024-02-11' },
    ]);
    expect(ids(rows)).toEqual([2, 3]);
  });

  it('text search is case-insensitive', () => {
    expect(ids(applyFilters(dataset.rows, [{ column: 'product', op: 'contains', text: 'WID' }]))).toEqual([1, 3, 5]);
  });

  it('missing / present', () => {
    expect(ids(applyFilters(dataset.rows, [{ column: 'units', op: 'missing' }]))).toEqual([3]);
    expect(applyFilters(dataset.rows, [{ column: 'units', op: 'present' }]).length).toBe(5);
  });

  it('combines filters with AND and reports positions', () => {
    const filters = [
      { column: 'region', op: 'in' as const, values: ['North'] },
      { column: 'units', op: 'range' as const, min: 4 },
    ];
    expect(ids(applyFilters(dataset.rows, filters))).toEqual([6]);
    expect(filterIndices(dataset.rows, filters)).toEqual([5]);
  });

  it('matches nothing when no row qualifies', () => {
    expect(applyFilters(dataset.rows, [{ column: 'units', op: 'range', min: 1000 }])).toEqual([]);
  });
});

describe('aggregations on the loaded rows', () => {
  const { dataset } = load(SALES);

  it('counts rows per group, most frequent first', () => {
    const result = runAnalysis(dataset, spec({ dimensions: [{ column: 'region' }] }));
    expect(result.rows).toEqual([
      { region: 'North', count: 3 },
      { region: 'South', count: 2 },
      { region: 'East', count: 1 },
    ]);
    expect(result.inputRows).toBe(6);
    expect(result.filteredRows).toBe(6);
    expect(result.totalGroups).toBe(3);
  });

  it('sums, averages, minimums and maximums ignore missing values', () => {
    const result = runAnalysis(
      dataset,
      spec({
        dimensions: [{ column: 'region' }],
        measures: [
          { fn: 'sum', column: 'units' },
          { fn: 'avg', column: 'units' },
          { fn: 'min', column: 'price' },
          { fn: 'max', column: 'price' },
        ],
        sort: { key: 'region', direction: 'asc' },
      }),
    );
    expect(result.rows).toEqual([
      { region: 'East', 'sum(units)': 7, 'avg(units)': 7, 'min(price)': 3.25, 'max(price)': 3.25 },
      { region: 'North', 'sum(units)': 8, 'avg(units)': 4, 'min(price)': 3.25, 'max(price)': 9.99 },
      { region: 'South', 'sum(units)': 3, 'avg(units)': 1.5, 'min(price)': 9.99, 'max(price)': 24.5 },
    ]);
    expect(result.warnings.join(' ')).toContain("1 of 6 rows have no value in 'units'");
  });

  it('counts distinct values and non-missing values of a column', () => {
    const result = runAnalysis(
      dataset,
      spec({
        dimensions: [{ column: 'region' }],
        measures: [
          { fn: 'count_distinct', column: 'product' },
          { fn: 'count', column: 'units' },
        ],
        sort: { key: 'region', direction: 'asc' },
      }),
    );
    expect(result.rows).toEqual([
      { region: 'East', 'count_distinct(product)': 1, 'count(units)': 1 },
      { region: 'North', 'count_distinct(product)': 2, 'count(units)': 2 },
      { region: 'South', 'count_distinct(product)': 2, 'count(units)': 2 },
    ]);
  });

  it('without grouping returns a single row of totals', () => {
    const result = runAnalysis(
      dataset,
      spec({ measures: [{ fn: 'count' }, { fn: 'sum', column: 'units' }, { fn: 'max', column: 'order_date' }] }),
    );
    expect(result.rows).toEqual([{ count: 6, 'sum(units)': 18, 'max(order_date)': '2024-03-01' }]);
  });

  it('groups dates by year, month or day', () => {
    const byMonth = runAnalysis(
      dataset,
      spec({ dimensions: [{ column: 'order_date', bin: 'month' }], measures: [{ fn: 'sum', column: 'units' }] }),
    );
    expect(byMonth.rows).toEqual([
      { 'order_date (month)': '2023-12', 'sum(units)': 5 },
      { 'order_date (month)': '2024-01', 'sum(units)': 4 },
      { 'order_date (month)': '2024-02', 'sum(units)': 7 },
      { 'order_date (month)': '2024-03', 'sum(units)': 2 },
    ]);
    const byYear = runAnalysis(
      dataset,
      spec({ dimensions: [{ column: 'order_date', bin: 'year' }], measures: [{ fn: 'count' }] }),
    );
    expect(byYear.rows).toEqual([
      { 'order_date (year)': '2023', count: 1 },
      { 'order_date (year)': '2024', count: 5 },
    ]);
  });

  it('groups by several columns', () => {
    const result = runAnalysis(
      dataset,
      spec({ dimensions: [{ column: 'region' }, { column: 'product' }], sort: { key: 'region', direction: 'asc' } }),
    );
    expect(result.totalGroups).toBe(5);
    expect(result.rows.find((r) => r.region === 'North' && r.product === 'Widget')?.count).toBe(2);
  });

  it('applies the filters before calculating', () => {
    const result = runAnalysis(
      dataset,
      spec({
        filters: [{ column: 'order_date', op: 'dateRange', from: '2024-01-01' }],
        dimensions: [{ column: 'region' }],
        measures: [{ fn: 'sum', column: 'units' }],
        sort: { key: 'region', direction: 'asc' },
      }),
    );
    expect(result.inputRows).toBe(6);
    expect(result.filteredRows).toBe(5);
    expect(result.rows).toEqual([
      { region: 'East', 'sum(units)': 7 },
      { region: 'North', 'sum(units)': 3 },
      { region: 'South', 'sum(units)': 3 },
    ]);
  });

  it('puts missing grouping values in their own group and says so', () => {
    const { dataset: d } = load('team,score\nA,1\n,2\nA,3\nNA,4\n');
    const result = runAnalysis(d, spec({ dimensions: [{ column: 'team' }], measures: [{ fn: 'sum', column: 'score' }] }));
    expect(result.rows).toContainEqual({ team: null, 'sum(score)': 6 });
    expect(result.warnings.join(' ')).toContain('grouped as (missing)');
  });

  it('gives null (not zero) when a group has no value to aggregate', () => {
    const { dataset: d } = load('g,v\nA,\nA,NA\nB,5\n');
    const result = runAnalysis(
      d,
      spec({
        dimensions: [{ column: 'g' }],
        measures: [{ fn: 'sum', column: 'v' }, { fn: 'avg', column: 'v' }, { fn: 'min', column: 'v' }],
        sort: { key: 'g', direction: 'asc' },
      }),
    );
    expect(result.rows[0]).toEqual({ g: 'A', 'sum(v)': null, 'avg(v)': null, 'min(v)': null });
    expect(result.rows[1]).toEqual({ g: 'B', 'sum(v)': 5, 'avg(v)': 5, 'min(v)': 5 });
  });

  it('returns an honest empty result when no row matches', () => {
    const grouped = runAnalysis(
      dataset,
      spec({ filters: [{ column: 'units', op: 'range', min: 999 }], dimensions: [{ column: 'region' }] }),
    );
    expect(grouped.rows).toEqual([]);
    expect(grouped.filteredRows).toBe(0);
    expect(grouped.warnings.join(' ')).toContain('No rows match the filters');

    const totals = runAnalysis(
      dataset,
      spec({
        filters: [{ column: 'units', op: 'range', min: 999 }],
        measures: [{ fn: 'count' }, { fn: 'sum', column: 'units' }],
      }),
    );
    expect(totals.rows).toEqual([{ count: 0, 'sum(units)': null }]);
  });
});

describe('sorting and limits', () => {
  const { dataset } = load(SALES);

  it('sorts by a chosen column in either direction', () => {
    const asc = runAnalysis(
      dataset,
      spec({
        dimensions: [{ column: 'region' }],
        measures: [{ fn: 'sum', column: 'units' }],
        sort: { key: 'sum(units)', direction: 'asc' },
      }),
    );
    expect(asc.rows.map((r) => r.region)).toEqual(['South', 'East', 'North']);
    const desc = runAnalysis(
      dataset,
      spec({
        dimensions: [{ column: 'region' }],
        measures: [{ fn: 'sum', column: 'units' }],
        sort: { key: 'sum(units)', direction: 'desc' },
      }),
    );
    expect(desc.rows.map((r) => r.region)).toEqual(['North', 'East', 'South']);
  });

  it('keeps missing values last in both directions and ties in input order', () => {
    const { dataset: d } = load('g,v\nA,\nB,2\nC,2\nD,1\n');
    for (const direction of ['asc', 'desc'] as const) {
      const result = runAnalysis(
        d,
        spec({
          dimensions: [{ column: 'g' }],
          measures: [{ fn: 'sum', column: 'v' }],
          sort: { key: 'sum(v)', direction },
        }),
      );
      expect(result.rows[result.rows.length - 1].g).toBe('A');
    }
    const desc = runAnalysis(
      d,
      spec({ dimensions: [{ column: 'g' }], measures: [{ fn: 'sum', column: 'v' }], sort: { key: 'sum(v)', direction: 'desc' } }),
    );
    expect(desc.rows.map((r) => r.g)).toEqual(['B', 'C', 'D', 'A']);
  });

  it('applies a top-N limit and reports that it truncated', () => {
    const result = runAnalysis(dataset, spec({ dimensions: [{ column: 'region' }], limit: 2 }));
    expect(result.rows.map((r) => r.region)).toEqual(['North', 'South']);
    expect(result.totalGroups).toBe(3);
    expect(result.truncated).toBe(true);
    expect(runAnalysis(dataset, spec({ dimensions: [{ column: 'region' }], limit: 3 })).truncated).toBe(false);
  });
});

describe('validation', () => {
  const { dataset } = load(SALES);
  const errors = (s: Partial<AnalysisSpec>) => validateSpec(dataset, spec(s)).join(' | ');

  it('accepts a valid specification', () => {
    expect(validateSpec(dataset, spec({ dimensions: [{ column: 'region' }], measures: [{ fn: 'avg', column: 'price' }] }))).toEqual([]);
  });

  it('rejects calculations that make no sense for the column', () => {
    expect(errors({ measures: [{ fn: 'sum', column: 'region' }] })).toContain('not numeric');
    expect(errors({ measures: [{ fn: 'avg', column: 'order_date' }] })).toContain('not numeric');
    expect(errors({ measures: [{ fn: 'min', column: 'region' }] })).toContain('not numeric or a date');
    expect(errors({ measures: [{ fn: 'sum' }] })).toContain('needs a column');
  });

  it('rejects unknown columns', () => {
    expect(errors({ dimensions: [{ column: 'nope' }] })).toContain("'nope' does not exist");
    expect(errors({ measures: [{ fn: 'sum', column: 'nope' }] })).toContain("'nope' does not exist");
    expect(errors({ filters: [{ column: 'nope', op: 'present' }] })).toContain("'nope' does not exist");
  });

  it('rejects a date bin on a non-date column and too many dimensions', () => {
    expect(errors({ dimensions: [{ column: 'region', bin: 'month' }] })).toContain('not a date column');
    const many = ['region', 'product', 'returned', 'order_date'].map((column) => ({ column }));
    expect(errors({ dimensions: many })).toContain('at most 3');
    expect(errors({ dimensions: [{ column: 'region' }, { column: 'region' }] })).toContain('used twice');
  });

  it('requires at least one measure and rejects duplicates', () => {
    expect(errors({ measures: [] })).toContain('at least one measure');
    expect(errors({ measures: [{ fn: 'count' }, { fn: 'count' }] })).toContain('calculated twice');
  });

  it('checks filters against the column type and their own values', () => {
    expect(errors({ filters: [{ column: 'region', op: 'range', min: 1 }] })).toContain('needs a numeric column');
    expect(errors({ filters: [{ column: 'units', op: 'dateRange', from: '2024-01-01' }] })).toContain('needs a date column');
    expect(errors({ filters: [{ column: 'units', op: 'range' }] })).toContain('minimum or a maximum');
    expect(errors({ filters: [{ column: 'units', op: 'range', min: 5, max: 1 }] })).toContain('greater than the maximum');
    expect(errors({ filters: [{ column: 'order_date', op: 'dateRange', from: '01/02/2024' }] })).toContain('2024-01-31');
    expect(errors({ filters: [{ column: 'order_date', op: 'dateRange', from: '2024-05-01', to: '2024-01-01' }] })).toContain('after the end');
    expect(errors({ filters: [{ column: 'region', op: 'in', values: [] }] })).toContain('at least one value');
    expect(errors({ filters: [{ column: 'region', op: 'contains', text: '  ' }] })).toContain('text to look for');
  });

  it('checks limit and sort', () => {
    expect(errors({ limit: 0 })).toContain('between 1 and');
    expect(errors({ limit: 2.5 })).toContain('whole number');
    expect(errors({ sort: { key: 'nope', direction: 'asc' } })).toContain("Cannot sort by 'nope'");
  });

  it('runAnalysis refuses an invalid specification', () => {
    expect(() => runAnalysis(dataset, spec({ measures: [{ fn: 'sum', column: 'region' }] }))).toThrow(AnalysisError);
  });

  it('refuses a grouping with an unmanageable number of groups', () => {
    const rows: Row[] = Array.from({ length: MAX_GROUPS + 1 }, (_, i) => ({ id: `row-${i}` }));
    const big: Dataset = {
      id: 1,
      fileName: 'big.csv',
      origin: 'upload',
      columns: [{ name: 'id', kind: 'categorical', nullCount: 0, invalidCount: 0, distinctCount: rows.length }],
      rows,
    };
    expect(() => runAnalysis(big, spec({ dimensions: [{ column: 'id' }] }))).toThrow(/more than 50,000 groups/);
  });
});

describe('distributions', () => {
  it('lists the most frequent values, counting missing ones', () => {
    const { dataset } = load('c,d\nA,1\nB,2\nA,3\n,4\nA,5\nB,6\n');
    expect(topValues(dataset.rows, 'c')).toEqual([
      { value: 'A', count: 3 },
      { value: 'B', count: 2 },
      { value: '(missing)', count: 1 },
    ]);
    expect(topValues(dataset.rows, 'c', 1)).toEqual([{ value: 'A', count: 3 }]);
  });

  it('bins numeric values and counts missing ones', () => {
    const { dataset } = load('v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n\n');
    const result = histogram(dataset.rows, 'v', 5);
    expect(result.bins.map((b) => b.count)).toEqual([2, 2, 2, 2, 2]);
    expect(result.bins[0].from).toBe(1);
    expect(result.bins[4].to).toBe(10);
    expect(result.missing).toBe(0);
  });

  it('handles a constant column and a column without numbers', () => {
    const { dataset } = load('v,t\n5,a\n5,b\n');
    expect(histogram(dataset.rows, 'v').bins).toEqual([{ from: 5, to: 5, count: 2 }]);
    expect(histogram(dataset.rows, 't').bins).toEqual([]);
  });

  it('copes with very large columns (no argument-spread overflow)', () => {
    const rows: Row[] = Array.from({ length: MAX_ROWS }, (_, i) => ({ v: i }));
    const result = histogram(rows, 'v', 10);
    expect(result.bins.reduce((sum, b) => sum + b.count, 0)).toBe(MAX_ROWS);
  });
});

describe('plain-language description', () => {
  it('describes what is calculated', () => {
    const text = specToSentence({
      filters: [{ column: 'order_date', op: 'dateRange', from: '2024-01-01' }, { column: 'region', op: 'in', values: ['North'] }],
      dimensions: [{ column: 'product' }, { column: 'order_date', bin: 'month' }],
      measures: [{ fn: 'avg', column: 'price' }, { fn: 'count' }],
      sort: { key: 'avg(price)', direction: 'desc' },
      limit: 5,
    });
    expect(text).toBe(
      'Average of price and Number of rows by product and order_date (per month), where order_date is on or after 2024-01-01 and region is North, sorted by avg(price) (highest first), top 5.',
    );
  });
});

describe('report and exports', () => {
  const { dataset, profile } = load(SALES);
  const analysis = spec({
    question: 'Which region sells the most?',
    filters: [{ column: 'region', op: 'in', values: ['North', 'South'] }],
    dimensions: [{ column: 'region' }],
    measures: [{ fn: 'sum', column: 'units' }],
  });
  const result = runAnalysis(dataset, analysis);

  it('describes the question, the data, the analysis and the result', () => {
    const report = buildReport({ dataset, profile, spec: analysis, result, generatedAt: new Date('2026-01-02T03:04:05Z') });
    expect(report.report_type).toBe('Quaero Web browser analysis');
    expect(report.generated_at).toBe('2026-01-02T03:04:05.000Z');
    expect(report.question).toBe('Which region sells the most?');
    expect(report.dataset).toMatchObject({ file_name: 'data.csv', origin: 'upload', rows: 6 });
    expect(report.dataset.columns.map((c) => c.name)).toEqual(profile.headers);
    expect(report.analysis.measures).toEqual([{ fn: 'sum', column: 'units', key: 'sum(units)' }]);
    expect(report.analysis.filters[0].description).toBe('region is one of North, South');
    expect(report.result).toMatchObject({ rows_in_dataset: 6, rows_after_filters: 5, groups: 2, rows_included: 2, rows_omitted: 0 });
    expect(report.result.rows).toEqual(result.rows);
    expect(report.data_quality.overall_score).toBe(profile.overallDqScore);
    expect(report.notes.join(' ')).toContain('nothing was uploaded');
    expect(report.notes.join(' ')).toContain('not the Quaero Python pipeline');
    // the report describes the analysis; it does not carry the raw dataset rows
    expect(JSON.stringify(report)).not.toContain('Widget');
  });

  it('records no question when none was written', () => {
    const report = buildReport({ dataset, profile, spec: spec(), result: runAnalysis(dataset, spec()) });
    expect(report.question).toBeNull();
  });

  it('omits rows beyond the report limit and says how many', () => {
    const many = { ...result, rows: Array.from({ length: REPORT_ROW_LIMIT + 7 }, (_, i) => ({ region: `r${i}`, 'sum(units)': i })) };
    const report = buildReport({ dataset, profile, spec: analysis, result: many });
    expect(report.result.rows_included).toBe(REPORT_ROW_LIMIT);
    expect(report.result.rows_omitted).toBe(7);
  });

  it('exports the aggregated result as CSV', () => {
    const table = parseCsvText(resultToCsv(result), ',');
    expect(table[0]).toEqual(['region', 'sum(units)']);
    expect(table.slice(1)).toEqual([
      ['North', '8'],
      ['South', '3'],
    ]);
  });

  it('exports an empty result with its header only', () => {
    const empty = runAnalysis(dataset, spec({ filters: [{ column: 'units', op: 'range', min: 999 }], dimensions: [{ column: 'region' }] }));
    expect(parseCsvText(resultToCsv(empty), ',')).toEqual([['region', 'count']]);
  });

  it('exports the filtered rows exactly as they were in the file', () => {
    const odd = load('code,amount,city\n007,9.50,"Rome, IT"\n010,1.00,Milan\n123,2.25,Rome\n');
    const exported = filteredDatasetToCsv(odd.dataset, odd.profile, [{ column: 'city', op: 'contains', text: 'rome' }]);
    expect(exported.rows).toBe(2);
    expect(parseCsvText(exported.csv, ',')).toEqual([
      ['code', 'amount', 'city'],
      ['007', '9.50', 'Rome, IT'],
      ['123', '2.25', 'Rome'],
    ]);
  });

  it('exports the whole file when nothing is filtered and nothing when no row matches', () => {
    expect(filteredDatasetToCsv(dataset, profile, []).rows).toBe(6);
    const none = filteredDatasetToCsv(dataset, profile, [{ column: 'units', op: 'range', min: 999 }]);
    expect(none.rows).toBe(0);
    expect(parseCsvText(none.csv, ',')).toEqual([profile.headers]);
  });
});

describe('only the loaded data is used', () => {
  it('gives different answers for different files and nothing predefined', () => {
    const first = load('k,v\na,1\nb,2\n', 1);
    const second = load('k,v\nx,10\nx,20\ny,5\n', 2);
    const analysis = spec({ dimensions: [{ column: 'k' }], measures: [{ fn: 'sum', column: 'v' }], sort: { key: 'k', direction: 'asc' } });
    expect(runAnalysis(first.dataset, analysis).rows).toEqual([
      { k: 'a', 'sum(v)': 1 },
      { k: 'b', 'sum(v)': 2 },
    ]);
    expect(runAnalysis(second.dataset, analysis).rows).toEqual([
      { k: 'x', 'sum(v)': 30 },
      { k: 'y', 'sum(v)': 5 },
    ]);
    expect(first.dataset.id).not.toBe(second.dataset.id);
  });

  it('the analysis never mutates the dataset', () => {
    const { dataset } = load(SALES);
    const before = JSON.stringify(dataset.rows);
    runAnalysis(dataset, spec({ dimensions: [{ column: 'region' }], sort: { key: 'count', direction: 'asc' }, limit: 1 }));
    expect(JSON.stringify(dataset.rows)).toBe(before);
  });
});

describe('size limits', () => {
  it('rejects empty and oversized files before reading them', () => {
    expect(checkFileSize(0)).toContain('empty');
    expect(checkFileSize(MAX_FILE_BYTES + 1)).toContain('limited to 10.0 MB');
    expect(checkFileSize(MAX_FILE_BYTES + 1)).toContain('run Quaero locally');
    expect(checkFileSize(MAX_FILE_BYTES)).toBeNull();
    expect(checkFileSize(1234)).toBeNull();
  });

  it('rejects datasets with no data, too many rows or too many columns', () => {
    expect(checkParsedSize(0, 3)).toContain('no data rows');
    expect(checkParsedSize(5, 0)).toContain('No columns');
    expect(checkParsedSize(MAX_ROWS + 1, 3)).toContain('200,000');
    expect(checkParsedSize(10, MAX_COLUMNS + 1)).toContain('201 columns');
    expect(checkParsedSize(MAX_ROWS, MAX_COLUMNS)).toBeNull();
  });

  it('a header-only file produces an empty dataset that the limits reject', () => {
    const { dataset } = load('a,b,c\n');
    expect(dataset.rows).toEqual([]);
    expect(checkParsedSize(dataset.rows.length, dataset.columns.length)).toContain('no data rows');
  });

  it('a single-column file works', () => {
    const { dataset } = load('x\n1\n2\n3\n');
    expect(runAnalysis(dataset, spec({ measures: [{ fn: 'sum', column: 'x' }] })).rows).toEqual([{ 'sum(x)': 6 }]);
  });
});

describe('floating-point noise', () => {
  it('does not leak binary rounding errors into results or exports', () => {
    const { dataset } = load(['id,group,amount', '1,a,0.1', '2,a,0.2', '3,b,58767.07', '4,b,0.0'].join('\n'));
    const result = runAnalysis(
      dataset,
      spec({ dimensions: [{ column: 'group' }], measures: [{ fn: 'sum', column: 'amount' }, { fn: 'avg', column: 'amount' }] }),
    );
    // plain addition gives 0.1 + 0.2 = 0.30000000000000004
    expect(result.rows.find((r) => r.group === 'a')).toMatchObject({ 'sum(amount)': 0.3, 'avg(amount)': 0.15 });
    expect(resultToCsv(result)).toContain('a,0.3,0.15');
  });
});
