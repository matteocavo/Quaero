import { describe, expect, it } from 'vitest';
import { parseCSV } from '../utils/csvProfiler';
import {
  ADVISOR_RULES,
  adviseColumns,
  adviseDataset,
  adviseUploadSetup,
  chooseBin,
  classifyColumn,
  numericFacts,
  usableAsMetric,
} from './advisor';
import { buildDataset } from './dataset';
import { runAnalysis, validateSpec } from './query';

const load = (csv: string) => {
  const profile = parseCSV(csv, 'test.csv');
  return { profile, dataset: buildDataset(profile, 'upload', 1) };
};
const csvOf = (header: string[], rows: (string | number)[][]) => [header.join(','), ...rows.map((r) => r.join(','))].join('\n');

/** The example from the brief: listeners and saves are zero everywhere, streams is the real measure. */
const SONGS = (() => {
  const years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];
  const streams = [1200, 3400, 2800, 5100, 4300, 6100, 5800, 7200, 6900, 250000];
  return csvOf(
    ['song', 'listeners', 'streams', 'saves', 'release_date'],
    years.map((y, i) => [`Song ${String.fromCharCode(65 + i)}`, 0, streams[i], 0, `${y}-03-1${i % 9}`]),
  );
})();

describe('column rules', () => {
  it('finds the measure, the label and the time column, and sets the constant zeros aside', () => {
    const { dataset } = load(SONGS);
    const roles = Object.fromEntries(adviseColumns(dataset).map((c) => [c.name, c.role]));
    expect(roles).toEqual({ song: 'label', listeners: 'unused', streams: 'metric', saves: 'unused', release_date: 'time' });
  });

  it('says what is wrong with a constant column, with the value', () => {
    const { dataset } = load(SONGS);
    const listeners = adviseColumns(dataset).find((c) => c.name === 'listeners')!;
    expect(listeners.concerns).toEqual([
      { code: 'constant', severity: 'warning', text: 'Every value is 0, so it cannot tell rows apart. Not used for suggestions.' },
    ]);
  });

  it('names the value of a constant text column', () => {
    const { dataset } = load(csvOf(['region', 'sales'], [['EU', 1], ['EU', 2], ['EU', 3]]));
    const region = adviseColumns(dataset).find((c) => c.name === 'region')!;
    expect(region.role).toBe('unused');
    expect(region.concerns[0].text).toContain('Every value is “EU”');
  });

  it('leaves out a column that is mostly missing, and only mentions one with a few gaps', () => {
    const rows = Array.from({ length: 10 }, (_, i) => [i, i < 6 ? '' : i * 5, i < 2 ? '' : i * 2]);
    const { dataset } = load(csvOf(['id', 'sparse', 'gappy'], rows));
    const byName = Object.fromEntries(adviseColumns(dataset).map((c) => [c.name, c]));
    expect(byName.sparse.role).toBe('unused');
    expect(byName.sparse.concerns[0]).toMatchObject({ code: 'mostly_missing', severity: 'warning' });
    expect(byName.sparse.concerns[0].text).toContain('6 rows (60%) have no value');
    expect(byName.gappy.role).toBe('metric');
    expect(byName.gappy.concerns[0]).toMatchObject({ code: 'some_missing', severity: 'info' });
    expect(byName.gappy.concerns[0].text).toContain('2 rows (20%) have no value; sums and averages leave them out');
  });

  it('flags a text column that is almost always the same value, but not on a handful of rows', () => {
    const many = Array.from({ length: 40 }, (_, i) => [i, i < 39 ? 'Active' : 'Closed', i]);
    const flagged = adviseColumns(load(csvOf(['id', 'status', 'n'], many)).dataset).find((c) => c.name === 'status')!;
    expect(flagged.role).toBe('unused');
    expect(flagged.concerns[0]).toMatchObject({ code: 'almost_constant' });
    expect(flagged.concerns[0].text).toContain('“Active” is 97.5% of its values');

    const few = Array.from({ length: 6 }, (_, i) => [i, i < 5 ? 'Active' : 'Closed', i]);
    expect(adviseColumns(load(csvOf(['id', 'status', 'n'], few)).dataset).find((c) => c.name === 'status')!.role).toBe('category');
  });

  it('treats a key as an identifier but not a numeric column that merely has unique values', () => {
    const rows = Array.from({ length: 12 }, (_, i) => [1000 + i, `${(i * 13.37).toFixed(2)}`, `C${i % 3}`]);
    const roles = Object.fromEntries(adviseColumns(load(csvOf(['customer_id', 'revenue', 'tier'], rows)).dataset).map((c) => [c.name, c.role]));
    expect(roles).toEqual({ customer_id: 'identifier', revenue: 'metric', tier: 'category' });
  });

  it('uses a text column with many different values as a label, and few values as a category', () => {
    const rows = Array.from({ length: 60 }, (_, i) => [`item_${i}`, `group_${i % 4}`, `bucket_${i % 40}`, i]);
    const roles = Object.fromEntries(adviseColumns(load(csvOf(['item', 'grp', 'bucket', 'n'], rows)).dataset).map((c) => [c.name, c.role]));
    expect(roles).toMatchObject({ item: 'label', grp: 'category', bucket: 'label' }); // 40 > 30 distinct
  });

  it('counts an unreadable value as missing once', () => {
    const csv = ['day', '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-13-45'].join('\n');
    const day = adviseColumns(load(csv).dataset)[0];
    expect(day.concerns.map((c) => c.code).sort()).toEqual(['some_missing', 'unreadable']);
    expect(day.concerns.find((c) => c.code === 'some_missing')!.text).toContain('1 row (16.7%)');
  });

  it('usableAsMetric rejects constant and key columns', () => {
    const { dataset } = load(SONGS);
    const usable = dataset.columns.filter((c) => usableAsMetric(dataset, c)).map((c) => c.name);
    expect(usable).toEqual(['streams']);
  });
});

describe('suggestions for the example CSV', () => {
  it('proposes exactly the analyses that the data supports, none about the constant columns', () => {
    const advice = adviseDataset(load(SONGS).dataset);
    expect(advice.suggestions.map((s) => s.id)).toEqual([
      'trend:streams:release_date',
      'top:streams:song',
      'distribution:streams',
      'outliers:streams',
    ]);
    expect(advice.suggestions.map((s) => s.title)).toEqual([
      'streams per year of release_date',
      'Top 10 song by streams',
      'Distribution of streams',
      'Unusual values in streams',
    ]);
    expect(JSON.stringify(advice.suggestions)).not.toMatch(/listeners|saves/);
    expect(advice.hidden).toBe(0);
    expect(advice.nothingBecause).toBeNull();
  });

  it('turns the query suggestions into specs for the existing query builder', () => {
    const [trend, top] = adviseDataset(load(SONGS).dataset).suggestions;
    expect(trend.spec).toEqual({ filters: [], dimensions: [{ column: 'release_date', bin: 'year' }], measures: [{ fn: 'sum', column: 'streams' }] });
    expect(top.spec).toEqual({
      filters: [],
      dimensions: [{ column: 'song' }],
      measures: [{ fn: 'sum', column: 'streams' }],
      sort: { key: 'sum(streams)', direction: 'desc' },
      limit: 10,
    });
  });

  it('shows the facts, the exact calculation and the limits of each one', () => {
    const [trend, top, distribution, outliers] = adviseDataset(load(SONGS).dataset).suggestions;
    expect(trend.because).toEqual([
      expect.stringMatching(/^release_date is a date column from 2015-03-10 to 2024-03-19$|^release_date is a date column from /),
      'streams is a numeric column: 10 values, from 1,200 to 250,000, average 29,280',
    ]);
    expect(trend.calculation).toBe('Sum of streams by release_date (per year).');
    expect(trend.limits[0]).toContain('A period with more rows usually has a larger sum');
    expect(top.because[0]).toBe('song has 10 different values (one per row), so it names items in a ranking');
    expect(top.calculation).toBe('Sum of streams by song, sorted by sum(streams) (highest first), top 10.');
    expect(distribution.calculation).toBe('Rows are counted in 10 equal-width ranges between 1,200 and 250,000.');
    expect(distribution.limits).toEqual(['Equal-width ranges: a few extreme values can leave most rows in one bar.']);
    expect(outliers.calculation).toContain('(1.5 x the interquartile range');
    expect(outliers.limits).toEqual(['Unusual is not the same as wrong: it only means far from most values.']);
    for (const s of [trend, top, distribution, outliers]) {
      expect(s.because.length).toBeGreaterThan(0);
      expect(s.calculation.length).toBeGreaterThan(0);
      expect(s.limits.length).toBeGreaterThan(0);
    }
  });

  it('every query suggestion is valid and runs on the existing engine, with the right numbers', () => {
    const { dataset } = load(SONGS);
    for (const s of adviseDataset(dataset).suggestions.filter((x) => x.kind === 'query')) {
      expect(validateSpec(dataset, s.spec!), s.id).toEqual([]);
      expect(runAnalysis(dataset, s.spec!).rows.length, s.id).toBeGreaterThan(0);
    }
    const [, top] = adviseDataset(dataset).suggestions;
    const result = runAnalysis(dataset, top.spec!);
    expect(result.rows[0]).toMatchObject({ song: 'Song J', 'sum(streams)': 250000 }); // independently: max of the column
    expect(result.rows).toHaveLength(10);
    const [trend] = adviseDataset(dataset).suggestions;
    expect(runAnalysis(dataset, trend.spec!).rows.map((r) => r['sum(streams)'])).toEqual([1200, 3400, 2800, 5100, 4300, 6100, 5800, 7200, 6900, 250000]);
  });

  it('counts unusual values with the same rule as the Data Quality profile', () => {
    const { dataset, profile } = load(SONGS);
    const facts = numericFacts(dataset.rows, 'streams')!;
    const streams = profile.columns.find((c) => c.name === 'streams')!;
    expect(facts.below + facts.above).toBe(streams.outlierCount);
    expect(facts.above).toBe(1);
    const outliers = adviseDataset(dataset).suggestions.find((s) => s.kind === 'outliers')!;
    expect(outliers.because[0]).toContain('1 value (10%)');
  });

  it('uses the 1.5 x IQR rule exactly, also for a value that is only mildly unusual', () => {
    // 10..19 and 29: Q1 = 12, Q3 = 18, so the upper fence is 27 at 1.5 x IQR (and 36 at 3 x IQR).
    const values = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 29];
    const { dataset, profile } = load(csvOf(['n'], values.map((v) => [v])));
    const facts = numericFacts(dataset.rows, 'n')!;
    expect({ q1: facts.q1, q3: facts.q3, upper: facts.upperFence, above: facts.above }).toEqual({ q1: 12, q3: 18, upper: 27, above: 1 });
    expect(profile.columns[0].outlierCount).toBe(1);
    expect(facts.above).toBe(profile.columns[0].outlierCount);
  });

  it('invents nothing: only plain facts, no scores or confidence', () => {
    const keys = new Set(adviseDataset(load(SONGS).dataset).suggestions.flatMap((s) => Object.keys(s)));
    expect([...keys].sort()).toEqual(['because', 'calculation', 'chart', 'column', 'id', 'kind', 'limits', 'spec', 'title']);
  });
});

describe('suggestions in general', () => {
  const sales = (n = 60) =>
    csvOf(
      ['order_id', 'region', 'product', 'units', 'price', 'order_date'],
      Array.from({ length: n }, (_, i) => [i + 1, ['North', 'South', 'East'][i % 3], `P${i}`, (i % 7) + 1, 5 + (i % 4), `2022-0${(i % 9) + 1}-15`]),
    );

  it('is deterministic', () => {
    const a = adviseDataset(load(sales()).dataset);
    const b = adviseDataset(load(sales()).dataset);
    expect(b).toEqual(a);
  });

  it('compares categories with both a sum and an average, and says why', () => {
    const advice = adviseDataset(load(sales()).dataset);
    const compare = advice.suggestions.find((s) => s.id === 'compare:units:region')!;
    expect(compare.spec!.measures).toEqual([{ fn: 'sum', column: 'units' }, { fn: 'avg', column: 'units' }]);
    expect(compare.because[0]).toBe('region has 3 distinct values, few enough to compare');
    expect(compare.limits.join(' ')).toContain('Groups with more rows have larger sums');
  });

  it('names the rows it leaves out', () => {
    const rows = Array.from({ length: 12 }, (_, i) => [`s${i}`, i === 2 || i === 5 ? '' : (i + 1) * 100, `2024-0${(i % 9) + 1}-10`]);
    const { suggestions } = adviseDataset(load(csvOf(['name', 'amount', 'day'], rows)).dataset);
    expect(suggestions.find((s) => s.id === 'top:amount:name')!.limits[0]).toBe('2 rows with no name or no amount are left out.');
  });

  it('caps the list, keeps file order, and says how many were not shown', () => {
    const header = ['name', 'grp', 'when', 'a', 'b', 'c', 'd'];
    const rows = Array.from({ length: 30 }, (_, i) => [`n${i}`, `g${i % 3}`, `2020-0${(i % 9) + 1}-05`, i * 3 + (i === 9 ? 999 : 0), i * 7, i * 11, i * 13]);
    const advice = adviseDataset(load(csvOf(header, rows)).dataset);
    expect(advice.suggestions).toHaveLength(ADVISOR_RULES.maxSuggestions);
    expect(advice.hidden).toBeGreaterThan(0);
    const metrics = new Set(advice.suggestions.map((s) => s.id.split(':')[1]));
    expect([...metrics].every((m) => ['a', 'b', 'c'].includes(m))).toBe(true); // the first three, never d
    expect(advice.suggestions[0].id.startsWith('trend:a:')).toBe(true);
  });

  it('counts rows when there is nothing to measure', () => {
    const rows = Array.from({ length: 9 }, (_, i) => [`item${i}`, ['x', 'y', 'z'][i % 3]]);
    const advice = adviseDataset(load(csvOf(['item', 'kind'], rows)).dataset);
    expect(advice.suggestions.map((s) => s.id)).toEqual(['count:kind']);
    expect(advice.suggestions[0].spec).toMatchObject({ measures: [{ fn: 'count' }] });
    expect(advice.suggestions[0].because[1]).toBe('There is no numeric column to sum or average');
  });

  it('says so, rather than inventing something, when nothing can be analyzed', () => {
    const advice = adviseDataset(load(csvOf(['a', 'b'], [[1, 'x'], [1, 'x'], [1, 'x']])).dataset);
    expect(advice.suggestions).toEqual([]);
    expect(advice.nothingBecause).toBe('Every column is empty, constant, mostly missing or a key, so there is nothing to analyze.');
  });

  it('chooses year, month or day for dates by their span', () => {
    expect(chooseBin('2015-01-01', '2024-05-01')).toBe('year');
    expect(chooseBin('2023-01-15', '2023-09-01')).toBe('month');
    expect(chooseBin('2024-03-01', '2024-03-20')).toBe('day');
  });

  it('works on a column of a single row without failing', () => {
    expect(() => adviseDataset(load(csvOf(['a', 'b'], [[1, 2]])).dataset)).not.toThrow();
    expect(numericFacts([], 'a')).toBeNull();
  });
});

describe('setup advice for a Python pipeline run (from the upload schema)', () => {
  const col = (name: string, dtype: string, nulls: number, unique: number, sample: string[]) => ({
    name,
    normalized_name: name,
    dtype,
    null_count: nulls,
    unique_count: unique,
    sample_values: sample,
  });
  const schema = {
    row_count: 10,
    numeric_columns: ['listeners', 'streams', 'saves'],
    candidate_time_columns: ['release_date'],
    columns: [
      col('song', 'string', 0, 10, ['Song A']),
      col('listeners', 'integer', 0, 1, ['0']),
      col('streams', 'integer', 0, 10, ['1200']),
      col('saves', 'integer', 0, 1, ['0']),
      col('release_date', 'datetime', 0, 10, ['2015-03-10']),
    ],
  };

  it('picks the measure that varies, not the first numeric column, and explains the constants', () => {
    const setup = adviseUploadSetup(schema);
    expect(setup).toMatchObject({ metric: 'streams', dimension: 'song', time: 'release_date' });
    expect(setup.notes).toEqual([
      { column: 'listeners', text: 'Every value is 0, so it cannot tell rows apart. Not used for suggestions.' },
      { column: 'saves', text: 'Every value is 0, so it cannot tell rows apart. Not used for suggestions.' },
    ]);
  });

  it('follows the server: a year stored as a number is time, and a *_id column is not a measure', () => {
    const setup = adviseUploadSetup({
      row_count: 20,
      numeric_columns: ['revenue'],
      candidate_time_columns: ['order_year'],
      columns: [
        col('order_year', 'integer', 0, 4, ['2021']),
        col('customer_id', 'integer', 0, 20, ['1001']),
        col('revenue', 'float', 0, 18, ['14500.5']),
        col('segment', 'string', 0, 3, ['SMB']),
      ],
    });
    expect(setup).toMatchObject({ metric: 'revenue', dimension: 'segment', time: 'order_year' });
  });

  it('returns nothing rather than guessing when no column qualifies', () => {
    const setup = adviseUploadSetup({ row_count: 5, numeric_columns: [], candidate_time_columns: [], columns: [col('note', 'string', 5, 0, [])] });
    expect(setup).toMatchObject({ metric: null, dimension: null, time: null });
  });
});

describe('classifyColumn directly', () => {
  it('does not judge dominance without the most frequent value', () => {
    const r = classifyColumn({ name: 'x', kind: 'categorical', rows: 100, missing: 0, distinct: 5 });
    expect(r.role).toBe('category');
  });
});
