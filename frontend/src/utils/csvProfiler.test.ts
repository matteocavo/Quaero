import { describe, expect, it } from 'vitest';
import {
  SAMPLE_GLOBAL_INDICATORS_CSV,
  SAMPLE_RELEASE_IMPACT_CSV,
  detectDelimiter,
  parseCSV,
} from './csvProfiler';

describe('CSV profiler (runs in the browser only)', () => {
  it('profiles the bundled release_impact sample', () => {
    const profile = parseCSV(SAMPLE_RELEASE_IMPACT_CSV, 'release_impact_sample.csv');
    expect(profile.totalRows).toBe(5);
    expect(profile.totalColumns).toBe(4);
    expect(profile.headers).toEqual(['release_year', 'provider', 'catalog', 'streams']);
    const streams = profile.columns.find((c) => c.name === 'streams');
    expect(streams?.dtype).toBe('integer');
    expect(streams?.completeness).toBe(100);
    expect(profile.overallDqScore).toBeGreaterThan(0);
    expect(profile.overallDqScore).toBeLessThanOrEqual(100);
  });

  it('profiles the illustrative macro-indicators sample', () => {
    const profile = parseCSV(SAMPLE_GLOBAL_INDICATORS_CSV, 'global_economic_indicators_sample.csv');
    expect(profile.totalRows).toBe(5);
    expect(profile.headers).toContain('inflation_pct');
  });

  it('detects delimiters', () => {
    expect(detectDelimiter('a,b,c\n1,2,3')).toBe(',');
    expect(detectDelimiter('a;b;c\n1;2;3')).toBe(';');
    expect(detectDelimiter('a\tb\tc\n1\t2\t3')).toBe('\t');
  });

  it('handles quoted fields, escaped quotes and CRLF', () => {
    const profile = parseCSV('name,note\r\n"Smith, J","said ""hi"""\r\nLee,ok\r\n', 'q.csv');
    expect(profile.totalRows).toBe(2);
    expect(profile.previewRows[0].name).toBe('Smith, J');
    expect(profile.previewRows[0].note).toBe('said "hi"');
  });

  it('measures missing values', () => {
    const profile = parseCSV('a,b\n1,\n2,x\n3,\n', 'm.csv');
    const b = profile.columns.find((c) => c.name === 'b');
    expect(b?.nullCount).toBe(2);
    expect(b?.completeness).toBeLessThan(100);
  });

  it('rejects empty input', () => {
    expect(() => parseCSV('', 'empty.csv')).toThrow('empty');
    expect(() => parseCSV('\n\n  \n', 'blank.csv')).toThrow('empty');
  });

  it('never performs network requests', () => {
    const before = globalThis.fetch;
    parseCSV(SAMPLE_RELEASE_IMPACT_CSV);
    expect(globalThis.fetch).toBe(before);
  });
});
