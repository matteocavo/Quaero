import { describe, expect, it } from 'vitest';
import { CsvFormatError, csvCell, detectDelimiter, parseCsvText, toCsv } from './csv';
import { parseCSV } from './csvProfiler';

describe('parseCsvText (RFC 4180)', () => {
  it('parses plain records', () => {
    expect(parseCsvText('a,b\n1,2\n3,4', ',')).toEqual([
      ['a', 'b'],
      ['1', '2'],
      ['3', '4'],
    ]);
  });

  it('keeps line breaks and delimiters inside quoted fields (regression: multi-line values)', () => {
    const rows = parseCsvText('id,note,amount\n1,"said ""hi"", ok",3\n2,"line1\nline2",4\n', ',');
    expect(rows).toEqual([
      ['id', 'note', 'amount'],
      ['1', 'said "hi", ok', '3'],
      ['2', 'line1\nline2', '4'],
    ]);
  });

  it('handles CRLF, CR and a UTF-8 byte order mark', () => {
    expect(parseCsvText('﻿a,b\r\n1,2\r\n3,4\r\n', ',')).toEqual([
      ['a', 'b'],
      ['1', '2'],
      ['3', '4'],
    ]);
    expect(parseCsvText('a,b\r1,2', ',')).toEqual([
      ['a', 'b'],
      ['1', '2'],
    ]);
  });

  it('skips blank lines but keeps empty cells', () => {
    expect(parseCsvText('a,b\n\n1,\n,2\n\n', ',')).toEqual([
      ['a', 'b'],
      ['1', ''],
      ['', '2'],
    ]);
  });

  it('trims unquoted cells and preserves quoted ones exactly', () => {
    expect(parseCsvText('a,b\n  x  ," y "', ',')).toEqual([
      ['a', 'b'],
      ['x', ' y '],
    ]);
  });

  it('treats a quote in the middle of a cell as a literal character', () => {
    expect(parseCsvText('size\n5" pipe', ',')).toEqual([['size'], ['5" pipe']]);
  });

  it('supports semicolon and tab delimiters', () => {
    expect(parseCsvText('a;b\n1;2', ';')).toEqual([
      ['a', 'b'],
      ['1', '2'],
    ]);
    expect(parseCsvText('a\tb\n1\t2', '\t')).toEqual([
      ['a', 'b'],
      ['1', '2'],
    ]);
  });

  it('rejects a quoted field that is never closed', () => {
    expect(() => parseCsvText('a,b\n1,"oops', ',')).toThrow(CsvFormatError);
  });

  it('returns no records for empty input', () => {
    expect(parseCsvText('', ',')).toEqual([]);
    expect(parseCsvText('\n\n', ',')).toEqual([]);
  });
});

describe('detectDelimiter', () => {
  it('ignores delimiters inside quotes in the header', () => {
    expect(detectDelimiter('"a,b,c";d;e\n1;2;3')).toBe(';');
  });

  it('falls back to a comma', () => {
    expect(detectDelimiter('single_column\n1')).toBe(',');
  });
});

describe('csvCell / toCsv', () => {
  it('quotes and escapes per RFC 4180 (regression: values were JSON-escaped with backslashes)', () => {
    expect(csvCell('said "hi", ok')).toBe('"said ""hi"", ok"');
    expect(csvCell('line1\nline2')).toBe('"line1\nline2"');
    expect(csvCell(' padded ')).toBe('" padded "');
    expect(csvCell('plain')).toBe('plain');
  });

  it('writes numbers and booleans without quotes and nulls as empty', () => {
    expect(csvCell(12.5)).toBe('12.5');
    expect(csvCell(-3)).toBe('-3');
    expect(csvCell(true)).toBe('true');
    expect(csvCell(null)).toBe('');
    expect(csvCell(undefined)).toBe('');
    expect(csvCell(Number.NaN)).toBe('');
  });

  it('neutralises spreadsheet formulas in text but not negative numbers', () => {
    expect(csvCell('=SUM(A1:A2)')).toBe("'=SUM(A1:A2)");
    expect(csvCell('+1 555')).toBe("'+1 555");
    expect(csvCell('@cmd')).toBe("'@cmd");
    expect(csvCell('-cmd')).toBe("'-cmd");
    expect(csvCell('-12.5')).toBe('-12.5');
    expect(csvCell('-1e3')).toBe('-1e3');
  });

  it('uses CRLF and a header row', () => {
    expect(toCsv(['a', 'b'], [{ a: 1, b: 'x' }, { a: 2 }])).toBe('a,b\r\n1,x\r\n2,');
  });

  it('round-trips awkward data exactly', () => {
    const columns = ['id', 'note', 'empty'];
    const rows = [
      { id: '1', note: 'said "hi", ok', empty: '' },
      { id: '2', note: 'line1\nline2', empty: '' },
      { id: '3', note: ' leading', empty: '' },
    ];
    const parsed = parseCsvText(toCsv(columns, rows), ',');
    expect(parsed.slice(1).map((r) => Object.fromEntries(columns.map((c, i) => [c, r[i]])))).toEqual(
      rows,
    );
  });
});

describe('parseCSV with the RFC 4180 reader', () => {
  it('imports a multi-line quoted value as one row (regression)', () => {
    const profile = parseCSV('id,note\n1,"line1\nline2"\n2,ok\n', 'multi.csv');
    expect(profile.totalRows).toBe(2);
    expect(profile.allRows[0].note).toBe('line1\nline2');
  });

  it('makes duplicate and empty header names unique instead of overwriting data', () => {
    const profile = parseCSV('a,a,,b\n1,2,3,4\n', 'dup.csv');
    expect(profile.headers).toEqual(['a', 'a_2', 'column_3', 'b']);
    expect(profile.allRows[0]).toEqual({ a: '1', a_2: '2', column_3: '3', b: '4' });
  });

  it('accepts a byte order mark and semicolon files', () => {
    const profile = parseCSV('﻿a;b\n1;2\n', 'bom.csv');
    expect(profile.headers).toEqual(['a', 'b']);
    expect(profile.delimiter).toBe(';');
  });

  it('exports what it imported', () => {
    const profile = parseCSV('id,note\n1,"said ""hi"", ok"\n', 'x.csv');
    const back = parseCSV(toCsv(profile.headers, profile.allRows), 'x.csv');
    expect(back.allRows).toEqual(profile.allRows);
  });
});
