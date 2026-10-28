/**
 * Browser-side CSV parser and data quality profiler.
 * Runs 100% locally in the browser with zero external network requests.
 * Computes descriptive statistics and column data quality metrics matching
 * the Quaero pipeline scoring logic (pipelines/dq_scorer.py).
 */

export interface ColumnProfile {
  name: string;
  dtype: 'integer' | 'float' | 'date' | 'boolean' | 'string';
  totalCount: number;
  nullCount: number;
  completeness: number; // 0 - 100%
  distinctCount: number;
  uniqueness: number;   // 0 - 100%
  formatScore: number;  // 0 - 100%
  dqScore: number;      // 0 - 100%
  outlierFlag: boolean;
  outlierCount: number;
  stats?: {
    min?: number | string;
    max?: number | string;
    mean?: number;
    median?: number;
    topValues?: { value: string; count: number }[];
  };
}

export interface DatasetProfile {
  fileName: string;
  delimiter: string;
  totalRows: number;
  totalColumns: number;
  duplicateRows: number;
  overallDqScore: number;
  headers: string[];
  columns: ColumnProfile[];
  previewRows: Record<string, any>[];
  allRows: Record<string, any>[];
}

export function detectDelimiter(text: string): string {
  const firstLine = text.split('\n')[0] || '';
  const commaCount = (firstLine.match(/,/g) || []).length;
  const semicolonCount = (firstLine.match(/;/g) || []).length;
  const tabCount = (firstLine.match(/\t/g) || []).length;

  if (tabCount > commaCount && tabCount > semicolonCount) return '\t';
  if (semicolonCount > commaCount) return ';';
  return ',';
}

export function parseCSV(rawText: string, fileName: string = 'dataset.csv'): DatasetProfile {
  const text = rawText.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = text.split('\n').filter((l) => l.trim().length > 0);

  if (lines.length === 0) {
    throw new Error('The provided CSV file is empty.');
  }

  const delimiter = detectDelimiter(text);

  // Parse lines into cells considering quotes
  const parseLine = (line: string): string[] => {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === delimiter && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };

  const rawHeaders = parseLine(lines[0]);
  const headers = rawHeaders.map((h, i) => (h ? h.replace(/^["']|["']$/g, '') : `column_${i + 1}`));

  const allRows: Record<string, any>[] = [];
  const rowHashTracker = new Set<string>();
  let duplicateRows = 0;

  for (let i = 1; i < lines.length; i++) {
    const rawCells = parseLine(lines[i]);
    if (rawCells.length === 1 && rawCells[0] === '') continue;

    const rowObj: Record<string, any> = {};
    const rowHash = rawCells.join('|');
    if (rowHashTracker.has(rowHash)) {
      duplicateRows++;
    } else {
      rowHashTracker.add(rowHash);
    }

    headers.forEach((header, colIdx) => {
      let cell = rawCells[colIdx] ?? '';
      // Strip surrounding quotes
      if (cell.startsWith('"') && cell.endsWith('"')) {
        cell = cell.slice(1, -1).replace(/""/g, '"');
      }
      rowObj[header] = cell;
    });

    allRows.push(rowObj);
  }

  // Profile each column
  const totalRows = allRows.length;
  const columnProfiles: ColumnProfile[] = headers.map((colName) => {
    const values = allRows.map((r) => r[colName]);

    // Check for null / blank
    const nullValues = values.filter(
      (v) =>
        v === undefined ||
        v === null ||
        v === '' ||
        String(v).toLowerCase() === 'null' ||
        String(v).toLowerCase() === 'nan' ||
        String(v).toLowerCase() === 'none' ||
        String(v).toLowerCase() === 'na',
    );
    const nullCount = nullValues.length;
    const nonNullValues = values.filter(
      (v) =>
        v !== undefined &&
        v !== null &&
        v !== '' &&
        String(v).toLowerCase() !== 'null' &&
        String(v).toLowerCase() !== 'nan' &&
        String(v).toLowerCase() !== 'none' &&
        String(v).toLowerCase() !== 'na',
    );

    const nonNullCount = nonNullValues.length;
    const completeness = totalRows > 0 ? Number(((nonNullCount / totalRows) * 100).toFixed(1)) : 0;

    const uniqueSet = new Set(nonNullValues);
    const distinctCount = uniqueSet.size;
    const uniqueness =
      nonNullCount > 0 ? Number(((distinctCount / nonNullCount) * 100).toFixed(1)) : 0;

    // Type Inference
    let numericCount = 0;
    let integerCount = 0;
    let dateCount = 0;
    let booleanCount = 0;

    const numericVals: number[] = [];

    nonNullValues.forEach((val) => {
      const strVal = String(val).trim();
      const num = Number(strVal);

      if (!isNaN(num) && strVal !== '') {
        numericCount++;
        numericVals.push(num);
        if (Number.isInteger(num)) {
          integerCount++;
        }
      } else if (
        ['true', 'false', '1', '0', 'yes', 'no'].includes(strVal.toLowerCase())
      ) {
        booleanCount++;
      } else if (
        /^\d{4}[-/]\d{2}[-/]\d{2}/.test(strVal) ||
        !isNaN(Date.parse(strVal)) && strVal.length >= 8
      ) {
        dateCount++;
      }
    });

    let dtype: ColumnProfile['dtype'] = 'string';
    if (numericCount === nonNullCount && nonNullCount > 0) {
      dtype = integerCount === numericCount ? 'integer' : 'float';
    } else if (dateCount > nonNullCount * 0.8 && nonNullCount > 0) {
      dtype = 'date';
    } else if (booleanCount === nonNullCount && nonNullCount > 0) {
      dtype = 'boolean';
    }

    // Format Score (consistency of primary type)
    let formatScore = 100;
    if (nonNullCount > 0) {
      if (dtype === 'integer' || dtype === 'float') {
        formatScore = Number(((numericCount / nonNullCount) * 100).toFixed(1));
      } else if (dtype === 'date') {
        formatScore = Number(((dateCount / nonNullCount) * 100).toFixed(1));
      } else {
        formatScore = 100;
      }
    }

    // Quaero weighted DQ score: completeness 40% + uniqueness 20% + format 40%
    const dqScore = Number(
      (0.4 * completeness + 0.2 * uniqueness + 0.4 * formatScore).toFixed(1),
    );

    // Outlier Detection (using 1.5 * IQR for robustness)
    let outlierCount = 0;
    let outlierFlag = false;
    let stats: ColumnProfile['stats'] = undefined;

    if ((dtype === 'integer' || dtype === 'float') && numericVals.length >= 4) {
      const sorted = [...numericVals].sort((a, b) => a - b);
      const min = sorted[0];
      const max = sorted[sorted.length - 1];
      const sum = sorted.reduce((a, b) => a + b, 0);
      const mean = Number((sum / sorted.length).toFixed(2));
      const median = sorted[Math.floor(sorted.length / 2)];

      const q1 = sorted[Math.floor(sorted.length * 0.25)];
      const q3 = sorted[Math.floor(sorted.length * 0.75)];
      const iqr = q3 - q1;
      const lowerFence = q1 - 1.5 * iqr;
      const upperFence = q3 + 1.5 * iqr;

      outlierCount = sorted.filter((v) => v < lowerFence || v > upperFence).length;
      outlierFlag = outlierCount > 0 && outlierCount / sorted.length > 0.05;

      stats = { min, max, mean, median };
    } else {
      // Frequency for categorical
      const freqMap: Record<string, number> = {};
      nonNullValues.forEach((v) => {
        const key = String(v);
        freqMap[key] = (freqMap[key] || 0) + 1;
      });
      const topValues = Object.entries(freqMap)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([value, count]) => ({ value, count }));

      stats = { topValues };
    }

    return {
      name: colName,
      dtype,
      totalCount: totalRows,
      nullCount,
      completeness,
      distinctCount,
      uniqueness,
      formatScore,
      dqScore,
      outlierFlag,
      outlierCount,
      stats,
    };
  });

  const overallDqScore =
    columnProfiles.length > 0
      ? Number(
          (
            columnProfiles.reduce((acc, c) => acc + c.dqScore, 0) /
            columnProfiles.length
          ).toFixed(1),
        )
      : 0;

  return {
    fileName,
    delimiter,
    totalRows,
    totalColumns: headers.length,
    duplicateRows,
    overallDqScore,
    headers,
    columns: columnProfiles,
    previewRows: allRows.slice(0, 50),
    allRows,
  };
}

// Sample 1: release_impact (music streaming catalog)
export const SAMPLE_RELEASE_IMPACT_CSV = `release_year,provider,catalog,streams
2021,Alpha,Pop,120
2022,Beta,Pop,180
2023,Alpha,Rock,210
2024,Gamma,Rock,450
2025,Alpha,Jazz,190`;

// Sample 2: illustrative macro indicators with rounded values, inspired by the World Bank World
// Development Indicators (CC BY 4.0). It is NOT an official extract and must not be cited as one.
export const SAMPLE_GLOBAL_INDICATORS_CSV = `country,year,gdp_trillions,inflation_pct,unemployment_pct
World,2019,87.7,2.3,5.4
World,2020,85.2,3.2,6.6
World,2021,96.5,4.7,6.2
World,2022,100.8,8.7,5.3
World,2023,105.4,5.8,5.1`;
