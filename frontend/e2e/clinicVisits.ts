/**
 * A CSV the app has never seen: built here, so the expected answers are computed independently of the
 * app's own code. 400 clinic visits, ~7% of the costs missing, one value with a quote and a comma.
 */
export const DEPARTMENTS = ['Cardiology', 'Oncology', 'Pediatrics', 'Neurology'];
export const CHANNELS = ['walk-in', 'referral', 'emergency'];
export const visits = Array.from({ length: 400 }, (_, i) => {
  const id = i + 1;
  return {
    id,
    department: DEPARTMENTS[(id * 7) % 4],
    channel: CHANNELS[(id * 5) % 3],
    cost: id % 14 === 0 ? null : Math.round(((id * 37) % 900 + 40 + (id % 100) / 100) * 100) / 100,
    date: `2024-${String((id % 12) + 1).padStart(2, '0')}-${String((id % 28) + 1).padStart(2, '0')}`,
    physician: id % 7 === 0 ? 'Bianchi, "Gigi"' : 'Rossi',
  };
});
export const csvQuote = (v: string) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
export const CSV = [
  'visit_id,department,channel,cost_eur,visit_date,physician',
  ...visits.map((v) => [v.id, v.department, v.channel, v.cost ?? '', v.date, csvQuote(v.physician)].join(',')),
].join('\n');

export const referrals = visits.filter((v) => v.channel === 'referral');
export const sumByDepartment = (rows: typeof visits) =>
  Object.fromEntries(
    DEPARTMENTS.map((d) => [
      d,
      Math.round(rows.filter((v) => v.department === d && v.cost !== null).reduce((s, v) => s + (v.cost as number), 0) * 100) / 100,
    ]),
  );

