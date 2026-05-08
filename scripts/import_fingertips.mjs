// Fingertips National GP Profile importer
// Pulls all indicators from Profile 20 at GP practice area level (area_type=7)
// Filters down to a single practice + its sub-ICB peers if specified

import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const SUB_ICB_CODE = (process.env.SUB_ICB_CODE || "").trim().toUpperCase();
const PROFILE_ID = process.env.PROFILE_ID || "20"; // National General Practice Profiles
const BATCH_SIZE = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing Supabase env vars");

const DATA_DIR = path.resolve("data");
fs.mkdirSync(DATA_DIR, { recursive: true });

const FT_BASE = "https://fingertips.phe.org.uk/api";
const FT_HEADERS = { "User-Agent": "Mozilla/5.0 CheckLoops-Importer/1.0" };

function parseCSVLine(line) {
  const out = [];
  let cur = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (q && line[i + 1] === '"') { cur += '"'; i++; }
      else q = !q;
    } else if (ch === "," && !q) { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur);
  return out;
}

function num(v) {
  if (v == null || v === "" || v === "-") return null;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

async function fetchProfile() {
  const res = await fetch(`${FT_BASE}/profiles`, { headers: FT_HEADERS });
  const data = await res.json();
  return data.find(p => p.Id === Number(PROFILE_ID));
}

async function fetchGroupIndicators(groupId) {
  const res = await fetch(`${FT_BASE}/indicator_metadata/by_group_id?group_ids=${groupId}`, { headers: FT_HEADERS });
  return res.json();
}

async function fetchIndicatorData(indicatorId) {
  // Get all GP-level data for indicator (area_type 7 = GP, parent=England)
  const url = `${FT_BASE}/all_data/csv/by_indicator_id?indicator_ids=${indicatorId}&parent_area_code=E92000001&child_area_type_id=7&parent_area_type_id=15`;
  const res = await fetch(url, { headers: FT_HEADERS });
  if (!res.ok) throw new Error(`Indicator ${indicatorId} fetch failed: ${res.status}`);
  return res.text();
}

async function upsert(rows) {
  if (rows.length === 0) return 0;
  const url = `${SUPABASE_URL}/rest/v1/fingertips_indicators`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Upsert failed (${res.status}): ${body.slice(0, 400)}`);
  }
  return rows.length;
}

async function upsertMetadata(rows) {
  if (rows.length === 0) return 0;
  const url = `${SUPABASE_URL}/rest/v1/fingertips_indicator_metadata?on_conflict=indicator_id`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) console.warn(`Metadata upsert failed (${res.status})`);
  return rows.length;
}

async function processIndicator(indicatorId, indicatorName, groupId, groupName) {
  console.log(`  Fetching indicator ${indicatorId}: ${indicatorName}...`);
  let csv;
  try {
    csv = await fetchIndicatorData(indicatorId);
  } catch (err) {
    console.warn(`    Failed: ${err.message}`);
    return 0;
  }

  const lines = csv.split(/\r?\n/);
  const headers = parseCSVLine(lines[0]);
  const idx = (name) => headers.findIndex(h => h.toLowerCase().trim() === name.toLowerCase());

  const cols = {
    indicator_id: idx("Indicator ID"),
    indicator_name: idx("Indicator Name"),
    parent_code: idx("Parent Code"),
    parent_name: idx("Parent Name"),
    area_code: idx("Area Code"),
    area_name: idx("Area Name"),
    area_type: idx("Area Type"),
    sex: idx("Sex"),
    age: idx("Age"),
    category_type: idx("Category Type"),
    category: idx("Category"),
    time_period: idx("Time period"),
    value: idx("Value"),
    lower_ci_95: idx("Lower CI 95.0 limit"),
    upper_ci_95: idx("Upper CI 95.0 limit"),
    count_value: idx("Count"),
    denominator: idx("Denominator"),
    value_note: idx("Value note"),
    recent_trend: idx("Recent Trend"),
    compared_to_england: idx("Compared to England value or percentiles"),
    time_period_sortable: idx("Time period Sortable"),
  };

  let batch = [];
  let total = 0;
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i]) continue;
    const fields = parseCSVLine(lines[i]);
    const code = fields[cols.area_code];
    if (!code) continue;

    // Filter
    if (PRACTICE_CODE && code !== PRACTICE_CODE) {
      // Also include if sub-ICB matches (parent code = sub-ICB code)
      if (!SUB_ICB_CODE || fields[cols.parent_code] !== SUB_ICB_CODE) continue;
    }

    batch.push({
      area_code: code,
      area_type: 'GP',
      area_name: fields[cols.area_name] || null,
      indicator_id: parseInt(fields[cols.indicator_id], 10),
      indicator_name: fields[cols.indicator_name] || indicatorName,
      group_id: groupId,
      group_name: groupName,
      sex: fields[cols.sex] || null,
      age: fields[cols.age] || null,
      category_type: fields[cols.category_type] || null,
      category: fields[cols.category] || null,
      time_period: fields[cols.time_period] || 'Unknown',
      time_period_sortable: parseInt(fields[cols.time_period_sortable], 10) || null,
      value: num(fields[cols.value]),
      lower_ci_95: num(fields[cols.lower_ci_95]),
      upper_ci_95: num(fields[cols.upper_ci_95]),
      count_value: num(fields[cols.count_value]),
      denominator: num(fields[cols.denominator]),
      value_note: fields[cols.value_note] || null,
      recent_trend: fields[cols.recent_trend] || null,
      compared_to_england: fields[cols.compared_to_england] || null,
      source: `Fingertips Profile ${PROFILE_ID}`,
      source_url: `https://fingertips.phe.org.uk/profile/general-practice/data#page/3/gid/${groupId}/iid/${indicatorId}`,
    });

    if (batch.length >= BATCH_SIZE) {
      total += await upsert(batch);
      batch = [];
    }
  }
  if (batch.length) total += await upsert(batch);
  console.log(`    -> ${total} rows`);
  return total;
}

async function main() {
  console.log(`Fingertips Profile ${PROFILE_ID} importer for practice ${PRACTICE_CODE} (sub-ICB ${SUB_ICB_CODE})`);

  const profile = await fetchProfile();
  if (!profile) throw new Error(`Profile ${PROFILE_ID} not found`);
  console.log(`Profile: ${profile.Name}`);

  const groups = profile.GroupMetadata || [];
  console.log(`Groups: ${groups.length}`);

  let grandTotal = 0;
  const metadataBatch = [];

  for (const g of groups) {
    console.log(`\nGroup ${g.Id}: ${g.Name}`);
    let indicators;
    try {
      indicators = await fetchGroupIndicators(g.Id);
    } catch (err) {
      console.warn(`  Group ${g.Id} indicators fetch failed: ${err.message}`);
      continue;
    }

    for (const [iid, meta] of Object.entries(indicators)) {
      const name = meta?.Descriptive?.Name || `Indicator ${iid}`;
      metadataBatch.push({
        indicator_id: parseInt(iid, 10),
        indicator_name: name,
        group_id: g.Id,
        group_name: g.Name,
        unit: meta?.Unit?.Label || null,
        definition: (meta?.Descriptive?.Definition || '').slice(0, 1000),
        rationale: (meta?.Descriptive?.Rationale || '').slice(0, 1000),
        data_source: (meta?.Descriptive?.DataSource || '').slice(0, 500),
      });

      const n = await processIndicator(parseInt(iid, 10), name, g.Id, g.Name);
      grandTotal += n;
    }
  }

  await upsertMetadata(metadataBatch);
  console.log(`\nDone. Total rows imported: ${grandTotal}`);
}

main().catch((err) => { console.error(err); process.exit(1); });
