import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const QOF_ZIP_URL = process.env.QOF_ZIP_URL || "https://files.digital.nhs.uk/95/4708D7/QOF2425.zip";
const QOF_YEAR = process.env.QOF_YEAR || "2024-25";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}

const DATA_DIR = path.resolve("data");
fs.mkdirSync(DATA_DIR, { recursive: true });
const ZIP_PATH = path.join(DATA_DIR, `qof_${QOF_YEAR}.zip`);
const EXTRACT_DIR = path.join(DATA_DIR, `qof_${QOF_YEAR}`);

function parseCSVLine(line) {
  const fields = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') inQ = !inQ;
    else if (ch === "," && !inQ) { fields.push(cur); cur = ""; }
    else cur += ch;
  }
  fields.push(cur);
  return fields;
}

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function int(v) {
  if (v == null || v === "") return null;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

async function downloadZip() {
  if (fs.existsSync(ZIP_PATH)) {
    console.log(`Using cached ZIP: ${ZIP_PATH}`);
    return;
  }
  console.log(`Downloading QOF ZIP from ${QOF_ZIP_URL}`);
  const res = await fetch(QOF_ZIP_URL);
  if (!res.ok) throw new Error(`QOF ZIP download failed: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(ZIP_PATH, buf);
  console.log(`Saved ${buf.length} bytes to ${ZIP_PATH}`);
}

function extractZip() {
  if (fs.existsSync(EXTRACT_DIR)) {
    console.log(`Using cached extract dir: ${EXTRACT_DIR}`);
    return;
  }
  fs.mkdirSync(EXTRACT_DIR, { recursive: true });
  const r = spawnSync("unzip", ["-o", ZIP_PATH, "-d", EXTRACT_DIR], { stdio: "inherit" });
  if (r.status !== 0) throw new Error(`unzip failed: ${r.status}`);
}

async function upsert(table, rows, conflict) {
  if (rows.length === 0) return 0;
  const url = `${SUPABASE_URL}/rest/v1/${table}?on_conflict=${conflict}`;
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
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Upsert ${table} failed (${res.status}): ${body.slice(0, 500)}`);
  }
  return rows.length;
}

function parseCsvFile(filePath) {
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length === 0) return { headers: [], rows: [] };
  const headers = parseCSVLine(lines[0]).map((h) => h.trim().replace(/^"|"$/g, ""));
  const rows = lines.slice(1).map((l) => parseCSVLine(l));
  return { headers, rows };
}

async function importIndicators() {
  const file = path.join(EXTRACT_DIR, `MAPPING_INDICATORS_${QOF_YEAR.replace("-", "")}.csv`);
  if (!fs.existsSync(file)) throw new Error(`Indicator mapping file not found: ${file}`);
  const { headers, rows } = parseCsvFile(file);
  const idx = (name) => headers.indexOf(name);
  const out = rows.map((f) => ({
    year: QOF_YEAR,
    indicator_code: f[idx("INDICATOR_CODE")] || null,
    indicator_description: f[idx("INDICATOR_DESCRIPTION")] || null,
    indicator_point_value: num(f[idx("INDICATOR_POINT_VALUE")]),
    group_code: f[idx("GROUP_CODE")] || null,
    group_description: f[idx("GROUP_DESCRIPTION")] || null,
    sub_domain_code: f[idx("SUB_DOMAIN_CODE")] || null,
    sub_domain_description: f[idx("SUB_DOMAIN_DESCRIPTION")] || null,
    domain_code: f[idx("DOMAIN_CODE")] || null,
    domain_description: f[idx("DOMAIN_DESCRIPTION")] || null,
    patient_list_type: f[idx("PATIENT_LIST_TYPE")] || null,
    imported_at: new Date().toISOString(),
  })).filter((r) => r.indicator_code);
  // Batch upsert in chunks to avoid request size limits
  let total = 0;
  for (let i = 0; i < out.length; i += 100) {
    total += await upsert("qof_indicators", out.slice(i, i + 100), "year,indicator_code");
  }
  console.log(`Indicators upserted: ${total}`);
  return out;
}

async function importPrevalence(indicatorsByGroup) {
  const file = path.join(EXTRACT_DIR, `PREVALENCE_${QOF_YEAR.replace("-", "")}.csv`);
  if (!fs.existsSync(file)) throw new Error(`Prevalence file not found: ${file}`);
  const { headers, rows } = parseCsvFile(file);
  const idx = (name) => headers.indexOf(name);
  const matched = rows.filter((f) => (f[idx("PRACTICE_CODE")] || "").toUpperCase() === PRACTICE_CODE);
  console.log(`Prevalence rows for ${PRACTICE_CODE}: ${matched.length}`);

  const out = matched.map((f) => {
    const groupCode = f[idx("GROUP_CODE")];
    const register = num(f[idx("REGISTER")]);
    const listSize = int(f[idx("PRACTICE_LIST_SIZE")]);
    const prevalencePercent = (register != null && listSize && listSize > 0)
      ? Math.round((register / listSize) * 100 * 10000) / 10000
      : null;
    return {
      practice_code: PRACTICE_CODE,
      year: QOF_YEAR,
      group_code: groupCode,
      group_description: indicatorsByGroup[groupCode] || null,
      register,
      patient_list_type: f[idx("PATIENT_LIST_TYPE")] || null,
      practice_list_size: listSize,
      prevalence_percent: prevalencePercent,
      source: `NHS Digital QOF ${QOF_YEAR} (PREVALENCE)`,
      imported_at: new Date().toISOString(),
    };
  });
  let total = 0;
  for (let i = 0; i < out.length; i += 100) {
    total += await upsert("gp_practice_qof_prevalence", out.slice(i, i + 100), "practice_code,year,group_code");
  }
  console.log(`Prevalence rows upserted: ${total}`);
}

async function importAchievement() {
  const yr = QOF_YEAR.replace("-", "");
  const regions = [
    "MIDLANDS", "NORTH_EAST_AND_YORKSHIRE", "NORTH_WEST",
    "SOUTH_EAST", "SOUTH_WEST", "EAST_OF_ENGLAND", "LONDON",
  ];

  const allRows = [];
  for (const region of regions) {
    const file = path.join(EXTRACT_DIR, `ACHIEVEMENT_${region}_${yr}.csv`);
    if (!fs.existsSync(file)) {
      console.warn(`Missing region file: ${file}`);
      continue;
    }
    const { headers, rows } = parseCsvFile(file);
    const idx = (name) => headers.indexOf(name);
    const matched = rows.filter((f) => (f[idx("PRACTICE_CODE")] || "").toUpperCase() === PRACTICE_CODE);
    if (matched.length > 0) {
      console.log(`Found ${matched.length} achievement rows in region ${region}`);
      for (const f of matched) {
        allRows.push({
          practice_code: PRACTICE_CODE,
          year: QOF_YEAR,
          indicator_code: f[idx("INDICATOR_CODE")] || null,
          measure: f[idx("MEASURE")] || null,
          value: num(f[idx("VALUE")]),
          region_name: f[idx("REGION_NAME")] || null,
          region_ods_code: f[idx("REGION_ODS_CODE")] || null,
          source: `NHS Digital QOF ${QOF_YEAR} (ACHIEVEMENT_${region})`,
          imported_at: new Date().toISOString(),
        });
      }
    }
  }
  const filtered = allRows.filter((r) => r.indicator_code && r.measure);
  let total = 0;
  for (let i = 0; i < filtered.length; i += 200) {
    total += await upsert(
      "gp_practice_qof_achievement",
      filtered.slice(i, i + 200),
      "practice_code,year,indicator_code,measure"
    );
  }
  console.log(`Achievement rows upserted: ${total}`);
}

async function main() {
  console.log(`QOF importer for practice ${PRACTICE_CODE}, year ${QOF_YEAR}`);
  await downloadZip();
  extractZip();

  const indicators = await importIndicators();
  const groupMap = Object.fromEntries(indicators.map((i) => [i.group_code, i.group_description]));

  await importPrevalence(groupMap);
  await importAchievement();
  console.log("QOF import complete.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
