import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "imd_lsoa_metrics";
const BATCH_SIZE = 2000;

const IMD_YEAR = Number.parseInt((process.env.IMD_YEAR || "2025").trim(), 10);
const PUBLICATION_LABEL = (process.env.PUBLICATION_LABEL || "English Indices of Deprivation 2025").trim();
const IMD_CSV_URL = (process.env.IMD_CSV_URL || "").trim();

const DATA_DIR = path.resolve("data");
const CSV_PATH = path.join(DATA_DIR, "imd_lsoa.csv");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!Number.isFinite(IMD_YEAR)) {
  throw new Error("IMD_YEAR must be a valid integer.");
}
if (!IMD_CSV_URL.startsWith("http")) {
  throw new Error("IMD_CSV_URL must be a valid http/https URL.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

async function downloadCsv() {
  const markerPath = `${CSV_PATH}.url`;
  const existingUrl = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, "utf8").trim() : "";
  if (fs.existsSync(CSV_PATH) && fs.statSync(CSV_PATH).size > 0 && existingUrl === IMD_CSV_URL) {
    const sizeMb = fs.statSync(CSV_PATH).size / 1024 / 1024;
    console.log(`IMD CSV already exists: ${CSV_PATH} (${sizeMb.toFixed(2)} MB)`);
    return;
  }
  console.log(`Downloading IMD CSV from ${IMD_CSV_URL}`);
  const res = await fetch(IMD_CSV_URL);
  if (!res.ok) {
    throw new Error(`Failed to download IMD CSV (${res.status}).`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(CSV_PATH, bytes);
  fs.writeFileSync(markerPath, IMD_CSV_URL);
  const sizeMb = bytes.length / 1024 / 1024;
  console.log(`Saved IMD CSV: ${sizeMb.toFixed(2)} MB`);
}

function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        current += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  out.push(current);
  return out;
}

function maybeNumber(value) {
  const t = (value || "").replaceAll(",", "").trim();
  if (!t) return { numeric: null, text: null };
  const n = Number.parseFloat(t);
  if (Number.isFinite(n)) return { numeric: n, text: null };
  return { numeric: null, text: t };
}

const LSOA_CODE_CANDIDATES = [
  "LSOA_CODE_2021",
  "LSOA_CODE_2011",
  "LSOA_CODE",
  "LSOA11CD",
  "LSOA21CD"
];
const LSOA_NAME_CANDIDATES = [
  "LSOA_NAME_2021",
  "LSOA_NAME_2011",
  "LSOA_NAME",
  "LSOA11NM",
  "LSOA21NM"
];
const LA_CODE_CANDIDATES = [
  "LOCAL_AUTHORITY_DISTRICT_CODE",
  "LAD_CODE",
  "LADCD",
  "LOCAL_AUTHORITY_DISTRICT_CODE_2021",
  "LOCAL_AUTHORITY_DISTRICT_CODE_2019"
];
const LA_NAME_CANDIDATES = [
  "LOCAL_AUTHORITY_DISTRICT_NAME",
  "LAD_NAME",
  "LADNM"
];
const REGION_CODE_CANDIDATES = ["REGION_CODE", "RGN11CD", "RGN21CD"];
const REGION_NAME_CANDIDATES = ["REGION_NAME", "RGN11NM", "RGN21NM"];

function findIndex(headers, candidates) {
  for (const c of candidates) {
    const i = headers.indexOf(c);
    if (i !== -1) return i;
  }
  return -1;
}

function buildHeaderInfo(headerLine) {
  const rawHeaders = parseCsvLine(headerLine.replace(/^﻿/, ""));
  const upperHeaders = rawHeaders.map((h) => h.trim().toUpperCase().replaceAll(" ", "_").replaceAll("-", "_"));

  const idxLsoaCode = findIndex(upperHeaders, LSOA_CODE_CANDIDATES);
  if (idxLsoaCode === -1) {
    throw new Error(`CSV is missing an LSOA code column. Headers: ${upperHeaders.join(",").slice(0, 800)}`);
  }
  const idxLsoaName = findIndex(upperHeaders, LSOA_NAME_CANDIDATES);
  const idxLaCode = findIndex(upperHeaders, LA_CODE_CANDIDATES);
  const idxLaName = findIndex(upperHeaders, LA_NAME_CANDIDATES);
  const idxRegionCode = findIndex(upperHeaders, REGION_CODE_CANDIDATES);
  const idxRegionName = findIndex(upperHeaders, REGION_NAME_CANDIDATES);

  const baseSet = new Set([idxLsoaCode, idxLsoaName, idxLaCode, idxLaName, idxRegionCode, idxRegionName].filter((i) => i !== -1));
  const metricColumns = upperHeaders
    .map((h, i) => ({ key: h, index: i }))
    .filter((x) => !baseSet.has(x.index) && x.key);

  return {
    rawHeaders,
    upperHeaders,
    idxLsoaCode,
    idxLsoaName,
    idxLaCode,
    idxLaName,
    idxRegionCode,
    idxRegionName,
    metricColumns
  };
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const q = new URLSearchParams({ on_conflict: "lsoa_code,imd_year,metric_key" });
  const url = `${SUPABASE_URL}/rest/v1/${TABLE_NAME}?${q.toString()}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify(rows)
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Supabase upsert failed (${res.status}): ${body}`);
  }
}

async function importRows(csvPath) {
  const nowIso = new Date().toISOString();
  const sourceCsvName = path.basename(new URL(IMD_CSV_URL).pathname) || "imd_lsoa.csv";
  let header = null;
  let total = 0;
  let imported = 0;
  let lsoaCount = 0;
  const batch = [];

  const stream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = buildHeaderInfo(line);
      console.log(`Detected ${header.metricColumns.length} metric column(s) per LSOA.`);
      continue;
    }
    total += 1;
    const cols = parseCsvLine(line);
    const lsoaCode = (cols[header.idxLsoaCode] || "").trim();
    if (!lsoaCode) continue;
    lsoaCount += 1;
    const lsoaName = header.idxLsoaName >= 0 ? (cols[header.idxLsoaName] || "").trim() || null : null;
    const laCode = header.idxLaCode >= 0 ? (cols[header.idxLaCode] || "").trim() || null : null;
    const laName = header.idxLaName >= 0 ? (cols[header.idxLaName] || "").trim() || null : null;
    const regionCode = header.idxRegionCode >= 0 ? (cols[header.idxRegionCode] || "").trim() || null : null;
    const regionName = header.idxRegionName >= 0 ? (cols[header.idxRegionName] || "").trim() || null : null;

    for (const metric of header.metricColumns) {
      const parsed = maybeNumber(cols[metric.index] || "");
      batch.push({
        lsoa_code: lsoaCode,
        lsoa_name: lsoaName,
        local_authority_code: laCode,
        local_authority_name: laName,
        region_code: regionCode,
        region_name: regionName,
        imd_year: IMD_YEAR,
        publication_label: PUBLICATION_LABEL || null,
        metric_key: metric.key,
        metric_value: parsed.numeric,
        metric_value_text: parsed.text,
        source_csv_url: IMD_CSV_URL,
        source_csv_name: sourceCsvName,
        imported_at: nowIso
      });
      if (batch.length >= BATCH_SIZE) {
        await upsertBatch(batch);
        imported += batch.length;
        batch.length = 0;
      }
    }
  }
  if (batch.length) {
    await upsertBatch(batch);
    imported += batch.length;
  }
  return { total, lsoaCount, imported };
}

async function main() {
  console.log(`IMD year: ${IMD_YEAR}`);
  console.log(`Publication label: ${PUBLICATION_LABEL}`);
  console.log(`IMD CSV URL: ${IMD_CSV_URL}`);

  await downloadCsv();
  const result = await importRows(CSV_PATH);
  console.log(
    `Done. CSV rows scanned=${result.total}, distinct LSOAs=${result.lsoaCount}, upserted metric rows=${result.imported}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
