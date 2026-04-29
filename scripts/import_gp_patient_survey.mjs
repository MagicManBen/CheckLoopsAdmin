import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "gp_practice_patient_survey_metrics";
const BATCH_SIZE = 2000;

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "M83076").trim().toUpperCase();
const SURVEY_YEAR = Number.parseInt((process.env.SURVEY_YEAR || "2025").trim(), 10);
const PUBLICATION_LABEL = (process.env.PUBLICATION_LABEL || "GP Patient Survey 2025 Practice Data (weighted)").trim();
const DEFAULT_SURVEY_CSV_URL = "https://gp-patient.co.uk/FileDownload/Download?fileRedirect=2025%2Fsurvey-results%2Fpractice-results%2Fpractice-data-csv%2FGPPS_2025_Practice_data_(weighted)_(csv)_PUBLIC.csv";
const SURVEY_CSV_URL = (process.env.SURVEY_CSV_URL || DEFAULT_SURVEY_CSV_URL).trim();

const DATA_DIR = path.resolve("data");
const CSV_PATH = path.join(DATA_DIR, "gpps_practice_weighted.csv");

const REQUIRED_BASE_COLUMNS = [
  "ad_practicecode",
  "ad_practicename",
  "ad_icscode",
  "ad_icsname",
  "ad_icscodeons",
  "ad_commissioningregioncode",
  "ad_commissioningregionname",
  "ad_pcncode",
  "ad_pcnname",
  "distributed",
  "received",
  "resprate"
];

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!Number.isFinite(SURVEY_YEAR)) {
  throw new Error("SURVEY_YEAR must be a valid number.");
}
if (!SURVEY_CSV_URL.startsWith("http")) {
  throw new Error("SURVEY_CSV_URL must be a valid http/https URL.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

async function downloadCsv() {
  const markerPath = `${CSV_PATH}.url`;
  const existingUrl = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, "utf8").trim() : "";
  if (fs.existsSync(CSV_PATH) && fs.statSync(CSV_PATH).size > 0 && existingUrl === SURVEY_CSV_URL) {
    const sizeMb = fs.statSync(CSV_PATH).size / 1024 / 1024;
    console.log(`Survey CSV already exists: ${CSV_PATH} (${sizeMb.toFixed(2)} MB)`);
    return;
  }
  console.log(`Downloading GP Patient Survey CSV from ${SURVEY_CSV_URL}`);
  const res = await fetch(SURVEY_CSV_URL);
  if (!res.ok) {
    throw new Error(`Failed to download GP Patient Survey CSV (${res.status}).`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(CSV_PATH, bytes);
  fs.writeFileSync(markerPath, SURVEY_CSV_URL);
  const sizeMb = bytes.length / 1024 / 1024;
  console.log(`Saved survey CSV: ${sizeMb.toFixed(2)} MB`);
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
  if (!t) {
    return { numeric: null, text: null };
  }
  const n = Number.parseFloat(t);
  if (Number.isFinite(n)) {
    return { numeric: n, text: null };
  }
  return { numeric: null, text: t };
}

function sourceCsvNameFromUrl(url) {
  try {
    const parsed = new URL(url);
    const redirected = parsed.searchParams.get("fileRedirect");
    if (redirected) {
      const decoded = decodeURIComponent(redirected);
      const parts = decoded.split("/");
      return parts[parts.length - 1] || "gpps_practice_weighted.csv";
    }
    const pathParts = parsed.pathname.split("/");
    return pathParts[pathParts.length - 1] || "gpps_practice_weighted.csv";
  } catch {
    return "gpps_practice_weighted.csv";
  }
}

function buildHeaderInfo(headerLine) {
  const headers = parseCsvLine(headerLine.replace(/^\uFEFF/, ""));
  const idx = {};
  headers.forEach((h, i) => {
    idx[h] = i;
  });

  for (const col of REQUIRED_BASE_COLUMNS) {
    if (idx[col] === undefined) {
      throw new Error(`CSV missing expected column: ${col}`);
    }
  }

  const metricHeaders = headers.filter((h) => !REQUIRED_BASE_COLUMNS.includes(h));
  return { headers, idx, metricHeaders };
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const q = new URLSearchParams({
    on_conflict: "practice_code,survey_year,metric_key"
  });
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

async function importSurveyRows(csvPath) {
  const nowIso = new Date().toISOString();
  const sourceCsvName = sourceCsvNameFromUrl(SURVEY_CSV_URL);
  const batch = [];
  let totalRows = 0;
  let matchedPracticeRows = 0;
  let skippedRows = 0;
  let importedMetricRows = 0;
  let headerInfo = null;

  const stream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line) continue;
    if (!headerInfo) {
      headerInfo = buildHeaderInfo(line);
      continue;
    }
    totalRows += 1;
    const cols = parseCsvLine(line);
    const practiceCode = (cols[headerInfo.idx.ad_practicecode] || "").trim().toUpperCase();
    if (practiceCode !== PRACTICE_CODE) {
      skippedRows += 1;
      continue;
    }
    matchedPracticeRows += 1;
    const practiceName = (cols[headerInfo.idx.ad_practicename] || "").trim() || null;
    const icsCode = (cols[headerInfo.idx.ad_icscode] || "").trim() || null;
    const icsName = (cols[headerInfo.idx.ad_icsname] || "").trim() || null;
    const icsCodeOns = (cols[headerInfo.idx.ad_icscodeons] || "").trim() || null;
    const commissioningRegionCode = (cols[headerInfo.idx.ad_commissioningregioncode] || "").trim() || null;
    const commissioningRegionName = (cols[headerInfo.idx.ad_commissioningregionname] || "").trim() || null;
    const pcnCode = (cols[headerInfo.idx.ad_pcncode] || "").trim() || null;
    const pcnName = (cols[headerInfo.idx.ad_pcnname] || "").trim() || null;
    const distributed = maybeNumber(cols[headerInfo.idx.distributed] || "").numeric;
    const received = maybeNumber(cols[headerInfo.idx.received] || "").numeric;
    const responseRate = maybeNumber(cols[headerInfo.idx.resprate] || "").numeric;

    for (const metricKey of headerInfo.metricHeaders) {
      const rawVal = cols[headerInfo.idx[metricKey]] || "";
      const parsed = maybeNumber(rawVal);
      batch.push({
        practice_code: practiceCode,
        practice_name: practiceName,
        survey_year: SURVEY_YEAR,
        publication_label: PUBLICATION_LABEL || null,
        ics_code: icsCode,
        ics_name: icsName,
        ics_code_ons: icsCodeOns,
        commissioning_region_code: commissioningRegionCode,
        commissioning_region_name: commissioningRegionName,
        pcn_code: pcnCode,
        pcn_name: pcnName,
        distributed,
        received,
        response_rate: responseRate,
        metric_key: metricKey,
        metric_value: parsed.numeric,
        metric_value_text: parsed.text,
        source_csv_url: SURVEY_CSV_URL,
        source_csv_name: sourceCsvName,
        imported_at: nowIso
      });
      if (batch.length >= BATCH_SIZE) {
        await upsertBatch(batch);
        importedMetricRows += batch.length;
        batch.length = 0;
      }
    }
  }
  if (batch.length) {
    await upsertBatch(batch);
    importedMetricRows += batch.length;
  }

  return {
    totalRows,
    matchedPracticeRows,
    skippedRows,
    importedMetricRows
  };
}

async function main() {
  console.log(`Target practice: ${PRACTICE_CODE}`);
  console.log(`Survey year: ${SURVEY_YEAR}`);
  console.log(`Publication label: ${PUBLICATION_LABEL}`);
  console.log(`Survey CSV URL: ${SURVEY_CSV_URL}`);

  await downloadCsv();
  const result = await importSurveyRows(CSV_PATH);
  console.log(
    `Done. Practice rows scanned=${result.totalRows}, matched practice rows=${result.matchedPracticeRows}, skipped rows=${result.skippedRows}, imported metric rows=${result.importedMetricRows}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
