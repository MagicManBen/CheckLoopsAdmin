import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { execFileSync } from "node:child_process";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "gp_practice_workforce_metrics";
const BATCH_SIZE = 1000;

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "M83076").trim().toUpperCase();
const SNAPSHOT_DATE = (process.env.SNAPSHOT_DATE || "2026-03-31").trim();
const PUBLICATION_LABEL = (process.env.PUBLICATION_LABEL || "General Practice Workforce 31 March 2026").trim();
const DEFAULT_PRACTICE_ZIP_URL = "https://files.digital.nhs.uk/85/7F0D90/GPWPracticeCSV.032026.zip";
const PRACTICE_ZIP_URL = (process.env.PRACTICE_ZIP_URL || DEFAULT_PRACTICE_ZIP_URL).trim();

const DATA_DIR = path.resolve("data");
const ZIP_PATH = path.join(DATA_DIR, "gp-workforce-practice.zip");
const EXTRACT_DIR = path.join(DATA_DIR, "workforce");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!SNAPSHOT_DATE) {
  throw new Error("Missing SNAPSHOT_DATE.");
}
if (!PRACTICE_ZIP_URL.startsWith("http")) {
  throw new Error("PRACTICE_ZIP_URL must be a valid http/https URL.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(EXTRACT_DIR, { recursive: true });

async function downloadZip() {
  const markerPath = `${ZIP_PATH}.url`;
  const existingUrl = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, "utf8").trim() : "";
  if (fs.existsSync(ZIP_PATH) && fs.statSync(ZIP_PATH).size > 0 && existingUrl === PRACTICE_ZIP_URL) {
    const sizeMb = fs.statSync(ZIP_PATH).size / 1024 / 1024;
    console.log(`Workforce ZIP already exists: ${ZIP_PATH} (${sizeMb.toFixed(2)} MB)`);
    return;
  }
  console.log(`Downloading workforce practice ZIP from ${PRACTICE_ZIP_URL}`);
  const res = await fetch(PRACTICE_ZIP_URL);
  if (!res.ok) {
    throw new Error(`Failed to download practice ZIP (${res.status}).`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(ZIP_PATH, bytes);
  fs.writeFileSync(markerPath, PRACTICE_ZIP_URL);
  const sizeMb = bytes.length / 1024 / 1024;
  console.log(`Saved practice ZIP: ${sizeMb.toFixed(2)} MB`);
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

function buildIndexMap(headerLine) {
  const headers = parseCsvLine(headerLine).map((h) => h.trim().toUpperCase());
  const required = [
    "PRAC_CODE",
    "PRAC_NAME",
    "STAFF_GROUP",
    "DETAILED_STAFF_ROLE",
    "MEASURE",
    "VALUE"
  ];
  const idx = {};
  for (const col of required) {
    const i = headers.indexOf(col);
    if (i === -1) {
      throw new Error(`CSV is missing expected column: ${col}`);
    }
    idx[col] = i;
  }
  return idx;
}

function selectHighLevelCsvEntry(zipPath) {
  const raw = execFileSync("unzip", ["-Z1", zipPath], { encoding: "utf8" });
  const entries = raw.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const match = entries.find((name) => /\.csv$/i.test(name) && /high level/i.test(name));
  if (!match) {
    throw new Error(`Could not find high-level practice CSV in ZIP. Entries: ${entries.join(" | ")}`);
  }
  return match;
}

function extractCsv(zipPath, zipEntry) {
  execFileSync("unzip", ["-o", zipPath, zipEntry, "-d", EXTRACT_DIR], { stdio: "inherit" });
  return path.join(EXTRACT_DIR, zipEntry);
}

function cleanNumber(value) {
  const t = (value || "").replaceAll(",", "").trim();
  if (!t) return 0;
  const n = Number.parseFloat(t);
  if (!Number.isFinite(n)) {
    throw new Error(`Invalid numeric value encountered: ${value}`);
  }
  return n;
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const q = new URLSearchParams({
    on_conflict: "practice_code,snapshot_date,staff_group,detailed_staff_role,measure"
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

async function importWorkforceRows(csvPath, sourceCsvName) {
  const nowIso = new Date().toISOString();
  const batch = [];
  let imported = 0;
  let matched = 0;
  let skipped = 0;
  let total = 0;
  let idx = null;

  const stream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line) continue;
    if (!idx) {
      idx = buildIndexMap(line.replace(/^\uFEFF/, ""));
      continue;
    }
    total += 1;
    const cols = parseCsvLine(line);
    const practiceCode = (cols[idx.PRAC_CODE] || "").trim().toUpperCase();
    if (practiceCode !== PRACTICE_CODE) {
      skipped += 1;
      continue;
    }
    matched += 1;
    batch.push({
      practice_code: practiceCode,
      practice_name: (cols[idx.PRAC_NAME] || "").trim() || null,
      snapshot_date: SNAPSHOT_DATE,
      publication_label: PUBLICATION_LABEL || null,
      staff_group: (cols[idx.STAFF_GROUP] || "").trim() || null,
      detailed_staff_role: (cols[idx.DETAILED_STAFF_ROLE] || "").trim() || null,
      measure: (cols[idx.MEASURE] || "").trim() || null,
      value: cleanNumber(cols[idx.VALUE]),
      source_practice_zip_url: PRACTICE_ZIP_URL,
      source_csv_name: sourceCsvName,
      imported_at: nowIso
    });
    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(batch);
      imported += batch.length;
      batch.length = 0;
    }
  }
  if (batch.length) {
    await upsertBatch(batch);
    imported += batch.length;
  }

  return { total, matched, skipped, imported };
}

async function main() {
  console.log(`Target practice: ${PRACTICE_CODE}`);
  console.log(`Snapshot date: ${SNAPSHOT_DATE}`);
  console.log(`Publication label: ${PUBLICATION_LABEL}`);
  console.log(`Practice ZIP URL: ${PRACTICE_ZIP_URL}`);

  await downloadZip();
  const csvEntry = selectHighLevelCsvEntry(ZIP_PATH);
  console.log(`High-level CSV entry: ${csvEntry}`);
  const csvPath = extractCsv(ZIP_PATH, csvEntry);
  console.log(`Extracted CSV path: ${csvPath}`);

  const result = await importWorkforceRows(csvPath, path.basename(csvEntry));
  console.log(
    `Done. CSV rows scanned=${result.total}, matched practice rows=${result.matched}, skipped=${result.skipped}, upserted=${result.imported}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
