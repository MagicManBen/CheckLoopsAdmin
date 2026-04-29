import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "gp_practice_prescribing_summary";
const BATCH_SIZE = 1000;

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "M83076").trim().toUpperCase();
const YEAR_MONTH = (process.env.YEAR_MONTH || "").trim();
const PUBLICATION_LABEL = (process.env.PUBLICATION_LABEL || "").trim();
const PRESCRIBING_CSV_URL = (process.env.PRESCRIBING_CSV_URL || "").trim();

const DATA_DIR = path.resolve("data");
const CSV_PATH = path.join(DATA_DIR, "prescribing_summary.csv");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!YEAR_MONTH || !/^\d{4}-\d{2}-01$/.test(YEAR_MONTH)) {
  throw new Error("YEAR_MONTH must be in format YYYY-MM-01 (first day of the publication month).");
}
if (!PRESCRIBING_CSV_URL.startsWith("http")) {
  throw new Error("PRESCRIBING_CSV_URL must be a valid http/https URL.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

async function downloadCsv() {
  const markerPath = `${CSV_PATH}.url`;
  const existingUrl = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, "utf8").trim() : "";
  if (fs.existsSync(CSV_PATH) && fs.statSync(CSV_PATH).size > 0 && existingUrl === PRESCRIBING_CSV_URL) {
    const sizeMb = fs.statSync(CSV_PATH).size / 1024 / 1024;
    console.log(`Prescribing CSV already exists: ${CSV_PATH} (${sizeMb.toFixed(2)} MB)`);
    return;
  }
  console.log(`Downloading prescribing CSV from ${PRESCRIBING_CSV_URL}`);
  const res = await fetch(PRESCRIBING_CSV_URL);
  if (!res.ok) {
    throw new Error(`Failed to download prescribing CSV (${res.status}).`);
  }
  const bytes = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(CSV_PATH, bytes);
  fs.writeFileSync(markerPath, PRESCRIBING_CSV_URL);
  const sizeMb = bytes.length / 1024 / 1024;
  console.log(`Saved prescribing CSV: ${sizeMb.toFixed(2)} MB`);
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
  const t = (value || "").replaceAll(",", "").replaceAll("£", "").trim();
  if (!t) return { numeric: null, text: null };
  const n = Number.parseFloat(t);
  if (Number.isFinite(n)) return { numeric: n, text: null };
  return { numeric: null, text: t };
}

const PRACTICE_KEY_CANDIDATES = [
  "PRACTICE_CODE",
  "PRACTICE",
  "PRACTICECODE",
  "PRAC_CODE",
  "PRAC",
  "ODS",
  "ODSCODE",
  "ODS_CODE",
  "REGISTERED_PRACTICE_ODS_CODE"
];
const PRACTICE_NAME_CANDIDATES = [
  "PRACTICE_NAME",
  "PRACTICENAME",
  "PRAC_NAME",
  "REGISTERED_PRACTICE_NAME"
];
const BNF_CHAPTER_CODE_CANDIDATES = ["BNF_CHAPTER", "BNF_CHAPTER_CODE", "BNFCHAPTER", "CHAPTER", "CHAPTER_CODE"];
const BNF_CHAPTER_NAME_CANDIDATES = ["BNF_CHAPTER_NAME", "CHAPTER_NAME"];

function findIndex(headers, candidates) {
  for (const c of candidates) {
    const i = headers.indexOf(c);
    if (i !== -1) return i;
  }
  return -1;
}

function buildHeaderInfo(headerLine) {
  const rawHeaders = parseCsvLine(headerLine.replace(/^﻿/, ""));
  const upperHeaders = rawHeaders.map((h) => h.trim().toUpperCase().replaceAll(" ", "_"));
  const idxPractice = findIndex(upperHeaders, PRACTICE_KEY_CANDIDATES);
  if (idxPractice === -1) {
    throw new Error(`CSV is missing a practice code column. Headers: ${upperHeaders.join(",").slice(0, 500)}`);
  }
  const idxPracticeName = findIndex(upperHeaders, PRACTICE_NAME_CANDIDATES);
  const idxBnfChapter = findIndex(upperHeaders, BNF_CHAPTER_CODE_CANDIDATES);
  const idxBnfChapterName = findIndex(upperHeaders, BNF_CHAPTER_NAME_CANDIDATES);

  const baseIndices = new Set([idxPractice, idxPracticeName, idxBnfChapter, idxBnfChapterName].filter((i) => i !== -1));
  const metricColumns = upperHeaders
    .map((h, i) => ({ key: h, index: i }))
    .filter((x) => !baseIndices.has(x.index) && x.key && !/^(YEAR_MONTH|YEARMONTH|MONTH|YEAR)$/.test(x.key));

  return {
    rawHeaders,
    upperHeaders,
    idxPractice,
    idxPracticeName,
    idxBnfChapter,
    idxBnfChapterName,
    metricColumns
  };
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const q = new URLSearchParams({
    on_conflict: "practice_code,year_month,bnf_chapter,metric_key"
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

async function importRows(csvPath) {
  const nowIso = new Date().toISOString();
  const sourceCsvName = path.basename(new URL(PRESCRIBING_CSV_URL).pathname) || "prescribing_summary.csv";
  let header = null;
  let total = 0;
  let matched = 0;
  let skipped = 0;
  let imported = 0;
  const batch = [];

  const stream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = buildHeaderInfo(line);
      console.log(`Detected ${header.metricColumns.length} metric column(s).`);
      continue;
    }
    total += 1;
    const cols = parseCsvLine(line);
    const practiceCode = (cols[header.idxPractice] || "").trim().toUpperCase();
    if (practiceCode !== PRACTICE_CODE) {
      skipped += 1;
      continue;
    }
    matched += 1;
    const practiceName = header.idxPracticeName >= 0 ? (cols[header.idxPracticeName] || "").trim() || null : null;
    const bnfChapterCode = header.idxBnfChapter >= 0 ? (cols[header.idxBnfChapter] || "").trim() || "ALL" : "ALL";
    const bnfChapterName = header.idxBnfChapterName >= 0 ? (cols[header.idxBnfChapterName] || "").trim() || null : null;

    for (const metric of header.metricColumns) {
      const parsed = maybeNumber(cols[metric.index] || "");
      batch.push({
        practice_code: practiceCode,
        practice_name: practiceName,
        year_month: YEAR_MONTH,
        publication_label: PUBLICATION_LABEL || null,
        bnf_chapter: bnfChapterCode,
        bnf_chapter_name: bnfChapterName,
        metric_key: metric.key,
        metric_value: parsed.numeric,
        metric_value_text: parsed.text,
        source_csv_url: PRESCRIBING_CSV_URL,
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
  return { total, matched, skipped, imported };
}

async function main() {
  console.log(`Target practice: ${PRACTICE_CODE}`);
  console.log(`Year-month: ${YEAR_MONTH}`);
  console.log(`Publication label: ${PUBLICATION_LABEL}`);
  console.log(`Prescribing CSV URL: ${PRESCRIBING_CSV_URL}`);

  await downloadCsv();
  const result = await importRows(CSV_PATH);
  console.log(
    `Done. CSV rows scanned=${result.total}, matched=${result.matched}, skipped=${result.skipped}, upserted=${result.imported}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
