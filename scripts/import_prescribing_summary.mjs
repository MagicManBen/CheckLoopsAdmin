// Streams a monthly NHSBSA EPD CSV (~1.5 GB), filters to a single practice,
// and aggregates ITEMS / NIC / ACTUAL_COST / QUANTITY by BNF chapter.
// Inserts rows into gp_practice_prescribing_summary.
//
// EPD CSV columns we use:
//   YEAR_MONTH, PRACTICE_NAME, PRACTICE_CODE, BNF_CHAPTER_PLUS_CODE,
//   QUANTITY, ITEMS, NIC, ACTUAL_COST
//
// Required env:
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, PRACTICE_CODE, PRESCRIBING_CSV_URL
// Optional env:
//   YEAR_MONTH (YYYY-MM-01) — derived from CSV first row if blank
//   PUBLICATION_LABEL — free-text label
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "gp_practice_prescribing_summary";
const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const PRESCRIBING_CSV_URL = (process.env.PRESCRIBING_CSV_URL || "").trim();
const YEAR_MONTH_INPUT = (process.env.YEAR_MONTH || "").trim();
const PUBLICATION_LABEL = (process.env.PUBLICATION_LABEL || "NHSBSA English Prescribing Dataset").trim();

const DATA_DIR = path.resolve("data");
const CSV_PATH = path.join(DATA_DIR, "epd_monthly.csv");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) throw new Error("Missing PRACTICE_CODE.");
if (!PRESCRIBING_CSV_URL.startsWith("http")) {
  throw new Error("PRESCRIBING_CSV_URL must be a valid http/https URL.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

async function downloadCsv() {
  const markerPath = `${CSV_PATH}.url`;
  const existingUrl = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, "utf8").trim() : "";
  if (fs.existsSync(CSV_PATH) && fs.statSync(CSV_PATH).size > 0 && existingUrl === PRESCRIBING_CSV_URL) {
    const sizeMb = fs.statSync(CSV_PATH).size / 1024 / 1024;
    console.log(`EPD CSV cache hit: ${CSV_PATH} (${sizeMb.toFixed(1)} MB)`);
    return;
  }
  console.log(`Downloading EPD CSV from ${PRESCRIBING_CSV_URL}`);
  const res = await fetch(PRESCRIBING_CSV_URL);
  if (!res.ok || !res.body) {
    throw new Error(`Failed to download EPD CSV (${res.status}).`);
  }
  const out = fs.createWriteStream(CSV_PATH);
  let received = 0;
  for await (const chunk of res.body) {
    received += chunk.length;
    out.write(chunk);
  }
  out.end();
  await new Promise((r) => out.on("finish", r));
  fs.writeFileSync(markerPath, PRESCRIBING_CSV_URL);
  console.log(`Saved EPD CSV: ${(received / 1024 / 1024).toFixed(1)} MB`);
}

function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
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

function num(v) {
  const t = (v || "").replaceAll(",", "").replaceAll("£", "").trim();
  if (!t) return 0;
  const n = Number.parseFloat(t);
  return Number.isFinite(n) ? n : 0;
}

function chapterCodeFromCombined(combined) {
  // "04: Central Nervous System" -> { code: "04", name: "Central Nervous System" }
  const m = /^(\d{2})\s*:\s*(.*)$/.exec((combined || "").trim());
  if (m) return { code: m[1], name: m[2].trim() || null };
  return { code: (combined || "OTHER").slice(0, 8) || "OTHER", name: combined || null };
}

function yearMonthFromYM(ym) {
  // 202506 -> 2025-06-01
  const m = /^(\d{4})(\d{2})$/.exec((ym || "").trim());
  if (!m) return null;
  return `${m[1]}-${m[2]}-01`;
}

async function streamAndAggregate(csvPath) {
  const stream = fs.createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let header = null;
  let idx = {};
  let total = 0;
  let matched = 0;
  let practiceName = null;
  let derivedYearMonth = null;

  // chapterCode -> { name, items, quantity, nic, actual_cost, line_count }
  const agg = new Map();

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = parseCsvLine(line.replace(/^﻿/, ""));
      const upper = header.map((h) => h.trim().toUpperCase().replaceAll(" ", "_"));
      idx = {
        yearMonth: upper.indexOf("YEAR_MONTH"),
        practiceCode: upper.indexOf("PRACTICE_CODE"),
        practiceName: upper.indexOf("PRACTICE_NAME"),
        bnfChapter: upper.indexOf("BNF_CHAPTER_PLUS_CODE"),
        items: upper.indexOf("ITEMS"),
        quantity: upper.indexOf("QUANTITY"),
        nic: upper.indexOf("NIC"),
        actualCost: upper.indexOf("ACTUAL_COST")
      };
      const missing = Object.entries(idx).filter(([, v]) => v < 0).map(([k]) => k);
      if (missing.length) throw new Error(`EPD CSV missing columns: ${missing.join(",")}`);
      console.log(`EPD header parsed; aggregating practice=${PRACTICE_CODE}`);
      continue;
    }
    total += 1;
    const cols = parseCsvLine(line);
    const code = (cols[idx.practiceCode] || "").trim().toUpperCase();
    if (code !== PRACTICE_CODE) continue;
    matched += 1;
    if (!practiceName) practiceName = (cols[idx.practiceName] || "").trim() || null;
    if (!derivedYearMonth) derivedYearMonth = yearMonthFromYM(cols[idx.yearMonth] || "");

    const { code: chapterCode, name: chapterName } = chapterCodeFromCombined(cols[idx.bnfChapter] || "");
    let row = agg.get(chapterCode);
    if (!row) {
      row = { chapterName, items: 0, quantity: 0, nic: 0, actual_cost: 0, lines: 0 };
      agg.set(chapterCode, row);
    }
    if (chapterName && !row.chapterName) row.chapterName = chapterName;
    row.items += num(cols[idx.items]);
    row.quantity += num(cols[idx.quantity]);
    row.nic += num(cols[idx.nic]);
    row.actual_cost += num(cols[idx.actualCost]);
    row.lines += 1;
  }

  return { total, matched, practiceName, derivedYearMonth, agg };
}

async function upsertRows(rows) {
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

function buildRows(agg, yearMonth, practiceName, sourceCsvName) {
  const nowIso = new Date().toISOString();
  const rows = [];
  // Per-chapter rows
  let totalItems = 0, totalQuantity = 0, totalNic = 0, totalActualCost = 0, totalLines = 0;
  for (const [chapter, v] of agg.entries()) {
    totalItems += v.items;
    totalQuantity += v.quantity;
    totalNic += v.nic;
    totalActualCost += v.actual_cost;
    totalLines += v.lines;
    const base = {
      practice_code: PRACTICE_CODE,
      practice_name: practiceName,
      year_month: yearMonth,
      publication_label: PUBLICATION_LABEL,
      bnf_chapter: chapter,
      bnf_chapter_name: v.chapterName,
      source_csv_url: PRESCRIBING_CSV_URL,
      source_csv_name: sourceCsvName,
      imported_at: nowIso
    };
    rows.push({ ...base, metric_key: "ITEMS", metric_value: v.items, metric_value_text: null });
    rows.push({ ...base, metric_key: "QUANTITY", metric_value: v.quantity, metric_value_text: null });
    rows.push({ ...base, metric_key: "NIC_GBP", metric_value: Number(v.nic.toFixed(2)), metric_value_text: null });
    rows.push({ ...base, metric_key: "ACTUAL_COST_GBP", metric_value: Number(v.actual_cost.toFixed(2)), metric_value_text: null });
    rows.push({ ...base, metric_key: "PRESCRIPTION_LINES", metric_value: v.lines, metric_value_text: null });
  }
  // ALL chapter totals
  const baseAll = {
    practice_code: PRACTICE_CODE,
    practice_name: practiceName,
    year_month: yearMonth,
    publication_label: PUBLICATION_LABEL,
    bnf_chapter: "ALL",
    bnf_chapter_name: "All chapters",
    source_csv_url: PRESCRIBING_CSV_URL,
    source_csv_name: sourceCsvName,
    imported_at: nowIso
  };
  rows.push({ ...baseAll, metric_key: "ITEMS", metric_value: totalItems, metric_value_text: null });
  rows.push({ ...baseAll, metric_key: "QUANTITY", metric_value: totalQuantity, metric_value_text: null });
  rows.push({ ...baseAll, metric_key: "NIC_GBP", metric_value: Number(totalNic.toFixed(2)), metric_value_text: null });
  rows.push({ ...baseAll, metric_key: "ACTUAL_COST_GBP", metric_value: Number(totalActualCost.toFixed(2)), metric_value_text: null });
  rows.push({ ...baseAll, metric_key: "PRESCRIPTION_LINES", metric_value: totalLines, metric_value_text: null });
  return rows;
}

async function main() {
  console.log(`Target practice: ${PRACTICE_CODE}`);
  console.log(`Prescribing CSV URL: ${PRESCRIBING_CSV_URL}`);

  await downloadCsv();
  const { total, matched, practiceName, derivedYearMonth, agg } = await streamAndAggregate(CSV_PATH);
  const yearMonth = YEAR_MONTH_INPUT || derivedYearMonth;
  if (!yearMonth) throw new Error("Could not derive YEAR_MONTH from CSV.");
  if (matched === 0) {
    console.log(`No rows matched practice ${PRACTICE_CODE}. CSV total rows scanned=${total}.`);
    return;
  }
  const sourceCsvName = path.basename(new URL(PRESCRIBING_CSV_URL).pathname) || "epd_monthly.csv";
  const rows = buildRows(agg, yearMonth, practiceName, sourceCsvName);
  console.log(
    `Aggregated ${matched} rows -> ${agg.size} chapters; upserting ${rows.length} metric rows for ${yearMonth}.`
  );

  // Send in chunks to be safe
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    await upsertRows(rows.slice(i, i + chunkSize));
  }
  console.log(`Done. CSV rows scanned=${total}, matched=${matched}, chapters=${agg.size}, metric rows=${rows.length}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
