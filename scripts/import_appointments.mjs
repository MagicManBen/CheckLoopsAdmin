import fs from "node:fs";
import path from "node:path";
import { createReadStream } from "node:fs";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const SUB_ICB_CODE = (process.env.SUB_ICB_CODE || "").trim().toUpperCase();   // optional - filter by Sub-ICB for peer practices
const APPT_ZIP_URL = process.env.APPT_ZIP_URL || "https://files.digital.nhs.uk/BC/A65BD0/Practice_Level_Crosstab_Feb_26.zip";
const TABLE = "gp_appointments";
const BATCH_SIZE = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing Supabase env vars");

const DATA_DIR = path.resolve("data");
fs.mkdirSync(DATA_DIR, { recursive: true });

const monthMap = { JAN:0, FEB:1, MAR:2, APR:3, MAY:4, JUN:5, JUL:6, AUG:7, SEP:8, OCT:9, NOV:10, DEC:11 };
function parseDate(s) {
  // 01FEB2026
  const m = s.match(/^(\d{2})([A-Z]{3})(\d{4})$/);
  if (!m) return null;
  const [, dd, mm, yyyy] = m;
  return `${yyyy}-${String((monthMap[mm] ?? 0) + 1).padStart(2, '0')}-${dd}`;
}

function parseCSVLine(line) {
  const out = [];
  let cur = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') q = !q;
    else if (ch === "," && !q) { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur);
  return out;
}

async function downloadZip() {
  const zipPath = path.join(DATA_DIR, "appts.zip");
  if (fs.existsSync(zipPath)) return zipPath;
  console.log(`Downloading ${APPT_ZIP_URL}`);
  const res = await fetch(APPT_ZIP_URL);
  if (!res.ok) throw new Error(`Download failed: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(zipPath, buf);
  console.log(`Saved ${(buf.length / 1024 / 1024).toFixed(1)} MB`);
  return zipPath;
}

function extractZip(zipPath) {
  const dir = path.join(DATA_DIR, "appts");
  if (fs.existsSync(dir) && fs.readdirSync(dir).some(f => f.endsWith('.csv'))) return dir;
  fs.mkdirSync(dir, { recursive: true });
  const r = spawnSync("unzip", ["-o", zipPath, "-d", dir], { stdio: "inherit" });
  if (r.status !== 0) throw new Error(`unzip failed`);
  return dir;
}

async function upsert(rows) {
  const url = `${SUPABASE_URL}/rest/v1/${TABLE}`;
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
    throw new Error(`Insert failed (${res.status}): ${body.slice(0, 400)}`);
  }
}

async function processFile(csvPath) {
  console.log(`Processing ${path.basename(csvPath)}`);
  const stream = createReadStream(csvPath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream });
  let headers;
  let batch = [];
  let total = 0;
  let inFilterCount = 0;

  for await (const line of rl) {
    if (!line) continue;
    const fields = parseCSVLine(line);
    if (!headers) {
      headers = fields.map(h => h.trim());
      continue;
    }
    const code = (fields[1] || "").toUpperCase();
    const subicb = (fields[6] || "").toUpperCase();
    // Filter: include this practice OR matching sub-ICB
    if (PRACTICE_CODE && code !== PRACTICE_CODE && (!SUB_ICB_CODE || subicb !== SUB_ICB_CODE)) continue;
    inFilterCount++;

    batch.push({
      practice_code: code,
      practice_name: fields[2] || null,
      appointment_month: parseDate(fields[0]),
      supplier: fields[3] || null,
      pcn_code: fields[4] || null,
      pcn_name: fields[5] || null,
      sub_icb_code: fields[6] || null,
      sub_icb_name: fields[7] || null,
      hcp_type: fields[8] || 'Unknown',
      appt_mode: fields[9] || 'Unknown',
      national_category: fields[10] || 'Unknown',
      time_between_book_and_appt: fields[11] || 'Unknown',
      count_of_appointments: parseInt(fields[12], 10) || 0,
      appt_status: fields[13] || 'Unknown',
      source: `NHS Digital Appointments in General Practice ${path.basename(csvPath)}`,
    });

    if (batch.length >= BATCH_SIZE) {
      await upsert(batch);
      total += batch.length;
      if (total % 2500 === 0) process.stdout.write(`\r    ${total} rows...`);
      batch = [];
    }
  }
  if (batch.length) { await upsert(batch); total += batch.length; }
  console.log(`\n  Imported ${total} rows from ${path.basename(csvPath)} (matched ${inFilterCount})`);
}

async function main() {
  if (!PRACTICE_CODE && !SUB_ICB_CODE) {
    throw new Error("Provide PRACTICE_CODE or SUB_ICB_CODE to filter (full file is too large to import).");
  }
  console.log(`Filtering for practice ${PRACTICE_CODE || '(any)'} + sub-ICB ${SUB_ICB_CODE || '(any)'}`);

  const zipPath = await downloadZip();
  const dir = extractZip(zipPath);
  const csvFiles = fs.readdirSync(dir).filter(f => f.startsWith("Practice_Level_Crosstab") && f.endsWith(".csv"));
  for (const f of csvFiles) {
    await processFile(path.join(dir, f));
  }

  console.log("\nRefreshing materialized view...");
  const refreshRes = await fetch(`${SUPABASE_URL}/rest/v1/rpc/refresh_appts_summary`, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
    },
    body: "{}",
  });
  if (!refreshRes.ok) {
    console.warn(`  (Materialized view refresh skipped: ${refreshRes.status})`);
  }
  console.log("Done.");
}

main().catch((err) => { console.error(err); process.exit(1); });
