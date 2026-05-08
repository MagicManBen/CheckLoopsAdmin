import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const LSOA_FILTER = (process.env.LSOA_FILTER || "").trim().toUpperCase().split(/\s*,\s*/).filter(Boolean);
const BATCH_SIZE = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing Supabase env");

const DATA_DIR = path.resolve("data");
fs.mkdirSync(DATA_DIR, { recursive: true });

const TABLES = [
  { code: "TS001", name: "Residence type" },
  { code: "TS007A", name: "Age (5-year bands)" },
  { code: "TS008", name: "Sex" },
  { code: "TS021", name: "Ethnic group" },
  { code: "TS025", name: "Country of birth" },
  { code: "TS030", name: "Religion" },
  { code: "TS037", name: "General health" },
  { code: "TS046", name: "Disability" },
  { code: "TS054", name: "Tenure" },
  { code: "TS061", name: "Method of travel to work" },
  { code: "TS066", name: "Economic activity status" },
];

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

function splitColumnHeader(header) {
  // e.g. "Ethnic group: Asian, Asian British or Asian Welsh: Bangladeshi"
  // Returns category (after first ':') and subcategory (after second ':')
  const parts = header.split(":").map(s => s.trim());
  if (parts.length === 1) return { category: parts[0], subcategory: null };
  if (parts.length === 2) return { category: parts[1], subcategory: null };
  return { category: parts[1], subcategory: parts.slice(2).join(": ") };
}

async function downloadAndExtract(code) {
  const lc = code.toLowerCase();
  const zipPath = path.join(DATA_DIR, `${lc}.zip`);
  const csvPath = path.join(DATA_DIR, `census2021-${lc}-lsoa.csv`);
  if (fs.existsSync(csvPath)) return csvPath;
  if (!fs.existsSync(zipPath)) {
    console.log(`  Downloading ${code}.zip`);
    const res = await fetch(`https://www.nomisweb.co.uk/output/census/2021/census2021-${lc}.zip`);
    if (!res.ok) throw new Error(`Download ${code} failed: ${res.status}`);
    const buf = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(zipPath, buf);
  }
  const r = spawnSync("unzip", ["-o", "-j", zipPath, `census2021-${lc}-lsoa.csv`, "-d", DATA_DIR], { stdio: "ignore" });
  if (r.status !== 0) throw new Error(`unzip failed for ${code}`);
  return csvPath;
}

async function upsert(rows) {
  const url = `${SUPABASE_URL}/rest/v1/census_2021_lsoa`;
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
    throw new Error(`Insert failed (${res.status}): ${(await res.text()).slice(0, 400)}`);
  }
}

async function processTable(meta) {
  console.log(`\n${meta.code}: ${meta.name}`);
  const csvPath = await downloadAndExtract(meta.code);
  const stream = createReadStream(csvPath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream });
  let headers;
  let batch = [];
  let total = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    const fields = parseCSVLine(line);
    if (!headers) {
      headers = fields.map(h => h.trim().replace(/^"|"$/g, ""));
      continue;
    }
    const lsoaName = (fields[1] || "").replace(/^"|"$/g, "");
    const lsoaCode = (fields[2] || "").replace(/^"|"$/g, "").toUpperCase();
    if (!lsoaCode.startsWith("E01") && !lsoaCode.startsWith("W01")) continue;
    if (LSOA_FILTER.length && !LSOA_FILTER.includes(lsoaCode)) continue;

    // Each value column maps to a category
    for (let i = 3; i < headers.length; i++) {
      const value = parseInt(String(fields[i]).replace(/[",]/g, ""), 10);
      if (!Number.isFinite(value)) continue;
      const { category, subcategory } = splitColumnHeader(headers[i]);
      batch.push({
        lsoa_code: lsoaCode,
        lsoa_name: lsoaName,
        table_code: meta.code,
        table_name: meta.name,
        category,
        subcategory,
        value,
        source: `ONS Census 2021 ${meta.code} (${meta.name})`,
      });

      if (batch.length >= BATCH_SIZE) {
        await upsert(batch);
        total += batch.length;
        if (total % 5000 === 0) process.stdout.write(`\r    ${total} rows...`);
        batch = [];
      }
    }
  }
  if (batch.length) { await upsert(batch); total += batch.length; }
  console.log(`\n  -> ${total} rows`);
  return total;
}

async function main() {
  console.log(`Census 2021 LSOA importer (filter: ${LSOA_FILTER.length ? LSOA_FILTER.join(", ") : "all LSOAs"})`);
  let grandTotal = 0;
  for (const t of TABLES) {
    grandTotal += await processTable(t);
  }
  console.log(`\nDone. Total: ${grandTotal} rows across ${TABLES.length} tables.`);
}

main().catch((err) => { console.error(err); process.exit(1); });
