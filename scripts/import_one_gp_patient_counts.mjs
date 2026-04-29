import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TABLE_NAME = "gp_practice_patient_age_counts";
const BATCH_SIZE = 500;
const ODS_CODE = (process.env.ODS_CODE || "M83076").trim().toUpperCase();
const DATA_DIR = path.resolve("data");

const FILES = {
  MALE: {
    url: "https://files.digital.nhs.uk/A1/BF1917/gp-reg-pat-prac-sing-age-male.csv",
    path: path.join(DATA_DIR, "gp-reg-pat-prac-sing-age-male.csv")
  },
  FEMALE: {
    url: "https://files.digital.nhs.uk/5D/93A12A/gp-reg-pat-prac-sing-age-female.csv",
    path: path.join(DATA_DIR, "gp-reg-pat-prac-sing-age-female.csv")
  }
};

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

async function downloadIfMissing(sex, info) {
  if (fs.existsSync(info.path) && fs.statSync(info.path).size > 0) {
    const sizeMb = fs.statSync(info.path).size / 1024 / 1024;
    console.log(`${sex} CSV already exists: ${info.path} (${sizeMb.toFixed(2)} MB)`);
    return;
  }
  console.log(`Downloading ${sex} CSV from ${info.url}`);
  const res = await fetch(info.url);
  if (!res.ok) throw new Error(`Failed to download ${sex} CSV (${res.status})`);
  const bytes = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(info.path, bytes);
  const sizeMb = bytes.length / 1024 / 1024;
  console.log(`Saved ${sex} CSV: ${sizeMb.toFixed(2)} MB`);
}

function cleanInt(v) {
  const t = (v || "").replaceAll(",", "").trim();
  if (!t) return 0;
  return Number.parseInt(t, 10);
}

async function upsertBatch(rows) {
  if (!rows.length) return;
  const q = new URLSearchParams({
    on_conflict: "practice_code,extract_date,sex,age"
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
    throw new Error(`Upsert failed (${res.status}): ${body}`);
  }
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
  const required = ["EXTRACT_DATE", "ORG_CODE", "POSTCODE", "SEX", "AGE", "NUMBER_OF_PATIENTS"];
  const idx = {};
  for (const col of required) {
    const i = headers.indexOf(col);
    if (i === -1) throw new Error(`Missing expected column: ${col}`);
    idx[col] = i;
  }
  return idx;
}

async function importOnePractice(sex, csvPath, sourceUrl) {
  const nowIso = new Date().toISOString();
  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return { sex, matched: 0, imported: 0, skipped: 0 };

  const idx = buildIndexMap(lines[0].replace(/^\uFEFF/, ""));
  let matched = 0;
  let skipped = 0;
  let imported = 0;
  const batch = [];

  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    const rowSex = (cols[idx.SEX] || "").trim().toUpperCase();
    const practiceCode = (cols[idx.ORG_CODE] || "").trim().toUpperCase();
    if (rowSex !== sex || practiceCode !== ODS_CODE) {
      skipped += 1;
      continue;
    }
    const extractDate = (cols[idx.EXTRACT_DATE] || "").trim();
    const age = (cols[idx.AGE] || "").trim();
    if (!extractDate || !age) {
      skipped += 1;
      continue;
    }
    matched += 1;
    batch.push({
      practice_code: practiceCode,
      extract_date: extractDate,
      publication: null,
      practice_postcode: (cols[idx.POSTCODE] || "").trim() || null,
      sex: rowSex,
      age,
      number_of_patients: cleanInt(cols[idx.NUMBER_OF_PATIENTS] || "0"),
      source_url: sourceUrl,
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
  return { sex, matched, imported, skipped };
}

async function main() {
  console.log(`Target practice: ${ODS_CODE}`);
  let grandTotal = 0;
  for (const [sex, info] of Object.entries(FILES)) {
    await downloadIfMissing(sex, info);
    const result = await importOnePractice(sex, info.path, info.url);
    grandTotal += result.imported;
    console.log(`${sex} -> matched=${result.matched}, imported=${result.imported}, skipped=${result.skipped}`);
  }
  console.log(`Total upserted rows for ${ODS_CODE}: ${grandTotal}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
