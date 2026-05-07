import fs from "node:fs";
import path from "node:path";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TABLE_NAME = "imd_lsoa_metrics";

const IMD_CSV_URL = process.env.IMD_CSV_URL ||
  "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/845345/File_7_-_All_IoD2019_Scores__Ranks__Deciles_and_Population_Denominators_3.csv";

const DATA_DIR = path.resolve("data");
const BATCH_SIZE = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

function parseCSVLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      fields.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current.trim());
  return fields;
}

function num(val) {
  if (val === "" || val == null) return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

function int(val) {
  if (val === "" || val == null) return null;
  const n = parseInt(val, 10);
  return Number.isFinite(n) ? n : null;
}

function mapRow(fields) {
  return {
    lsoa_code: fields[0] || null,
    lsoa_name: fields[1] || null,
    local_authority_code: fields[2] || null,
    local_authority: fields[3] || null,
    imd_score: num(fields[4]),
    imd_rank: int(fields[5]),
    imd_decile: int(fields[6]),
    income_score: num(fields[7]),
    income_rank: int(fields[8]),
    income_decile: int(fields[9]),
    employment_score: num(fields[10]),
    employment_rank: int(fields[11]),
    employment_decile: int(fields[12]),
    education_score: num(fields[13]),
    education_rank: int(fields[14]),
    education_decile: int(fields[15]),
    health_score: num(fields[16]),
    health_rank: int(fields[17]),
    health_decile: int(fields[18]),
    crime_score: num(fields[19]),
    crime_rank: int(fields[20]),
    crime_decile: int(fields[21]),
    housing_score: num(fields[22]),
    housing_rank: int(fields[23]),
    housing_decile: int(fields[24]),
    environment_score: num(fields[25]),
    environment_rank: int(fields[26]),
    environment_decile: int(fields[27]),
    idaci_score: num(fields[28]),
    idaci_rank: int(fields[29]),
    idaci_decile: int(fields[30]),
    idaopi_score: num(fields[31]),
    idaopi_rank: int(fields[32]),
    idaopi_decile: int(fields[33]),
    total_population: int(fields[51]),
    dependent_children_0_15: int(fields[52]),
    population_16_59: int(fields[53]),
    older_population_60_plus: int(fields[54]),
    source: "MHCLG English Indices of Deprivation 2019",
    source_url: IMD_CSV_URL,
    imported_at: new Date().toISOString(),
  };
}

async function upsertBatch(rows) {
  const q = new URLSearchParams({ on_conflict: "lsoa_code" });
  const url = `${SUPABASE_URL}/rest/v1/${TABLE_NAME}?${q}`;
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
    throw new Error(`Supabase upsert failed (${res.status}): ${body.slice(0, 400)}`);
  }
}

async function main() {
  console.log(`Downloading IMD 2019 CSV...`);
  const res = await fetch(IMD_CSV_URL);
  if (!res.ok) throw new Error(`CSV download failed: ${res.status}`);

  const csvPath = path.join(DATA_DIR, "imd_2019_all.csv");
  const fileStream = createWriteStream(csvPath);
  await pipeline(Readable.fromWeb(res.body), fileStream);
  console.log(`Downloaded to ${csvPath}`);

  const content = fs.readFileSync(csvPath, "utf-8");
  const lines = content.split(/\r?\n/).filter((l) => l.trim());
  console.log(`Parsed ${lines.length - 1} data rows`);

  let batch = [];
  let total = 0;

  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    if (!fields[0] || !fields[0].startsWith("E")) continue;
    const row = mapRow(fields);
    batch.push(row);

    if (batch.length >= BATCH_SIZE) {
      await upsertBatch(batch);
      total += batch.length;
      process.stdout.write(`\r  Upserted ${total} rows...`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await upsertBatch(batch);
    total += batch.length;
  }

  console.log(`\nDone. Imported ${total} LSOA rows into ${TABLE_NAME}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
