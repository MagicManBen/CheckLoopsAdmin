import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const TABLE_NAME = "gp_practice_geography";

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const POSTCODE = (process.env.POSTCODE || "").trim();
const POSTCODES_API_BASE = (process.env.POSTCODES_API_BASE || "https://api.postcodes.io").trim().replace(/\/+$/, "");

const DATA_DIR = path.resolve("data");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!POSTCODE) {
  throw new Error("Missing POSTCODE.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

function normalisePostcode(pc) {
  return pc.replace(/\s+/g, "").toUpperCase();
}

function splitPostcode(pc) {
  const norm = normalisePostcode(pc);
  if (norm.length < 5) return { outcode: norm, incode: "" };
  const incode = norm.slice(-3);
  const outcode = norm.slice(0, norm.length - 3);
  return { outcode, incode };
}

async function lookupPostcode(pc) {
  const norm = normalisePostcode(pc);
  const url = `${POSTCODES_API_BASE}/postcodes/${encodeURIComponent(norm)}`;
  const res = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": "CheckLoopsAdmin-Importer/1.0" }
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`postcodes.io request failed (${res.status}) for ${norm}: ${body.slice(0, 400)}`);
  }
  const json = await res.json();
  if (json.status !== 200 || !json.result) {
    throw new Error(`postcodes.io returned no result for ${norm}: ${JSON.stringify(json).slice(0, 400)}`);
  }
  return json.result;
}

async function upsertRow(row) {
  const q = new URLSearchParams({ on_conflict: "practice_code" });
  const url = `${SUPABASE_URL}/rest/v1/${TABLE_NAME}?${q.toString()}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify([row])
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Supabase upsert failed (${res.status}): ${body}`);
  }
}

async function main() {
  console.log(`Looking up postcode ${POSTCODE} for practice ${PRACTICE_CODE}...`);
  const result = await lookupPostcode(POSTCODE);
  fs.writeFileSync(
    path.join(DATA_DIR, `geography_${PRACTICE_CODE}.json`),
    JSON.stringify(result, null, 2)
  );

  const norm = normalisePostcode(POSTCODE);
  const { outcode, incode } = splitPostcode(POSTCODE);
  const codes = result.codes || {};

  const row = {
    practice_code: PRACTICE_CODE,
    postcode: result.postcode || POSTCODE,
    postcode_normalised: norm,
    outcode,
    incode,
    country: result.country || null,
    country_code: codes.country || null,
    region: result.region || null,
    region_code: codes.region || null,
    local_authority_district: result.admin_district || null,
    local_authority_district_code: codes.admin_district || null,
    ward: result.admin_ward || null,
    ward_code: codes.admin_ward || null,
    parish: result.parish || null,
    parish_code: codes.parish || null,
    parliamentary_constituency: result.parliamentary_constituency || null,
    parliamentary_constituency_code: codes.parliamentary_constituency || null,
    ccg: result.ccg || null,
    ccg_code: codes.ccg || null,
    nhs_ha: result.nhs_ha || null,
    nhs_ha_code: codes.nhs_ha || null,
    lsoa: result.lsoa || null,
    lsoa_code: codes.lsoa || null,
    msoa: result.msoa || null,
    msoa_code: codes.msoa || null,
    oa_code: codes.oa || null,
    latitude: typeof result.latitude === "number" ? result.latitude : null,
    longitude: typeof result.longitude === "number" ? result.longitude : null,
    eastings: Number.isFinite(result.eastings) ? result.eastings : null,
    northings: Number.isFinite(result.northings) ? result.northings : null,
    source: "postcodes.io (ONS data)",
    source_url: `${POSTCODES_API_BASE}/postcodes/${encodeURIComponent(norm)}`,
    raw_payload: result,
    imported_at: new Date().toISOString()
  };

  await upsertRow(row);
  console.log(`Upserted geography for practice ${PRACTICE_CODE}: LSOA=${row.lsoa_code || "n/a"}, LA=${row.local_authority_district || "n/a"}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
