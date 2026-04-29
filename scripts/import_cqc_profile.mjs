import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CQC_SUBSCRIPTION_KEY = process.env.CQC_SUBSCRIPTION_KEY;

const TABLE_NAME = "gp_practice_cqc_profile";

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const LOCATION_ID_INPUT = (process.env.CQC_LOCATION_ID || "").trim();
const PROVIDER_ID_INPUT = (process.env.CQC_PROVIDER_ID || "").trim();
const DEFAULT_BASE_URL = "https://api.service.cqc.org.uk/public/v1";
const CQC_API_BASE = (process.env.CQC_API_BASE || DEFAULT_BASE_URL).trim().replace(/\/+$/, "");

const DATA_DIR = path.resolve("data");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!CQC_SUBSCRIPTION_KEY) {
  throw new Error("Missing CQC_SUBSCRIPTION_KEY environment variable.");
}
if (!PRACTICE_CODE && !LOCATION_ID_INPUT) {
  throw new Error("Provide PRACTICE_CODE (ODS code) or CQC_LOCATION_ID.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

function cqcHeaders() {
  return {
    "Ocp-Apim-Subscription-Key": CQC_SUBSCRIPTION_KEY,
    Accept: "application/json",
    "User-Agent": "CheckLoopsAdmin-Importer/1.0"
  };
}

async function cqcFetch(url) {
  const res = await fetch(url, { headers: cqcHeaders() });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`CQC request failed (${res.status}) for ${url}: ${body.slice(0, 400)}`);
  }
  return res.json();
}

async function findLocationByOds(odsCode) {
  const url = `${CQC_API_BASE}/locations?odsCode=${encodeURIComponent(odsCode)}&perPage=50&page=1`;
  const data = await cqcFetch(url);
  const items = data?.locations || [];
  if (!items.length) {
    return null;
  }
  const active = items.find((x) => (x.registrationStatus || "").toLowerCase() === "registered") || items[0];
  return active.locationId || null;
}

async function getLocationDetail(locationId) {
  const url = `${CQC_API_BASE}/locations/${encodeURIComponent(locationId)}`;
  return cqcFetch(url);
}

async function getProviderDetail(providerId) {
  if (!providerId) return null;
  const url = `${CQC_API_BASE}/providers/${encodeURIComponent(providerId)}`;
  return cqcFetch(url);
}

function pickRating(currentRatings, key) {
  if (!currentRatings) return null;
  const overall = currentRatings.overall;
  if (key === "overall") return overall?.rating || null;
  const kq = (overall?.keyQuestionRatings || []).find((q) => (q.name || "").toLowerCase() === key.toLowerCase());
  return kq?.rating || null;
}

function asJsonOrNull(v) {
  if (v === undefined || v === null) return null;
  return v;
}

function parseDate(v) {
  if (!v) return null;
  const m = /^\d{4}-\d{2}-\d{2}/.exec(String(v));
  return m ? m[0] : null;
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
  let locationId = LOCATION_ID_INPUT;
  if (!locationId) {
    console.log(`Looking up CQC location for ODS ${PRACTICE_CODE}...`);
    locationId = await findLocationByOds(PRACTICE_CODE);
    if (!locationId) {
      throw new Error(`No CQC location found for ODS code ${PRACTICE_CODE}.`);
    }
    console.log(`Resolved CQC location: ${locationId}`);
  }

  const location = await getLocationDetail(locationId);
  fs.writeFileSync(path.join(DATA_DIR, `cqc_location_${locationId}.json`), JSON.stringify(location, null, 2));

  const providerId = PROVIDER_ID_INPUT || location.providerId || null;
  let provider = null;
  try {
    provider = await getProviderDetail(providerId);
    if (provider) {
      fs.writeFileSync(path.join(DATA_DIR, `cqc_provider_${providerId}.json`), JSON.stringify(provider, null, 2));
    }
  } catch (e) {
    console.warn(`Provider fetch warning: ${String(e)}`);
  }

  const currentRatings = location.currentRatings || null;

  const row = {
    practice_code: PRACTICE_CODE || (location.constituency || locationId).toString().toUpperCase(),
    location_id: location.locationId || locationId,
    provider_id: providerId,
    location_name: location.name || null,
    provider_name: provider?.name || null,
    type: location.type || null,
    registration_status: location.registrationStatus || null,
    registration_date: parseDate(location.registrationDate),
    deregistration_date: parseDate(location.deregistrationDate),
    postal_code: location.postalCode || null,
    region: location.region || null,
    local_authority: location.localAuthority || null,
    constituency: location.constituency || null,
    number_of_beds: Number.isFinite(location.numberOfBeds) ? location.numberOfBeds : null,
    registered_manager_absent: typeof location.registeredManagerAbsent === "boolean" ? location.registeredManagerAbsent : null,
    last_inspection_date: parseDate(location.lastInspection?.date),
    last_report_publication_date: parseDate(location.lastReport?.publicationDate),
    overall_rating: pickRating(currentRatings, "overall"),
    safe_rating: pickRating(currentRatings, "safe"),
    effective_rating: pickRating(currentRatings, "effective"),
    caring_rating: pickRating(currentRatings, "caring"),
    responsive_rating: pickRating(currentRatings, "responsive"),
    well_led_rating: pickRating(currentRatings, "well-led"),
    registered_activities: asJsonOrNull(location.regulatedActivities),
    gac_service_types: asJsonOrNull(location.gacServiceTypes),
    inspection_categories: asJsonOrNull(location.inspectionCategories),
    specialisms: asJsonOrNull(location.specialisms),
    inspection_areas: asJsonOrNull(location.inspectionAreas),
    current_ratings: asJsonOrNull(currentRatings),
    historic_ratings: asJsonOrNull(location.historicRatings),
    reports: asJsonOrNull(location.reports),
    source_location_url: `${CQC_API_BASE}/locations/${encodeURIComponent(locationId)}`,
    source_provider_url: providerId ? `${CQC_API_BASE}/providers/${encodeURIComponent(providerId)}` : null,
    raw_payload: { location, provider },
    imported_at: new Date().toISOString()
  };

  await upsertRow(row);
  console.log(`Upserted CQC profile for practice ${row.practice_code} (location ${row.location_id}).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
