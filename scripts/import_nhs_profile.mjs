import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const NHS_API_KEY = process.env.NHS_API_KEY;

const TABLE_NAME = "gp_practice_nhs_profile";

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const NHS_API_BASE = (process.env.NHS_API_BASE || "https://api.nhs.uk/service-search").trim().replace(/\/+$/, "");
const NHS_API_VERSION = (process.env.NHS_API_VERSION || "2").trim();

const DATA_DIR = path.resolve("data");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!NHS_API_KEY) {
  throw new Error("Missing NHS_API_KEY environment variable (Ocp-Apim-Subscription-Key for api.nhs.uk).");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

function nhsHeaders() {
  return {
    "Ocp-Apim-Subscription-Key": NHS_API_KEY,
    "subscription-key": NHS_API_KEY,
    Accept: "application/json",
    "User-Agent": "CheckLoopsAdmin-Importer/1.0"
  };
}

async function nhsFetch(url) {
  const res = await fetch(url, { headers: nhsHeaders() });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`NHS API request failed (${res.status}) for ${url}: ${body.slice(0, 400)}`);
  }
  return res.json();
}

async function searchByOds(odsCode) {
  const params = new URLSearchParams({
    "api-version": NHS_API_VERSION,
    search: odsCode,
    searchFields: "OrganisationCode,ODSCode",
    $top: "5"
  });
  const url = `${NHS_API_BASE}/search?${params.toString()}`;
  const data = await nhsFetch(url);
  const items = data?.value || [];
  if (!items.length) {
    return null;
  }
  const exact = items.find((x) => {
    const c = (x.OrganisationCode || x.ODSCode || "").toString().toUpperCase();
    return c === odsCode;
  });
  return exact || items[0];
}

function pick(obj, ...keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && obj[k] !== "") {
      return obj[k];
    }
  }
  return null;
}

function toNumber(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

function asJsonOrNull(v) {
  if (v === undefined || v === null) return null;
  if (Array.isArray(v) && v.length === 0) return null;
  if (typeof v === "object" && Object.keys(v).length === 0) return null;
  return v;
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
  console.log(`Searching NHS service-search for ODS ${PRACTICE_CODE}...`);
  const item = await searchByOds(PRACTICE_CODE);
  if (!item) {
    throw new Error(`No NHS service-search result for ODS code ${PRACTICE_CODE}.`);
  }

  fs.writeFileSync(
    path.join(DATA_DIR, `nhs_profile_${PRACTICE_CODE}.json`),
    JSON.stringify(item, null, 2)
  );

  const lat = toNumber(pick(item, "Latitude", "latitude")) ?? toNumber(item.GeoLocation?.Latitude);
  const lng = toNumber(pick(item, "Longitude", "longitude")) ?? toNumber(item.GeoLocation?.Longitude);

  const row = {
    practice_code: PRACTICE_CODE,
    organisation_name: pick(item, "OrganisationName", "organisationName", "Name"),
    organisation_type: pick(item, "OrganisationType", "organisationType", "OrganisationSubType"),
    parent_organisation: pick(item, "ParentOrganisation", "ParentName"),
    address_line_1: pick(item, "Address1", "AddressLine1"),
    address_line_2: pick(item, "Address2", "AddressLine2"),
    address_line_3: pick(item, "Address3", "AddressLine3"),
    town: pick(item, "City", "Town"),
    county: pick(item, "County"),
    postcode: pick(item, "Postcode", "PostCode"),
    country: pick(item, "Country"),
    phone: pick(item, "Phone", "Telephone", "Contacts"),
    fax: pick(item, "Fax"),
    email: pick(item, "Email"),
    website: pick(item, "URL", "Website", "OrganisationURL"),
    latitude: lat,
    longitude: lng,
    accepting_new_patients: pick(item, "AcceptingPatients", "AcceptingNewPatients", "GPRegistration"),
    accepting_new_patients_updated_at: null,
    online_booking_url: pick(item, "OnlineBookingURL"),
    prescription_ordering_url: pick(item, "PrescriptionOrderingURL"),
    appointment_booking_url: pick(item, "AppointmentBookingURL"),
    opening_times: asJsonOrNull(item.OpeningTimes || item.openingTimes),
    reception_times: asJsonOrNull(item.ReceptionTimes),
    consulting_times: asJsonOrNull(item.ConsultingTimes),
    facilities: asJsonOrNull(item.Facilities),
    accessibility: asJsonOrNull(item.Accessibility || item.AccessibilityInfo),
    services: asJsonOrNull(item.Services || item.ServicesProvided),
    staff: asJsonOrNull(item.Staff || item.GPs),
    metrics: asJsonOrNull(item.Metrics),
    source_url: `${NHS_API_BASE}/search?search=${encodeURIComponent(PRACTICE_CODE)}`,
    source_api: "api.nhs.uk service-search",
    raw_payload: item,
    imported_at: new Date().toISOString()
  };

  await upsertRow(row);
  console.log(`Upserted NHS profile for practice ${PRACTICE_CODE}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
