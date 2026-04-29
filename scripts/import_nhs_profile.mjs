import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const NHS_API_KEY = (process.env.NHS_API_KEY || "").trim();
const NHS_PRIVATE_KEY_PEM = (process.env.NHS_PRIVATE_KEY_PEM || "").trim();
const NHS_KID = (process.env.NHS_KID || "checkloops-key-1").trim();

const TABLE_NAME = "gp_practice_nhs_profile";

const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const NHS_API_BASE = (process.env.NHS_API_BASE || "https://sandbox.api.service.nhs.uk").trim().replace(/\/+$/, "");
const NHS_TOKEN_URL = (process.env.NHS_TOKEN_URL || `${NHS_API_BASE}/oauth2/token`).trim();
const NHS_SEARCH_PATH = (process.env.NHS_SEARCH_PATH || "/service-search-api").trim();
const NHS_API_VERSION = (process.env.NHS_API_VERSION || "3").trim();

const DATA_DIR = path.resolve("data");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}
if (!NHS_API_KEY) {
  throw new Error("Missing NHS_API_KEY (client_id from the NHS Developer console).");
}
if (!NHS_PRIVATE_KEY_PEM || !NHS_PRIVATE_KEY_PEM.includes("PRIVATE KEY")) {
  throw new Error("Missing or malformed NHS_PRIVATE_KEY_PEM. Paste the full PEM including BEGIN/END markers.");
}

fs.mkdirSync(DATA_DIR, { recursive: true });

function base64UrlEncode(buffer) {
  const b64 = Buffer.isBuffer(buffer) ? buffer.toString("base64") : Buffer.from(buffer).toString("base64");
  return b64.replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function buildSignedClientAssertion() {
  const now = Math.floor(Date.now() / 1000);
  const header = {
    alg: "RS512",
    typ: "JWT",
    kid: NHS_KID
  };
  const payload = {
    iss: NHS_API_KEY,
    sub: NHS_API_KEY,
    aud: NHS_TOKEN_URL,
    jti: crypto.randomUUID(),
    iat: now,
    exp: now + 300
  };
  const signingInput = `${base64UrlEncode(JSON.stringify(header))}.${base64UrlEncode(JSON.stringify(payload))}`;
  const signature = crypto.sign("sha512", Buffer.from(signingInput), {
    key: NHS_PRIVATE_KEY_PEM,
    padding: crypto.constants.RSA_PKCS1_PADDING
  });
  return `${signingInput}.${base64UrlEncode(signature)}`;
}

async function exchangeForAccessToken() {
  const assertion = buildSignedClientAssertion();
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_assertion_type: "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
    client_assertion: assertion
  });
  console.log(`Requesting access token from ${NHS_TOKEN_URL}`);
  const res = await fetch(NHS_TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
      "User-Agent": "CheckLoopsAdmin-Importer/1.0"
    },
    body
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`NHS token exchange failed (${res.status}): ${text.slice(0, 600)}`);
  }
  const json = await res.json();
  if (!json.access_token) {
    throw new Error(`NHS token response missing access_token: ${JSON.stringify(json).slice(0, 400)}`);
  }
  console.log(`Got NHS access token (expires_in=${json.expires_in || "?"}s).`);
  return json.access_token;
}

async function nhsAuthedFetch(url, accessToken) {
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      apikey: NHS_API_KEY,
      "User-Agent": "CheckLoopsAdmin-Importer/1.0"
    }
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`NHS API request failed (${res.status}) for ${url}: ${text.slice(0, 600)}`);
  }
  return res.json();
}

function buildSearchUrl() {
  if (/[?&]/.test(NHS_SEARCH_PATH)) {
    return `${NHS_API_BASE}${NHS_SEARCH_PATH}`;
  }
  const params = new URLSearchParams();
  params.set("api-version", NHS_API_VERSION);
  params.set("$filter", `OrganisationCode eq '${PRACTICE_CODE}' or ODSCode eq '${PRACTICE_CODE}'`);
  params.set("$top", "5");
  return `${NHS_API_BASE}${NHS_SEARCH_PATH}?${params.toString()}`;
}

async function searchByOds(accessToken) {
  const url = buildSearchUrl();
  console.log(`Searching NHS for ODS ${PRACTICE_CODE} at ${url}`);
  const data = await nhsAuthedFetch(url, accessToken);
  const items = data?.value || data?.results || data?.organisations || [];
  if (!Array.isArray(items) || !items.length) {
    return null;
  }
  const exact = items.find((x) => {
    const c = (x.OrganisationCode || x.ODSCode || x.organisationCode || x.odsCode || "").toString().toUpperCase();
    return c === PRACTICE_CODE;
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
  const accessToken = await exchangeForAccessToken();
  const item = await searchByOds(accessToken);
  if (!item) {
    throw new Error(`No NHS organisation found for ODS code ${PRACTICE_CODE}.`);
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
    source_url: buildSearchUrl(),
    source_api: "NHS API platform — Directory of Services Search API (Private Key JWT)",
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
