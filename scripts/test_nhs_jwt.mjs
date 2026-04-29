import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const NHS_API_KEY = (process.env.NHS_API_KEY || "").trim();
const NHS_KID = (process.env.NHS_KID || "checkloops-key-1").trim();
const NHS_API_BASE = (process.env.NHS_API_BASE || "https://sandbox.api.service.nhs.uk").trim().replace(/\/+$/, "");
const NHS_TOKEN_URL = (process.env.NHS_TOKEN_URL || `${NHS_API_BASE}/oauth2/token`).trim();
const KEY_PATH = process.env.NHS_PRIVATE_KEY_PATH || "nhs_private.pem";

if (!NHS_API_KEY) {
  console.error("Set NHS_API_KEY (the client_id from the NHS Developer console).");
  process.exit(2);
}
const pemAbs = path.resolve(KEY_PATH);
if (!fs.existsSync(pemAbs)) {
  console.error(`Private key not found at ${pemAbs}`);
  process.exit(2);
}
const pem = fs.readFileSync(pemAbs, "utf8");
if (!pem.includes("PRIVATE KEY")) {
  console.error(`File at ${pemAbs} does not look like a PEM private key.`);
  process.exit(2);
}

function b64url(buf) {
  const b = Buffer.isBuffer(buf) ? buf.toString("base64") : Buffer.from(buf).toString("base64");
  return b.replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

const now = Math.floor(Date.now() / 1000);
const header = { alg: "RS512", typ: "JWT", kid: NHS_KID };
const payload = {
  iss: NHS_API_KEY,
  sub: NHS_API_KEY,
  aud: NHS_TOKEN_URL,
  jti: crypto.randomUUID(),
  iat: now,
  exp: now + 300
};
const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(payload))}`;
const signature = crypto.sign("sha512", Buffer.from(signingInput), {
  key: pem,
  padding: crypto.constants.RSA_PKCS1_PADDING
});
const assertion = `${signingInput}.${b64url(signature)}`;

console.log(`Token endpoint: ${NHS_TOKEN_URL}`);
console.log(`client_id (iss/sub): ${NHS_API_KEY}`);
console.log(`kid: ${NHS_KID}`);
console.log(`JWT length: ${assertion.length} chars`);

const body = new URLSearchParams({
  grant_type: "client_credentials",
  client_assertion_type: "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
  client_assertion: assertion
});

const res = await fetch(NHS_TOKEN_URL, {
  method: "POST",
  headers: {
    "Content-Type": "application/x-www-form-urlencoded",
    Accept: "application/json",
    "User-Agent": "CheckLoopsAdmin-Test/1.0"
  },
  body
});
const text = await res.text();
console.log(`\n--- HTTP ${res.status} ---`);
console.log(text.slice(0, 1500));

if (res.ok) {
  try {
    const j = JSON.parse(text);
    if (j.access_token) {
      console.log(`\n✓ Got access token (length=${j.access_token.length}, expires_in=${j.expires_in}).`);
    }
  } catch {}
}
