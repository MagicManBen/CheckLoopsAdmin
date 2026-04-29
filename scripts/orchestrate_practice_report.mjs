import { spawn } from "node:child_process";
import process from "node:process";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PRACTICE_CODE = (process.env.PRACTICE_CODE || "").trim().toUpperCase();
const PROVIDED_POSTCODE = (process.env.POSTCODE || "").trim();
const RUN_ID_INPUT = (process.env.RUN_ID || "").trim();
const TRIGGERED_BY = (process.env.TRIGGERED_BY || "orchestrator").trim();
const GITHUB_RUN_ID = process.env.GITHUB_RUN_ID || null;
const GITHUB_RUN_URL = process.env.GITHUB_SERVER_URL && process.env.GITHUB_REPOSITORY && GITHUB_RUN_ID
  ? `${process.env.GITHUB_SERVER_URL}/${process.env.GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}`
  : null;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY.");
}
if (!PRACTICE_CODE) {
  throw new Error("Missing PRACTICE_CODE.");
}

/* --------- Supabase REST helpers --------- */
async function supaRequest(method, path, { params, body } = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${path}`);
  if (params) for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const res = await fetch(url, {
    method,
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=representation"
    },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase ${method} ${path} failed (${res.status}): ${text.slice(0, 400)}`);
  }
  return res.json();
}
const supaInsert = (table, row) => supaRequest("POST", table, { body: [row] }).then((r) => r[0]);
const supaUpdate = (table, match, patch) =>
  supaRequest("PATCH", table, { params: match, body: patch }).then((r) => r[0]);

/* --------- Practice resolution via NHS ORD --------- */
async function lookupPracticeViaOrd(odsCode) {
  const url = `https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/${encodeURIComponent(odsCode)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    return { name: null, postcode: null };
  }
  const json = await res.json();
  const org = json?.Organisation;
  const name = org?.Name || null;
  const postcode = org?.GeoLoc?.Location?.PostCode || null;
  return { name, postcode };
}

/* --------- Pipeline registry --------- */
const PIPELINES = [
  {
    dataset: "patient_counts",
    label: "Patient age counts",
    script: "scripts/import_one_gp_patient_counts.mjs",
    needsPostcode: false,
    env: () => ({
      ODS_CODE: PRACTICE_CODE,
      MALE_CSV_URL: "https://files.digital.nhs.uk/A1/BF1917/gp-reg-pat-prac-sing-age-male.csv",
      FEMALE_CSV_URL: "https://files.digital.nhs.uk/5D/93A12A/gp-reg-pat-prac-sing-age-female.csv"
    })
  },
  {
    dataset: "workforce",
    label: "Workforce metrics",
    script: "scripts/import_gp_workforce.mjs",
    needsPostcode: false,
    env: () => ({
      PRACTICE_CODE,
      SNAPSHOT_DATE: "2026-03-31",
      PUBLICATION_LABEL: "General Practice Workforce 31 March 2026",
      PRACTICE_ZIP_URL: "https://files.digital.nhs.uk/85/7F0D90/GPWPracticeCSV.032026.zip"
    })
  },
  {
    dataset: "gpps",
    label: "GP Patient Survey",
    script: "scripts/import_gp_patient_survey.mjs",
    needsPostcode: false,
    env: () => ({
      PRACTICE_CODE,
      SURVEY_YEAR: "2025",
      PUBLICATION_LABEL: "GP Patient Survey 2025 Practice Data (weighted)",
      SURVEY_CSV_URL: "https://gp-patient.co.uk/FileDownload/Download?fileRedirect=2025%2Fsurvey-results%2Fpractice-results%2Fpractice-data-csv%2FGPPS_2025_Practice_data_(weighted)_(csv)_PUBLIC.csv"
    })
  },
  {
    dataset: "nhs_profile",
    label: "NHS.uk public profile",
    script: "scripts/import_nhs_profile.mjs",
    needsPostcode: false,
    optional: true,
    env: () => ({
      PRACTICE_CODE,
      NHS_API_BASE: process.env.NHS_API_BASE || "https://sandbox.api.service.nhs.uk",
      NHS_SEARCH_PATH: process.env.NHS_SEARCH_PATH || "/service-search-api",
      NHS_API_VERSION: "3",
      NHS_API_KEY: process.env.NHS_API_KEY || "",
      NHS_KID: process.env.NHS_KID || "checkloops-key-1",
      NHS_PRIVATE_KEY_PEM: process.env.NHS_PRIVATE_KEY_PEM || ""
    })
  },
  {
    dataset: "geography",
    label: "ONS geography",
    script: "scripts/import_ons_geography.mjs",
    needsPostcode: true,
    env: (postcode) => ({
      PRACTICE_CODE,
      POSTCODE: postcode,
      POSTCODES_API_BASE: "https://api.postcodes.io"
    })
  }
];

/* --------- Run a child script and capture status --------- */
function runChild(scriptPath, env) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [scriptPath], {
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); process.stdout.write(d); });
    child.stderr.on("data", (d) => { stderr += d.toString(); process.stderr.write(d); });
    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

/* --------- Main --------- */
async function main() {
  console.log(`Orchestrating practice report for ${PRACTICE_CODE}`);

  const ord = await lookupPracticeViaOrd(PRACTICE_CODE);
  const postcode = PROVIDED_POSTCODE || ord.postcode || "";
  console.log(`ORD: name=${ord.name || "?"}, postcode=${ord.postcode || "?"}, using postcode=${postcode || "(none)"}`);

  let run;
  if (RUN_ID_INPUT) {
    run = await supaUpdate("practice_ingestion_runs", { id: `eq.${RUN_ID_INPUT}` }, {
      practice_name: ord.name,
      practice_postcode: postcode || null,
      status: "running",
      github_run_id: GITHUB_RUN_ID ? Number(GITHUB_RUN_ID) : null,
      github_run_url: GITHUB_RUN_URL,
      started_at: new Date().toISOString()
    });
  } else {
    run = await supaInsert("practice_ingestion_runs", {
      practice_code: PRACTICE_CODE,
      practice_name: ord.name,
      practice_postcode: postcode || null,
      status: "running",
      github_run_id: GITHUB_RUN_ID ? Number(GITHUB_RUN_ID) : null,
      github_run_url: GITHUB_RUN_URL,
      triggered_by: TRIGGERED_BY,
      started_at: new Date().toISOString()
    });
  }
  console.log(`Run id: ${run.id}`);

  // Pre-create job rows so the UI sees them all immediately
  const jobRows = await supaRequest("POST", "practice_ingestion_jobs", {
    body: PIPELINES.map((p) => ({
      run_id: run.id,
      practice_code: PRACTICE_CODE,
      dataset: p.dataset,
      dataset_label: p.label,
      status: (p.needsPostcode && !postcode) ? "skipped" : "queued",
      message: (p.needsPostcode && !postcode) ? "No postcode available" : null
    }))
  });
  const jobByDataset = Object.fromEntries(jobRows.map((j) => [j.dataset, j]));

  let anyFailure = false;
  for (const pipeline of PIPELINES) {
    const job = jobByDataset[pipeline.dataset];
    if (job.status === "skipped") {
      console.log(`\n--- ${pipeline.label}: SKIPPED (${job.message}) ---`);
      continue;
    }
    if (pipeline.optional) {
      const env = pipeline.env(postcode);
      if (pipeline.dataset === "nhs_profile" && (!env.NHS_API_KEY || !env.NHS_PRIVATE_KEY_PEM)) {
        await supaUpdate("practice_ingestion_jobs", { id: `eq.${job.id}` }, {
          status: "skipped",
          message: "NHS_API_KEY or NHS_PRIVATE_KEY_PEM not set"
        });
        console.log(`\n--- ${pipeline.label}: SKIPPED (NHS secrets missing) ---`);
        continue;
      }
    }

    console.log(`\n--- ${pipeline.label} ---`);
    const startedAt = new Date();
    await supaUpdate("practice_ingestion_jobs", { id: `eq.${job.id}` }, {
      status: "running",
      started_at: startedAt.toISOString()
    });

    let result;
    try {
      result = await runChild(pipeline.script, pipeline.env(postcode));
    } catch (err) {
      result = { code: 1, stdout: "", stderr: String(err) };
    }
    const completedAt = new Date();
    const ok = result.code === 0;
    if (!ok) anyFailure = true;

    const tail = (result.stdout + "\n" + result.stderr).trim().split("\n").slice(-6).join("\n").slice(-1000);
    await supaUpdate("practice_ingestion_jobs", { id: `eq.${job.id}` }, {
      status: ok ? "success" : "failed",
      completed_at: completedAt.toISOString(),
      duration_ms: completedAt - startedAt,
      message: tail || null
    });
    console.log(`--- ${pipeline.label}: ${ok ? "✓ SUCCESS" : "✗ FAILED"} (${completedAt - startedAt}ms) ---`);
  }

  await supaUpdate("practice_ingestion_runs", { id: `eq.${run.id}` }, {
    status: anyFailure ? "completed_with_errors" : "completed",
    completed_at: new Date().toISOString()
  });

  console.log(`\nDone. Run id: ${run.id}`);
  if (anyFailure) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
