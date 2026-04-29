// CheckLoops — start-report edge function (mirrored copy)
// This file is the source of truth for the Supabase edge function deployed
// at https://kvrcmqpwdkfiqemybmkc.supabase.co/functions/v1/start-report
//
// To redeploy after editing this file, either:
//   1. Use the Supabase MCP `deploy_edge_function` tool, or
//   2. Run `supabase functions deploy start-report` in this folder.
//
// Required Supabase secrets (Project Settings → Edge Functions → Manage secrets):
//   GITHUB_DISPATCH_PAT   — GitHub PAT with `actions:write` scope on this repo
//   GITHUB_OWNER          — optional, defaults to "MagicManBen"
//   GITHUB_REPO           — optional, defaults to "CheckLoopsAdmin"
//   GITHUB_BRANCH         — optional, defaults to "main"
// SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are auto-injected by Supabase.

import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GITHUB_PAT = Deno.env.get("GITHUB_DISPATCH_PAT") || Deno.env.get("GITHUB_PAT");
const GITHUB_OWNER = Deno.env.get("GITHUB_OWNER") || "MagicManBen";
const GITHUB_REPO = Deno.env.get("GITHUB_REPO") || "CheckLoopsAdmin";
const GITHUB_BRANCH = Deno.env.get("GITHUB_BRANCH") || "main";
const WORKFLOW_FILE = "import-all-for-practice.yml";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS"
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders }
  });
}

function sanitiseOds(s: string) {
  return s.replace(/[^A-Za-z0-9]/g, "").toUpperCase().slice(0, 12);
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return json({ error: "Use POST" }, 405);
  }
  if (!GITHUB_PAT) {
    return json({ error: "Server missing GITHUB_DISPATCH_PAT secret" }, 500);
  }

  let body: any = {};
  try { body = await req.json(); } catch { /* ignore */ }
  const practiceCodeRaw = (body?.practice_code || body?.ods_code || "").toString();
  const practiceCode = sanitiseOds(practiceCodeRaw);
  const postcode = (body?.postcode || "").toString().trim().slice(0, 16);

  if (!practiceCode || practiceCode.length < 4) {
    return json({ error: "Provide a valid practice_code (ODS code)." }, 400);
  }

  const insert = await fetch(`${SUPABASE_URL}/rest/v1/practice_ingestion_runs`, {
    method: "POST",
    headers: {
      apikey: SERVICE_ROLE,
      Authorization: `Bearer ${SERVICE_ROLE}`,
      "Content-Type": "application/json",
      Prefer: "return=representation"
    },
    body: JSON.stringify([{
      practice_code: practiceCode,
      practice_postcode: postcode || null,
      status: "queued",
      triggered_by: "edge-function"
    }])
  });
  if (!insert.ok) {
    return json({ error: "Failed to create run", detail: (await insert.text()).slice(0, 400) }, 500);
  }
  const [run] = await insert.json();

  const dispatch = await fetch(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
    {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${GITHUB_PAT}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        ref: GITHUB_BRANCH,
        inputs: {
          practice_code: practiceCode,
          postcode,
          run_id: run.id,
          triggered_by: "edge-function"
        }
      })
    }
  );

  if (!dispatch.ok) {
    const detail = (await dispatch.text()).slice(0, 400);
    await fetch(`${SUPABASE_URL}/rest/v1/practice_ingestion_runs?id=eq.${run.id}`, {
      method: "PATCH",
      headers: {
        apikey: SERVICE_ROLE,
        Authorization: `Bearer ${SERVICE_ROLE}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ status: "failed", error_message: `Dispatch failed: ${detail}` })
    });
    return json({ error: "GitHub dispatch failed", detail, run_id: run.id }, 502);
  }

  return json({
    ok: true,
    run_id: run.id,
    practice_code: practiceCode,
    status: "queued"
  });
});
