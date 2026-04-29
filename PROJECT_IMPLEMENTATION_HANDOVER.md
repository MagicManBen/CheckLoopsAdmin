# CheckLoops Admin - Full Implementation Handover

Last updated: 2026-04-29

## 1) Purpose of this document

This file is a full handover and implementation log for the CheckLoops Admin repo.  
It explains what was requested, what was built, how it works, where each part lives in the repo, and what to do next.

## 2) Project context and overall goal

The project objective is to run public NHS/GP data imports remotely in GitHub (GitHub Actions), then upsert into Supabase, without local CSV processing on a MacBook.

The system now supports three separate import paths:

1. GP patient age-count import
2. GP workforce practice metrics import (high-level practice CSV only)
3. GP Patient Survey (GPPS) practice metrics import

All imports are triggered from a GitHub Pages dashboard and executed in GitHub Actions using Node scripts.

## 3) Current architecture

### UI / control plane

- `index.html`
- Hosted on GitHub Pages
- Used to:
  - Trigger workflow dispatches via GitHub API
  - Poll run status/history
  - Check Supabase table arrival snapshots

### Execution plane

- GitHub Actions workflows under `.github/workflows/`
- Node importer scripts under `scripts/`
- Each workflow passes inputs and secrets into one script

### Data plane

- Supabase project (existing)
- Writes performed through PostgREST with service role key
- Upserts use explicit `on_conflict` keys

## 4) What existed first in this repo

Initial repo baseline:

- GitHub Pages deployment workflow
- One patient-count import workflow
- One patient-count import script
- Basic admin page, later expanded into a dashboard

## 5) Implemented pipelines

## 5.1 Patient-count import (existing path, enhanced)

### Workflow

- `.github/workflows/import-one-gp.yml`

### Script

- `scripts/import_one_gp_patient_counts.mjs`

### Input behavior

- Takes GP practice M/ODS code (example `M83076`)
- Takes male/female CSV URLs (now user-editable in UI)
- Downloads CSVs in workflow runtime, parses, filters to one practice

### Target table

- `gp_practice_patient_age_counts` (existing table)

### Upsert conflict key

- `practice_code,extract_date,sex,age`

### Notes

- Workflow preflight checks for required secrets/environment
- CSV URL override support added so publication URL changes are easy

## 5.2 GP workforce import (new separate path)

### Workflow

- `.github/workflows/import-gp-workforce.yml`

### Script

- `scripts/import_gp_workforce.mjs`

### Table SQL

- `sql/create_gp_practice_workforce_metrics.sql`

### Target table

- `gp_practice_workforce_metrics`

### Imported dataset (current phase)

- GP workforce **practice ZIP**
- Extracts **high-level practice CSV** from ZIP
- Imports for one practice code (for controlled testing)
- Individual-level workforce ZIP intentionally not imported yet

### Table shape

- `practice_code`
- `practice_name`
- `snapshot_date`
- `publication_label`
- `staff_group`
- `detailed_staff_role`
- `measure`
- `value`
- `source_practice_zip_url`
- `source_csv_name`
- `imported_at`

### Upsert conflict key

- `practice_code,snapshot_date,staff_group,detailed_staff_role,measure`

### Source values confirmed during build

From high-level CSV:

- `STAFF_GROUP` values:
  - `Admin/Non-Clinical`
  - `Direct Patient Care`
  - `GP`
  - `Nurses`
- `MEASURE` values:
  - `FTE`
  - `Headcount`
- `DETAILED_STAFF_ROLE`:
  - 62 distinct values (includes `Total`, role-specific titles)

## 5.3 GP Patient Survey import (new separate path)

### Workflow

- `.github/workflows/import-gp-patient-survey.yml`

### Script

- `scripts/import_gp_patient_survey.mjs`

### Table SQL

- `sql/create_gp_practice_patient_survey_metrics.sql`

### Target table

- `gp_practice_patient_survey_metrics`

### Source

- GPPS weighted public practice CSV:
- `https://gp-patient.co.uk/FileDownload/Download?fileRedirect=2025%2Fsurvey-results%2Fpractice-results%2Fpractice-data-csv%2FGPPS_2025_Practice_data_(weighted)_(csv)_PUBLIC.csv`

### Source structure observed

- Wide CSV (1105 columns in sampled file)
- Base identifiers + many metric columns
- Example base columns:
  - `ad_practicecode`
  - `ad_practicename`
  - ICS/region/PCN columns
  - `distributed`, `received`, `resprate`

### How data is stored

The script normalizes wide columns into a tall metric table:

- One row per `(practice_code, survey_year, metric_key)`
- `metric_key` = original GPPS column name
- Numeric values saved in `metric_value`
- Non-numeric values saved in `metric_value_text`

### Upsert conflict key

- `practice_code,survey_year,metric_key`

## 6) Dashboard expansion details

`index.html` now has three isolated sections:

1. Patient Count Import
2. Workforce Metric Import
3. GP Patient Survey Import

Each section includes:

- Dedicated workflow dispatch button
- Dedicated run refresh table
- Dedicated run watcher (polling)
- Dedicated Supabase data-arrival check cards/status

Shared configuration at top:

- GitHub owner/repo/branch
- GitHub token for Actions API
- Supabase URL + anon/publishable key for read checks

## 7) Workflow and secret requirements

Repo Actions secrets required:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Fallback secret names are supported in some workflows (`SUPABASE_SECRET_KEY`, `SUPABASE_KEY`, etc.), but standard names above are recommended.

## 8) Important implementation/ops notes

## 8.1 Workflow-scope push limitation

A recurring GitHub limitation occurred when pushing new files in `.github/workflows/`:

- Push rejected without token permissions for workflow updates
- Resolution used: push via PAT that includes workflow update scope

## 8.2 Source URL change handling

All import paths now accept source URLs as workflow/UI inputs, so publication URL changes do not require code edits.

## 8.3 Data model separation

The pipelines are separated by table and workflow:

- Patient counts are not mixed with workforce
- Workforce is not mixed with GPPS
- Individual workforce remains future-phase only

## 9) SQL files in repo

- `sql/create_gp_practice_workforce_metrics.sql`
- `sql/create_gp_practice_patient_survey_metrics.sql`

Run these in Supabase SQL Editor before first use of each corresponding import workflow.

## 10) Commit timeline in this repo

Recent commit progression:

- `65ace65` Initial GitHub Pages admin + Actions Supabase importer
- `f9b4f20` Improved secret handling in patient workflow
- `805e8f2` Upgraded UI to dashboard with monitoring
- `ec2accf` Added configurable patient CSV URLs and explicit M-code input
- `bac7b9a` Added separate workforce practice-metric import path
- `255f6a5` Added separate GP Patient Survey import path

## 11) Current status summary

Implemented and wired:

- Patient import path
- Workforce (practice-high-level) path
- GPPS path
- Dashboard controls for all three
- Run monitoring and table-arrival checks for all three

Pending / future-phase ideas:

- Workforce individual-level import (if approved)
- Optional richer GPPS metric grouping metadata
- Optional scheduled runs (cron/manual governance)
- Optional stronger validation/range checks per dataset

## 12) Quick runbook

1. Ensure Supabase tables exist by running SQL files under `sql/`.
2. Ensure repo secrets exist: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`.
3. Open GitHub Pages dashboard.
4. Enter GitHub token with Actions write permission.
5. Pick import section (Patient / Workforce / GPPS), set practice and URLs, run import.
6. Watch run status in dashboard.
7. Use corresponding data-check panel to confirm rows arrived.
