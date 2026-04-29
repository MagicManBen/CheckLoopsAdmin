# CheckLoops Admin - Full Implementation Handover

Last updated: 2026-04-29

## 1) Purpose of this document

This file is a full handover and implementation log for the CheckLoops Admin repo.  
It explains what was requested, what was built, how it works, where each part lives in the repo, and what to do next.

## 2) Project context and overall goal

The project objective is to run public NHS/GP data imports remotely in GitHub (GitHub Actions), then upsert into Supabase, without local CSV processing on a MacBook.

The system now supports eight separate import paths:

1. GP patient age-count import
2. GP workforce practice metrics import (high-level practice CSV only)
3. GP Patient Survey (GPPS) practice metrics import
4. CQC public profile import (per practice — ratings, last inspection, registered activities)
5. NHS.uk GP surgery public profile import (per practice — address, opening times, accepting patients)
6. Practice-level prescribing summary import (per practice, per month)
7. English Indices of Deprivation (LSOA) bulk import (any release year)
8. ONS geography (postcode → LSOA/MSOA/ward/LA/region) import (per practice)

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

## 5.4 CQC public profile import (new path)

### Workflow
- `.github/workflows/import-cqc-profile.yml`

### Script
- `scripts/import_cqc_profile.mjs`

### Table SQL
- `sql/create_gp_practice_cqc_profile.sql`

### Target table
- `gp_practice_cqc_profile` (PK = `practice_code`)

### Source
- CQC public API (default base `https://api.service.cqc.org.uk/public/v1`)
- Endpoints used:
  - `GET /locations?odsCode={ODS}` — to resolve a CQC location ID from a practice ODS code (skipped if `cqc_location_id` is supplied)
  - `GET /locations/{locationId}` — full location detail
  - `GET /providers/{providerId}` — derived from the location's providerId

### Stored fields
- Identifiers: `practice_code`, `location_id`, `provider_id`
- Names: `location_name`, `provider_name`
- Registration: `registration_status`, `registration_date`, `deregistration_date`, `type`
- Geography: `postal_code`, `region`, `local_authority`, `constituency`
- Inspection: `last_inspection_date`, `last_report_publication_date`
- Ratings (parsed from `currentRatings.overall.keyQuestionRatings`): `overall_rating`, `safe_rating`, `effective_rating`, `caring_rating`, `responsive_rating`, `well_led_rating`
- JSONB extras: `registered_activities`, `gac_service_types`, `inspection_categories`, `specialisms`, `inspection_areas`, `current_ratings`, `historic_ratings`, `reports`, `raw_payload`
- Provenance: `source_location_url`, `source_provider_url`, `imported_at`

### Required secret
- `CQC_SUBSCRIPTION_KEY` (Ocp-Apim-Subscription-Key for CQC's APIM tenant)

## 5.5 NHS.uk GP profile import (new path)

### Workflow
- `.github/workflows/import-nhs-profile.yml`

### Script
- `scripts/import_nhs_profile.mjs`

### Table SQL
- `sql/create_gp_practice_nhs_profile.sql`

### Target table
- `gp_practice_nhs_profile` (PK = `practice_code`)

### Source
- NHS service-search API at `https://api.nhs.uk/service-search/search?api-version=2&search={ODS}&searchFields=OrganisationCode,ODSCode`
- Picks the row whose `OrganisationCode`/`ODSCode` matches the requested ODS exactly (falls back to first result)

### Stored fields
- Identification + address: `organisation_name`, `organisation_type`, `parent_organisation`, `address_line_1..3`, `town`, `county`, `postcode`, `country`
- Contact: `phone`, `fax`, `email`, `website`, `latitude`, `longitude`
- Patient access: `accepting_new_patients`, `online_booking_url`, `prescription_ordering_url`, `appointment_booking_url`
- JSONB extras: `opening_times`, `reception_times`, `consulting_times`, `facilities`, `accessibility`, `services`, `staff`, `metrics`, `raw_payload`
- Provenance: `source_url`, `source_api`, `imported_at`

### Required secret
- `NHS_API_KEY` (Ocp-Apim-Subscription-Key for api.nhs.uk)

## 5.6 Practice-level prescribing summary import (new path)

### Workflow
- `.github/workflows/import-prescribing-summary.yml`

### Script
- `scripts/import_prescribing_summary.mjs`

### Table SQL
- `sql/create_gp_practice_prescribing_summary.sql`

### Target table
- `gp_practice_prescribing_summary`
- PK: `(practice_code, year_month, bnf_chapter, metric_key)`

### Source
- Direct CSV URL from the NHSBSA / digital.nhs.uk practice-level prescribing summary publication. URL is supplied as a workflow input (no fixed default — it changes monthly).

### Behaviour
- Streams CSV line-by-line, filters to one `practice_code`, normalises wide columns to a tall `metric_key` / `metric_value` shape (mirrors the GPPS approach).
- Auto-detects the practice code header from a candidate list (`PRACTICE_CODE`, `PRAC_CODE`, `ODS_CODE`, etc.) so it tolerates the slight schema drift between NHSBSA monthly files.
- Auto-detects optional BNF chapter columns; otherwise stores `bnf_chapter = 'ALL'`.
- Numeric vs text handled identically to GPPS.

### Required input
- `practice_code`, `year_month` (must be `YYYY-MM-01`), `publication_label`, `prescribing_csv_url`

## 5.7 English Indices of Deprivation (LSOA) import (new path)

### Workflow
- `.github/workflows/import-imd-lsoa.yml`

### Script
- `scripts/import_imd_lsoa.mjs`

### Table SQL
- `sql/create_imd_lsoa_metrics.sql`

### Target table
- `imd_lsoa_metrics`
- PK: `(lsoa_code, imd_year, metric_key)`

### Source
- Any LSOA-level IMD CSV (gov.uk publication). URL is supplied as a workflow input. Designed to handle both 2019-style and 2025-style headers without code changes.

### Behaviour
- Auto-detects LSOA code/name, local-authority code/name and region code/name columns from a candidate list (`LSOA_CODE_2021`, `LSOA11CD`, `LSOA21CD`, `LADCD`, etc.).
- Stores everything else (all IMD scores, ranks, deciles, sub-domains) as tall `metric_key` / `metric_value` rows — robust to whatever shape the chosen release uses.
- Bulk upsert in 2000-row batches.

### Required input
- `imd_year`, `publication_label`, `imd_csv_url`

### Joinability
- Once both `gp_practice_geography.lsoa_code` and `imd_lsoa_metrics.lsoa_code` are populated, you can join practice → LSOA → IMD to derive a deprivation profile per practice.

## 5.8 ONS geography (postcode lookup) import (new path)

### Workflow
- `.github/workflows/import-ons-geography.yml`

### Script
- `scripts/import_ons_geography.mjs`

### Table SQL
- `sql/create_gp_practice_geography.sql`

### Target table
- `gp_practice_geography` (PK = `practice_code`)

### Source
- `https://api.postcodes.io/postcodes/{normalisedPostcode}` (postcodes.io serves the ONS postcode reference data without needing a key).

### Stored fields
- Identification: `practice_code`, `postcode`, `postcode_normalised`, `outcode`, `incode`
- Geography labels and codes (paired): country / region / local_authority_district / ward / parish / parliamentary_constituency / ccg / nhs_ha / lsoa / msoa / oa
- Coordinates: `latitude`, `longitude`, `eastings`, `northings`
- Provenance: `source`, `source_url`, `raw_payload`, `imported_at`

### Required input
- `practice_code`, `postcode`, `postcodes_api_base`

## 6) Dashboard expansion details

`index.html` now has eight isolated import sections:

1. Patient Count Import
2. Workforce Metric Import
3. GP Patient Survey Import
4. CQC Profile Import
5. NHS.uk GP Profile Import
6. Practice Prescribing Summary Import
7. English Indices of Deprivation (LSOA) Import
8. ONS Geography (Postcode Lookup) Import

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
- `CQC_SUBSCRIPTION_KEY` — required for the CQC profile workflow only
- `NHS_API_KEY` — required for the NHS.uk profile workflow only

Fallback secret names are supported in some workflows (`SUPABASE_SECRET_KEY`, `SUPABASE_KEY`, etc.), but standard names above are recommended.

To register for the third-party API keys:

- CQC: subscribe to the CQC Public API on the CQC API portal (Azure APIM). Add the resulting subscription key as `CQC_SUBSCRIPTION_KEY`.
- NHS: subscribe to the NHS service-search APIM product on developer.api.nhs.uk. Add the resulting subscription key as `NHS_API_KEY`.

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
- `sql/create_gp_practice_cqc_profile.sql`
- `sql/create_gp_practice_nhs_profile.sql`
- `sql/create_gp_practice_prescribing_summary.sql`
- `sql/create_imd_lsoa_metrics.sql`
- `sql/create_gp_practice_geography.sql`

Run these in Supabase SQL Editor before first use of each corresponding import workflow.

## 10) Commit timeline in this repo

Recent commit progression:

- `65ace65` Initial GitHub Pages admin + Actions Supabase importer
- `f9b4f20` Improved secret handling in patient workflow
- `805e8f2` Upgraded UI to dashboard with monitoring
- `ec2accf` Added configurable patient CSV URLs and explicit M-code input
- `bac7b9a` Added separate workforce practice-metric import path
- `255f6a5` Added separate GP Patient Survey import path
- `(next)` Added five public-data import paths: CQC profile, NHS.uk profile, prescribing summary, IMD (LSOA), ONS geography

## 11) Current status summary

Implemented and wired:

- Patient import path
- Workforce (practice-high-level) path
- GPPS path
- CQC profile path
- NHS.uk profile path
- Prescribing summary path
- IMD (LSOA) bulk path
- ONS geography (per-practice) path
- Dashboard controls for all eight
- Run monitoring and table-arrival checks for all eight

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
