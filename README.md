# CheckLoops Admin

GitHub-hosted admin tool to run GP patient-count imports without downloading CSVs to your local machine.

## What this repo does

- Hosts a browser page on GitHub Pages (`index.html`).
- Includes JS server/import code in-repo (`scripts/import_one_gp_patient_counts.mjs`).
- Includes GP workforce import code (`scripts/import_gp_workforce.mjs`).
- Runs import fully in GitHub Actions (`.github/workflows/import-one-gp.yml`).
- Runs workforce import fully in GitHub Actions (`.github/workflows/import-gp-workforce.yml`).

## One-time setup

1. In GitHub repo settings, add repository secrets:
   - `SUPABASE_URL` = `https://kvrcmqpwdkfiqemybmkc.supabase.co`
   - `SUPABASE_SERVICE_ROLE_KEY` = your service role key
2. In repo Settings -> Pages, set Source to **GitHub Actions**.
3. Push to `main` (this repo already includes `.github/workflows/deploy-pages.yml` for deployment).

## Use it

1. Open your GitHub Pages URL for this repo.
2. Fill in owner/repo/branch, GP practice M/ODS code (for example `M83076`), and the latest male/female NHS CSV URLs.
3. Paste a GitHub token that has `actions:write` for this repository.
4. Click **Run Import Workflow**.
5. Check GitHub Actions for progress and logs.

## Workforce import (practice-high-level only)

Current implemented workforce path imports from the **high-level practice CSV** inside:

`https://files.digital.nhs.uk/85/7F0D90/GPWPracticeCSV.032026.zip`

Target table:

- `public.gp_practice_workforce_metrics`

Create table SQL:

- `sql/create_gp_practice_workforce_metrics.sql`

Conflict key used by upsert:

- `practice_code,snapshot_date,staff_group,detailed_staff_role,measure`

Notes:

- This phase imports practice-high-level workforce metrics only.
- Individual-level workforce CSV is intentionally not imported yet (future phase).

## Notes

- CSV files are downloaded during the action run and processed there.
- Upsert conflict key is `practice_code,extract_date,sex,age`.
- The page dispatches a workflow only; all import logic runs in GitHub.
