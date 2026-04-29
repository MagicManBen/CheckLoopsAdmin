# CheckLoops Admin

GitHub-hosted admin tool to run GP patient-count imports without downloading CSVs to your local machine.

## What this repo does

- Hosts a browser page on GitHub Pages (`index.html`).
- Includes JS server/import code in-repo (`scripts/import_one_gp_patient_counts.mjs`).
- Runs import fully in GitHub Actions (`.github/workflows/import-one-gp.yml`).

## One-time setup

1. In GitHub repo settings, add repository secrets:
   - `SUPABASE_URL` = `https://kvrcmqpwdkfiqemybmkc.supabase.co`
   - `SUPABASE_SERVICE_ROLE_KEY` = your service role key
2. In repo Settings -> Pages, set Source to **GitHub Actions**.
3. Push to `main` (this repo already includes `.github/workflows/deploy-pages.yml` for deployment).

## Use it

1. Open your GitHub Pages URL for this repo.
2. Fill in owner/repo/branch/ODS code.
3. Paste a GitHub token that has `actions:write` for this repository.
4. Click **Run Import Workflow**.
5. Check GitHub Actions for progress and logs.

## Notes

- CSV files are downloaded during the action run and processed there.
- Upsert conflict key is `practice_code,extract_date,sex,age`.
- The page dispatches a workflow only; all import logic runs in GitHub.
