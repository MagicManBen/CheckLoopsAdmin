create table if not exists public.gp_practice_patient_survey_metrics (
  practice_code text not null,
  practice_name text null,
  survey_year integer not null,
  publication_label text null,
  ics_code text null,
  ics_name text null,
  ics_code_ons text null,
  commissioning_region_code text null,
  commissioning_region_name text null,
  pcn_code text null,
  pcn_name text null,
  distributed numeric null,
  received numeric null,
  response_rate numeric null,
  metric_key text not null,
  metric_value numeric null,
  metric_value_text text null,
  source_csv_url text null,
  source_csv_name text null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_patient_survey_metrics_pkey primary key (
    practice_code,
    survey_year,
    metric_key
  )
);

create index if not exists idx_gp_patient_survey_practice
on public.gp_practice_patient_survey_metrics using btree (practice_code);

create index if not exists idx_gp_patient_survey_year
on public.gp_practice_patient_survey_metrics using btree (survey_year);

create index if not exists idx_gp_patient_survey_metric_key
on public.gp_practice_patient_survey_metrics using btree (metric_key);
