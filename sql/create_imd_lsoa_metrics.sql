create table if not exists public.imd_lsoa_metrics (
  lsoa_code text not null,
  lsoa_name text null,
  local_authority_code text null,
  local_authority_name text null,
  region_code text null,
  region_name text null,
  imd_year integer not null,
  publication_label text null,
  metric_key text not null,
  metric_value numeric null,
  metric_value_text text null,
  source_csv_url text null,
  source_csv_name text null,
  imported_at timestamp with time zone not null default now(),
  constraint imd_lsoa_metrics_pkey primary key (
    lsoa_code,
    imd_year,
    metric_key
  )
);

create index if not exists idx_imd_lsoa_metrics_lsoa
  on public.imd_lsoa_metrics using btree (lsoa_code);

create index if not exists idx_imd_lsoa_metrics_year
  on public.imd_lsoa_metrics using btree (imd_year);

create index if not exists idx_imd_lsoa_metrics_metric_key
  on public.imd_lsoa_metrics using btree (metric_key);

create index if not exists idx_imd_lsoa_metrics_la
  on public.imd_lsoa_metrics using btree (local_authority_code);
