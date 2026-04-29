create table if not exists public.gp_practice_prescribing_summary (
  practice_code text not null,
  practice_name text null,
  year_month date not null,
  publication_label text null,
  bnf_chapter text not null default 'ALL',
  bnf_chapter_name text null,
  metric_key text not null,
  metric_value numeric null,
  metric_value_text text null,
  source_csv_url text null,
  source_csv_name text null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_prescribing_summary_pkey primary key (
    practice_code,
    year_month,
    bnf_chapter,
    metric_key
  )
);

create index if not exists idx_gp_prescribing_summary_practice
  on public.gp_practice_prescribing_summary using btree (practice_code);

create index if not exists idx_gp_prescribing_summary_month
  on public.gp_practice_prescribing_summary using btree (year_month);

create index if not exists idx_gp_prescribing_summary_metric
  on public.gp_practice_prescribing_summary using btree (metric_key);
