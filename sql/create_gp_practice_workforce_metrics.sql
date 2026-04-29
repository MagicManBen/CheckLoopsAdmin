create table if not exists public.gp_practice_workforce_metrics (
  practice_code text not null,
  practice_name text null,
  snapshot_date date not null,
  publication_label text null,
  staff_group text not null,
  detailed_staff_role text not null,
  measure text not null,
  value numeric null,
  source_practice_zip_url text null,
  source_csv_name text null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_workforce_metrics_pkey primary key (
    practice_code,
    snapshot_date,
    staff_group,
    detailed_staff_role,
    measure
  )
);

create index if not exists idx_gp_workforce_metrics_practice
on public.gp_practice_workforce_metrics using btree (practice_code);

create index if not exists idx_gp_workforce_metrics_snapshot
on public.gp_practice_workforce_metrics using btree (snapshot_date);

create index if not exists idx_gp_workforce_metrics_group_role
on public.gp_practice_workforce_metrics using btree (staff_group, detailed_staff_role);
