create table if not exists public.gp_practice_cqc_profile (
  practice_code text not null,
  location_id text null,
  provider_id text null,
  location_name text null,
  provider_name text null,
  type text null,
  registration_status text null,
  registration_date date null,
  deregistration_date date null,
  postal_code text null,
  region text null,
  local_authority text null,
  constituency text null,
  number_of_beds integer null,
  registered_manager_absent boolean null,
  last_inspection_date date null,
  last_report_publication_date date null,
  overall_rating text null,
  safe_rating text null,
  effective_rating text null,
  caring_rating text null,
  responsive_rating text null,
  well_led_rating text null,
  registered_activities jsonb null,
  gac_service_types jsonb null,
  inspection_categories jsonb null,
  specialisms jsonb null,
  inspection_areas jsonb null,
  current_ratings jsonb null,
  historic_ratings jsonb null,
  reports jsonb null,
  source_location_url text null,
  source_provider_url text null,
  raw_payload jsonb null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_cqc_profile_pkey primary key (practice_code)
);

create index if not exists idx_gp_practice_cqc_profile_location
  on public.gp_practice_cqc_profile using btree (location_id);

create index if not exists idx_gp_practice_cqc_profile_provider
  on public.gp_practice_cqc_profile using btree (provider_id);

create index if not exists idx_gp_practice_cqc_profile_postcode
  on public.gp_practice_cqc_profile using btree (postal_code);
