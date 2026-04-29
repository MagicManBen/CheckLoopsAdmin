create table if not exists public.gp_practice_geography (
  practice_code text not null,
  postcode text not null,
  postcode_normalised text null,
  outcode text null,
  incode text null,
  country text null,
  country_code text null,
  region text null,
  region_code text null,
  local_authority_district text null,
  local_authority_district_code text null,
  ward text null,
  ward_code text null,
  parish text null,
  parish_code text null,
  parliamentary_constituency text null,
  parliamentary_constituency_code text null,
  ccg text null,
  ccg_code text null,
  nhs_ha text null,
  nhs_ha_code text null,
  lsoa text null,
  lsoa_code text null,
  msoa text null,
  msoa_code text null,
  oa_code text null,
  latitude numeric null,
  longitude numeric null,
  eastings integer null,
  northings integer null,
  source text null,
  source_url text null,
  raw_payload jsonb null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_geography_pkey primary key (practice_code)
);

create index if not exists idx_gp_practice_geography_postcode
  on public.gp_practice_geography using btree (postcode_normalised);

create index if not exists idx_gp_practice_geography_lsoa
  on public.gp_practice_geography using btree (lsoa_code);

create index if not exists idx_gp_practice_geography_la
  on public.gp_practice_geography using btree (local_authority_district_code);
