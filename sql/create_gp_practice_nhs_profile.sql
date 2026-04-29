create table if not exists public.gp_practice_nhs_profile (
  practice_code text not null,
  organisation_name text null,
  organisation_type text null,
  parent_organisation text null,
  address_line_1 text null,
  address_line_2 text null,
  address_line_3 text null,
  town text null,
  county text null,
  postcode text null,
  country text null,
  phone text null,
  fax text null,
  email text null,
  website text null,
  latitude numeric null,
  longitude numeric null,
  accepting_new_patients text null,
  accepting_new_patients_updated_at timestamp with time zone null,
  online_booking_url text null,
  prescription_ordering_url text null,
  appointment_booking_url text null,
  opening_times jsonb null,
  reception_times jsonb null,
  consulting_times jsonb null,
  facilities jsonb null,
  accessibility jsonb null,
  services jsonb null,
  staff jsonb null,
  metrics jsonb null,
  source_url text null,
  source_api text null,
  raw_payload jsonb null,
  imported_at timestamp with time zone not null default now(),
  constraint gp_practice_nhs_profile_pkey primary key (practice_code)
);

create index if not exists idx_gp_practice_nhs_profile_postcode
  on public.gp_practice_nhs_profile using btree (postcode);

create index if not exists idx_gp_practice_nhs_profile_org_name
  on public.gp_practice_nhs_profile using btree (organisation_name);
