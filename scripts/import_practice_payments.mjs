import fs from "node:fs";
import path from "node:path";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CSV_URL = process.env.PAYMENTS_CSV_URL || "https://files.digital.nhs.uk/D0/C6D126/nhspaymentsgp-24-25-prac-csv.csv";
const PAYMENT_YEAR = process.env.PAYMENT_YEAR || "2024-25";
const TABLE = "gp_practice_payments";
const BATCH_SIZE = 200;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing Supabase env vars");

const DATA_DIR = path.resolve("data");
fs.mkdirSync(DATA_DIR, { recursive: true });

function parseCSVLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (ch === "," && !inQ) { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur);
  return out;
}

function num(v) {
  if (v == null || v === "" || v === "-") return null;
  const cleaned = String(v).replace(/[£,]/g, "").trim();
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function dateOrNull(v) {
  if (!v) return null;
  // Handle UK format DD/MM/YYYY
  const m = String(v).match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  return null;
}

function strOrNull(v) {
  if (v == null || v === "") return null;
  return String(v).trim();
}

async function downloadCsv() {
  const csvPath = path.join(DATA_DIR, "nhs_payments.csv");
  if (fs.existsSync(csvPath)) {
    console.log(`Using cached CSV: ${csvPath}`);
    return csvPath;
  }
  console.log(`Downloading ${CSV_URL}`);
  const res = await fetch(CSV_URL);
  if (!res.ok) throw new Error(`CSV download failed: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(csvPath, buf);
  console.log(`Saved ${buf.length} bytes`);
  return csvPath;
}

async function upsert(rows) {
  const url = `${SUPABASE_URL}/rest/v1/${TABLE}?on_conflict=practice_code,payment_year`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(rows),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Upsert failed (${res.status}): ${body.slice(0, 600)}`);
  }
  return rows.length;
}

async function main() {
  const csvPath = await downloadCsv();
  const content = fs.readFileSync(csvPath, "utf-8").replace(/^﻿/, "");
  const lines = content.split(/\r?\n/);
  // Headers may span multiple lines if CSV has multiline column names; the file has 65 columns
  // First non-empty line is the header
  let headerLine = lines.shift();
  while (lines.length && headerLine.split(",").length < 60) {
    headerLine += " " + lines.shift();
  }
  const headers = parseCSVLine(headerLine).map((h) => h.trim().replace(/^"|"$/g, ""));
  console.log(`Headers (${headers.length}):`);
  headers.forEach((h, i) => console.log(`  ${i}: ${h}`));

  // Helper to get index by header (fuzzy)
  const idx = (needle) => {
    const n = needle.toLowerCase().replace(/[^a-z0-9]/g, "");
    return headers.findIndex((h) => h.toLowerCase().replace(/[^a-z0-9]/g, "").includes(n));
  };

  const cols = {
    region_code: idx("nhsenglandregioncode"),
    region_name: idx("nhsenglandregionname"),
    sub_icb_code: idx("subicbcode"),
    sub_icb_name: idx("subicbname"),
    pcn_code: idx("pcncode"),
    pcn_name: idx("pcnname"),
    practice_code: idx("practicecode"),
    practice_name: idx("practicename"),
    practice_address: idx("practiceaddress"),
    practice_postcode: idx("practicepostcode"),
    practice_open_date: idx("practiceopendate"),
    practice_close_date: idx("practiceclosedate"),
    contract_type: idx("contracttype"),
    dispensing_practice: idx("dispensingpractice"),
    practice_type: headers.findIndex((h) => h.toLowerCase().replace(/[^a-z0-9]/g, "") === "practicetype"),
    practice_rurality: idx("practicerurality"),
    atypical_characteristics: idx("atypicalcharacteristics"),
    avg_registered_patients: idx("averagenumberofregisteredpatients"),
    avg_weighted_patients: idx("averagenumberofweightedpatients"),
    avg_payments_per_registered_patient: headers.findIndex((h) => /average payments per registered patient/i.test(h) && !/covid/i.test(h)),
    avg_payments_per_weighted_patient: headers.findIndex((h) => /average payments per weighted patient/i.test(h) && !/covid/i.test(h)),
    global_sum: idx("globalsum"),
    mpig_correction_factor: idx("mpigcorrectionfactor"),
    balance_of_pms_expenditure: idx("balanceofpmsexpenditure"),
    total_qof_payments: idx("totalqofpayments"),
    childhood_vacc_imm_scheme: idx("childhoodvaccinationandimmunisationscheme"),
    influenza_pneumococcal_imm: idx("influenzaandpneumococcalimmunisations"),
    learning_disabilities: idx("learningdisabilities"),
    meningitis: idx("meningitis"),
    minor_surgery: idx("minorsurgery"),
    out_of_area_in_hours_urgent_care: idx("outofareainhoursurgentcare"),
    pertussis: idx("pertussis"),
    rotavirus_shingles_imm: idx("rotavirusandshinglesimmunisation"),
    services_for_violent_patients: idx("servicesforviolentpatients"),
    medical_assessment_reviews: idx("medicalassessmentreviews"),
    weight_management_service: idx("weightmanagementservice"),
    local_incentive_schemes: idx("localincentiveschemes"),
    gp_extended_hours_access: idx("gpextendedhoursaccess"),
    premises_payments: idx("premisespayments"),
    seniority: headers.findIndex((h) => h.toLowerCase().replace(/[^a-z0-9]/g, "") === "seniority"),
    doctors_retainer_scheme: idx("doctorsretainerscheme"),
    total_locum_allowances: idx("totallocumallowances"),
    appraiser_costs_locums: idx("appraiserappraisercosts"),
    prolonged_study_leave: idx("prolongedstudyleave"),
    pco_admin_other: idx("pcoadminother"),
    information_management_technology: idx("informationmanagementandtechnology"),
    non_des_pneumococcal_childhood: idx("nondesitempneumococcalvaccine"),
    rsv: idx("respiratorysyncytial"),
    general_practice_transformation: idx("generalpracticetransformation"),
    pcn_participation: idx("pcnparticipation"),
    prescribing_fee_payments: idx("prescribingfeepayments"),
    dispensing_fee_payments: idx("dispensingfeepayments"),
    reimbursement_of_drugs: idx("reimbursementofdrugs"),
    winter_access_fund: idx("winteraccessfund"),
    other_payments: headers.findIndex((h) => h.toLowerCase().replace(/[^a-z0-9]/g, "") === "otherpayments"),
    covid_immunisation: idx("covidimmunisation"),
    covid_support_and_expansion: idx("covidsupport"),
    long_covid: idx("longcovid"),
    total_nhs_payments: headers.findIndex((h) => /^total nhs payments to general practice$/i.test(h)),
    deductions_pensions_levies_prescriptions: idx("deductionsforpensions"),
    total_nhs_payments_minus_deductions: headers.findIndex((h) => /total nhs payments to general practice minus deductions/i.test(h) && !/covid/i.test(h)),
    total_nhs_payments_including_covid: headers.findIndex((h) => /total nhs payments to general practice including covid vaccination/i.test(h) && !/minus/i.test(h)),
    total_nhs_payments_including_covid_minus_deductions: headers.findIndex((h) => /total nhs payments to general practice including covid minus deductions/i.test(h)),
    avg_payments_per_registered_patient_including_covid: headers.findIndex((h) => /average payments per registered patient including covid/i.test(h)),
    avg_payments_per_weighted_patient_including_covid: headers.findIndex((h) => /average payments per weighted patient including covid/i.test(h)),
  };

  // Log any unmapped columns
  for (const [k, v] of Object.entries(cols)) {
    if (v === -1) console.warn(`  WARN: column not found for "${k}"`);
  }

  let batch = [];
  let total = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    const fields = parseCSVLine(line);
    const practice_code = strOrNull(fields[cols.practice_code]);
    if (!practice_code || !/^[A-Z][0-9]{5,}/i.test(practice_code)) continue;

    const row = {
      practice_code: practice_code.toUpperCase(),
      payment_year: PAYMENT_YEAR,
      region_code: strOrNull(fields[cols.region_code]),
      region_name: strOrNull(fields[cols.region_name]),
      sub_icb_code: strOrNull(fields[cols.sub_icb_code]),
      sub_icb_name: strOrNull(fields[cols.sub_icb_name]),
      pcn_code: strOrNull(fields[cols.pcn_code]),
      pcn_name: strOrNull(fields[cols.pcn_name]),
      practice_name: strOrNull(fields[cols.practice_name]),
      practice_address: strOrNull(fields[cols.practice_address]),
      practice_postcode: strOrNull(fields[cols.practice_postcode]),
      practice_open_date: dateOrNull(fields[cols.practice_open_date]),
      practice_close_date: dateOrNull(fields[cols.practice_close_date]),
      contract_type: strOrNull(fields[cols.contract_type]),
      dispensing_practice: strOrNull(fields[cols.dispensing_practice]),
      practice_type: strOrNull(fields[cols.practice_type]),
      practice_rurality: strOrNull(fields[cols.practice_rurality]),
      atypical_characteristics: strOrNull(fields[cols.atypical_characteristics]),
      avg_registered_patients: num(fields[cols.avg_registered_patients]),
      avg_weighted_patients: num(fields[cols.avg_weighted_patients]),
      avg_payments_per_registered_patient: num(fields[cols.avg_payments_per_registered_patient]),
      avg_payments_per_weighted_patient: num(fields[cols.avg_payments_per_weighted_patient]),
      global_sum: num(fields[cols.global_sum]),
      mpig_correction_factor: num(fields[cols.mpig_correction_factor]),
      balance_of_pms_expenditure: num(fields[cols.balance_of_pms_expenditure]),
      total_qof_payments: num(fields[cols.total_qof_payments]),
      childhood_vacc_imm_scheme: num(fields[cols.childhood_vacc_imm_scheme]),
      influenza_pneumococcal_imm: num(fields[cols.influenza_pneumococcal_imm]),
      learning_disabilities: num(fields[cols.learning_disabilities]),
      meningitis: num(fields[cols.meningitis]),
      minor_surgery: num(fields[cols.minor_surgery]),
      out_of_area_in_hours_urgent_care: num(fields[cols.out_of_area_in_hours_urgent_care]),
      pertussis: num(fields[cols.pertussis]),
      rotavirus_shingles_imm: num(fields[cols.rotavirus_shingles_imm]),
      services_for_violent_patients: num(fields[cols.services_for_violent_patients]),
      medical_assessment_reviews: num(fields[cols.medical_assessment_reviews]),
      weight_management_service: num(fields[cols.weight_management_service]),
      local_incentive_schemes: num(fields[cols.local_incentive_schemes]),
      gp_extended_hours_access: num(fields[cols.gp_extended_hours_access]),
      premises_payments: num(fields[cols.premises_payments]),
      seniority: num(fields[cols.seniority]),
      doctors_retainer_scheme: num(fields[cols.doctors_retainer_scheme]),
      total_locum_allowances: num(fields[cols.total_locum_allowances]),
      appraiser_costs_locums: num(fields[cols.appraiser_costs_locums]),
      prolonged_study_leave: num(fields[cols.prolonged_study_leave]),
      pco_admin_other: num(fields[cols.pco_admin_other]),
      information_management_technology: num(fields[cols.information_management_technology]),
      non_des_pneumococcal_childhood: num(fields[cols.non_des_pneumococcal_childhood]),
      rsv: num(fields[cols.rsv]),
      general_practice_transformation: num(fields[cols.general_practice_transformation]),
      pcn_participation: num(fields[cols.pcn_participation]),
      prescribing_fee_payments: num(fields[cols.prescribing_fee_payments]),
      dispensing_fee_payments: num(fields[cols.dispensing_fee_payments]),
      reimbursement_of_drugs: num(fields[cols.reimbursement_of_drugs]),
      winter_access_fund: num(fields[cols.winter_access_fund]),
      other_payments: num(fields[cols.other_payments]),
      covid_immunisation: num(fields[cols.covid_immunisation]),
      covid_support_and_expansion: num(fields[cols.covid_support_and_expansion]),
      long_covid: num(fields[cols.long_covid]),
      total_nhs_payments: num(fields[cols.total_nhs_payments]),
      deductions_pensions_levies_prescriptions: num(fields[cols.deductions_pensions_levies_prescriptions]),
      total_nhs_payments_minus_deductions: num(fields[cols.total_nhs_payments_minus_deductions]),
      total_nhs_payments_including_covid: num(fields[cols.total_nhs_payments_including_covid]),
      total_nhs_payments_including_covid_minus_deductions: num(fields[cols.total_nhs_payments_including_covid_minus_deductions]),
      avg_payments_per_registered_patient_including_covid: num(fields[cols.avg_payments_per_registered_patient_including_covid]),
      avg_payments_per_weighted_patient_including_covid: num(fields[cols.avg_payments_per_weighted_patient_including_covid]),
      source: `NHS Digital — NHS Payments to General Practice ${PAYMENT_YEAR}`,
      source_url: CSV_URL,
    };

    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      total += await upsert(batch);
      process.stdout.write(`\r  Upserted ${total}...`);
      batch = [];
    }
  }
  if (batch.length) total += await upsert(batch);
  console.log(`\nDone. Imported ${total} practices for year ${PAYMENT_YEAR}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
