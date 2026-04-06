package internal

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// sortRowsByCPT sorts rows by cpt_code (nulls first) to match Parquet output order.
func sortRowsByCPT(rows []HospitalChargeRow) {
	slices.SortFunc(rows, func(a, b HospitalChargeRow) int {
		return cmpOptStr(a.CPTCode, b.CPTCode)
	})
}

// writeTallCSV creates a Tall-format CSV test file.
// Includes both payer_name/plan_name AND standard_charge|negotiated_dollar
// columns — the combination that previously triggered a non-deterministic
// Wide detection bug (fixed in detectFormat).
func writeTallCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tall.csv")

	content := `hospital_name,last_updated_on,version,hospital_location,hospital_address
Test General Hospital,2024-01-15,2.0.0,"New York, NY","123 Main St, New York, NY 10001"
description,setting,code|1,code|1|type,code|2,code|2|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|methodology,drug_unit_of_measurement,drug_type_of_measurement,additional_generic_notes,modifiers
ECHOCARDIOGRAM COMPLETE,outpatient,93306,CPT,G0389,HCPCS,1500.00,750.00,500.00,2000.00,Aetna,Aetna PPO,900.00,fee_schedule,,,,
ECHOCARDIOGRAM COMPLETE,outpatient,93306,CPT,G0389,HCPCS,1500.00,750.00,500.00,2000.00,UnitedHealthcare,UHC Choice Plus,1100.00,case_rate,,,,
ACETAMINOPHEN 500MG TABLET,inpatient,00456-0422-01,NDC,,,15.50,8.25,5.00,20.00,Cigna,Cigna Open Access,10.00,fee_schedule,500.0,ME,Oral tablet only,
HEART TRANSPLANT WITH MCC,inpatient,001,MS-DRG,,,500000.00,250000.00,200000.00,750000.00,,,,,,,,26 59
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write tall CSV: %v", err)
	}
	return path
}

// writeWideCSV creates a Wide-format CSV test file.
func writeWideCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "wide.csv")

	content := `hospital_name,last_updated_on,version,hospital_location,hospital_address
Wide Test Hospital,2024-06-01,2.0.0,Brooklyn NY,456 Oak Ave Brooklyn NY 11201
description,setting,code|1,code|1|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,standard_charge|Aetna|PPO|negotiated_dollar,standard_charge|Aetna|PPO|methodology,standard_charge|UHC|Choice_Plus|negotiated_dollar,standard_charge|UHC|Choice_Plus|methodology
X-RAY CHEST,outpatient,71046,CPT,250.00,125.00,80.00,300.00,150.00,fee_schedule,175.00,case_rate
MRI BRAIN,inpatient,70553,CPT,3500.00,1750.00,1200.00,4000.00,2200.00,per_diem,,
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write wide CSV: %v", err)
	}
	return path
}

// csvToParquet reads a CSV file via CSVReader, writes all rows to a parquet file
// via ChargeWriter, and returns the parquet path and collected rows.
func csvToParquet(t *testing.T, csvPath string) (string, []HospitalChargeRow) {
	t.Helper()

	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader(%s): %v", csvPath, err)
	}
	defer reader.Close()

	var allRows []HospitalChargeRow
	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("CSVReader.Next: %v", err)
		}
		allRows = append(allRows, rows...)
	}

	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "output.parquet")
	w, err := NewChargeWriter(parquetPath)
	if err != nil {
		t.Fatalf("NewChargeWriter: %v", err)
	}
	if _, err := w.Write(allRows); err != nil {
		t.Fatalf("ChargeWriter.Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ChargeWriter.Close: %v", err)
	}

	return parquetPath, allRows
}

// readParquet reads all HospitalChargeRow records from a parquet file.
func readParquet(t *testing.T, path string) []HospitalChargeRow {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[HospitalChargeRow](f)
	defer reader.Close()

	rows := make([]HospitalChargeRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read parquet: %v", err)
	}
	return rows[:n]
}

func strPtr(s string) *string   { return &s }
func f64Ptr(f float64) *float64 { return &f }

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.01
}

func assertStrPtrEq(t *testing.T, label string, got, want *string) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %q, want nil", label, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = nil, want %q", label, *want)
		return
	}
	if *got != *want {
		t.Errorf("%s = %q, want %q", label, *got, *want)
	}
}

func assertF64PtrEq(t *testing.T, label string, got, want *float64) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %f, want nil", label, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = nil, want %f", label, *want)
		return
	}
	if !approxEqual(*got, *want) {
		t.Errorf("%s = %f, want %f", label, *got, *want)
	}
}

func TestCSVReaderTallToParquet(t *testing.T) {
	csvPath := writeTallCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// Tall CSV has 4 data rows → 4 HospitalChargeRows (1 per CSV row)
	if len(csvRows) != 4 {
		t.Fatalf("CSV produced %d rows, want 4", len(csvRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "tall" {
		t.Errorf("format = %q, want %q", reader.Format(), "tall")
	}

	// ── Verify hospital metadata propagated to all rows ──────────────
	for i, row := range pqRows {
		if row.HospitalName != "Test General Hospital" {
			t.Errorf("row[%d].HospitalName = %q, want %q", i, row.HospitalName, "Test General Hospital")
		}
		if row.LastUpdatedOn != "2024-01-15" {
			t.Errorf("row[%d].LastUpdatedOn = %q, want %q", i, row.LastUpdatedOn, "2024-01-15")
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q, want %q", i, row.Version, "2.0.0")
		}
		if row.HospitalAddress != "123 Main St, New York, NY 10001" {
			t.Errorf("row[%d].HospitalAddress = %q", i, row.HospitalAddress)
		}
	}

	// Parquet rows are sorted by cpt_code (nulls first). Build a lookup
	// by description+payer to verify specific rows regardless of order.
	find := func(desc string, payer *string) *HospitalChargeRow {
		for i := range pqRows {
			r := &pqRows[i]
			payerMatch := (payer == nil && r.PayerName == nil) ||
				(payer != nil && r.PayerName != nil && *payer == *r.PayerName)
			if r.Description == desc && payerMatch {
				return r
			}
		}
		t.Fatalf("row not found: desc=%q payer=%v", desc, payer)
		return nil
	}

	// ── ECHOCARDIOGRAM / Aetna ───────────────────────────────────────
	r := find("ECHOCARDIOGRAM COMPLETE", strPtr("Aetna"))
	if r.Setting != "outpatient" {
		t.Errorf("ECHO/Aetna Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "ECHO/Aetna CPTCode", r.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "ECHO/Aetna HCPCSCode", r.HCPCSCode, strPtr("G0389"))
	assertF64PtrEq(t, "ECHO/Aetna GrossCharge", r.GrossCharge, f64Ptr(1500.00))
	assertF64PtrEq(t, "ECHO/Aetna DiscountedCash", r.DiscountedCash, f64Ptr(750.00))
	assertF64PtrEq(t, "ECHO/Aetna MinCharge", r.MinCharge, f64Ptr(500.00))
	assertF64PtrEq(t, "ECHO/Aetna MaxCharge", r.MaxCharge, f64Ptr(2000.00))
	assertStrPtrEq(t, "ECHO/Aetna PlanName", r.PlanName, strPtr("Aetna PPO"))
	assertF64PtrEq(t, "ECHO/Aetna NegotiatedDollar", r.NegotiatedDollar, f64Ptr(900.00))
	assertStrPtrEq(t, "ECHO/Aetna Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── ECHOCARDIOGRAM / UHC ─────────────────────────────────────────
	r = find("ECHOCARDIOGRAM COMPLETE", strPtr("UnitedHealthcare"))
	assertStrPtrEq(t, "ECHO/UHC PlanName", r.PlanName, strPtr("UHC Choice Plus"))
	assertF64PtrEq(t, "ECHO/UHC NegotiatedDollar", r.NegotiatedDollar, f64Ptr(1100.00))
	assertStrPtrEq(t, "ECHO/UHC Methodology", r.Methodology, strPtr("case_rate"))
	assertStrPtrEq(t, "ECHO/UHC CPTCode", r.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "ECHO/UHC HCPCSCode", r.HCPCSCode, strPtr("G0389"))

	// ── ACETAMINOPHEN / drug info ────────────────────────────────────
	r = find("ACETAMINOPHEN 500MG TABLET", strPtr("Cigna"))
	if r.Setting != "inpatient" {
		t.Errorf("ACET Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "ACET NDCCode", r.NDCCode, strPtr("00456-0422-01"))
	assertStrPtrEq(t, "ACET CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "ACET GrossCharge", r.GrossCharge, f64Ptr(15.50))
	assertF64PtrEq(t, "ACET DiscountedCash", r.DiscountedCash, f64Ptr(8.25))
	assertF64PtrEq(t, "ACET DrugUnitOfMeasurement", r.DrugUnitOfMeasurement, f64Ptr(500.0))
	assertStrPtrEq(t, "ACET DrugTypeOfMeasurement", r.DrugTypeOfMeasurement, strPtr("ME"))
	assertStrPtrEq(t, "ACET AdditionalGenericNotes", r.AdditionalGenericNotes, strPtr("Oral tablet only"))
	assertStrPtrEq(t, "ACET PlanName", r.PlanName, strPtr("Cigna Open Access"))
	assertF64PtrEq(t, "ACET NegotiatedDollar", r.NegotiatedDollar, f64Ptr(10.00))
	assertStrPtrEq(t, "ACET Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── HEART TRANSPLANT / no payer, with modifiers ──────────────────
	r = find("HEART TRANSPLANT WITH MCC", nil)
	assertStrPtrEq(t, "HEART MSDRGCode", r.MSDRGCode, strPtr("001"))
	assertStrPtrEq(t, "HEART PayerName", r.PayerName, nil)
	assertStrPtrEq(t, "HEART PlanName", r.PlanName, nil)
	assertF64PtrEq(t, "HEART GrossCharge", r.GrossCharge, f64Ptr(500000.00))
	assertF64PtrEq(t, "HEART DiscountedCash", r.DiscountedCash, f64Ptr(250000.00))
	assertStrPtrEq(t, "HEART Modifiers", r.Modifiers, strPtr("26 59"))

	// ── Round-trip: CSV rows match parquet rows (order-independent) ──
	sortRowsByCPT(csvRows)
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		if csv.Setting != pq.Setting {
			t.Errorf("row[%d] Setting mismatch: csv=%q pq=%q", i, csv.Setting, pq.Setting)
		}
		assertStrPtrEq(t, "roundtrip CPTCode", pq.CPTCode, csv.CPTCode)
		assertStrPtrEq(t, "roundtrip HCPCSCode", pq.HCPCSCode, csv.HCPCSCode)
		assertStrPtrEq(t, "roundtrip MSDRGCode", pq.MSDRGCode, csv.MSDRGCode)
		assertStrPtrEq(t, "roundtrip NDCCode", pq.NDCCode, csv.NDCCode)
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertStrPtrEq(t, "roundtrip PlanName", pq.PlanName, csv.PlanName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip DiscountedCash", pq.DiscountedCash, csv.DiscountedCash)
		assertF64PtrEq(t, "roundtrip MinCharge", pq.MinCharge, csv.MinCharge)
		assertF64PtrEq(t, "roundtrip MaxCharge", pq.MaxCharge, csv.MaxCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
		assertStrPtrEq(t, "roundtrip Modifiers", pq.Modifiers, csv.Modifiers)
		assertF64PtrEq(t, "roundtrip DrugUnit", pq.DrugUnitOfMeasurement, csv.DrugUnitOfMeasurement)
		assertStrPtrEq(t, "roundtrip DrugType", pq.DrugTypeOfMeasurement, csv.DrugTypeOfMeasurement)
	}
}

func TestCSVReaderWideToParquet(t *testing.T) {
	csvPath := writeWideCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// Wide CSV: 2 data rows, each with 2 payer columns.
	// Row 1 (X-RAY): both Aetna and UHC have data → 2 rows
	// Row 2 (MRI): Aetna has data, UHC has no dollar/method → 1 row (Aetna only)
	if len(csvRows) != 3 {
		t.Fatalf("CSV produced %d rows, want 3", len(csvRows))
	}
	if len(pqRows) != 3 {
		t.Fatalf("parquet has %d rows, want 3", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "wide" {
		t.Errorf("format = %q, want %q", reader.Format(), "wide")
	}
	if reader.PayerPlanCount() != 2 {
		t.Errorf("PayerPlanCount = %d, want 2", reader.PayerPlanCount())
	}

	// ── Hospital metadata ────────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Wide Test Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
	}

	// Parquet rows are sorted by cpt_code. Lookup by description+payer.
	find := func(desc string, payer *string) *HospitalChargeRow {
		for i := range pqRows {
			r := &pqRows[i]
			payerMatch := (payer == nil && r.PayerName == nil) ||
				(payer != nil && r.PayerName != nil && *payer == *r.PayerName)
			if r.Description == desc && payerMatch {
				return r
			}
		}
		t.Fatalf("row not found: desc=%q payer=%v", desc, payer)
		return nil
	}

	// ── X-RAY / Aetna ───────────────────────────────────────────────
	r := find("X-RAY CHEST", strPtr("Aetna"))
	if r.Setting != "outpatient" {
		t.Errorf("XRAY/Aetna Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "XRAY/Aetna CPTCode", r.CPTCode, strPtr("71046"))
	assertF64PtrEq(t, "XRAY/Aetna GrossCharge", r.GrossCharge, f64Ptr(250.00))
	assertF64PtrEq(t, "XRAY/Aetna DiscountedCash", r.DiscountedCash, f64Ptr(125.00))
	assertStrPtrEq(t, "XRAY/Aetna PlanName", r.PlanName, strPtr("PPO"))
	assertF64PtrEq(t, "XRAY/Aetna NegotiatedDollar", r.NegotiatedDollar, f64Ptr(150.00))
	assertStrPtrEq(t, "XRAY/Aetna Methodology", r.Methodology, strPtr("fee_schedule"))

	// ── X-RAY / UHC ─────────────────────────────────────────────────
	r = find("X-RAY CHEST", strPtr("UHC"))
	assertStrPtrEq(t, "XRAY/UHC PlanName", r.PlanName, strPtr("Choice Plus"))
	assertF64PtrEq(t, "XRAY/UHC NegotiatedDollar", r.NegotiatedDollar, f64Ptr(175.00))
	assertStrPtrEq(t, "XRAY/UHC Methodology", r.Methodology, strPtr("case_rate"))
	assertF64PtrEq(t, "XRAY/UHC GrossCharge", r.GrossCharge, f64Ptr(250.00))

	// ── MRI / Aetna only (UHC has no data) ──────────────────────────
	r = find("MRI BRAIN", strPtr("Aetna"))
	if r.Setting != "inpatient" {
		t.Errorf("MRI Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "MRI CPTCode", r.CPTCode, strPtr("70553"))
	assertStrPtrEq(t, "MRI PlanName", r.PlanName, strPtr("PPO"))
	assertF64PtrEq(t, "MRI NegotiatedDollar", r.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "MRI Methodology", r.Methodology, strPtr("per_diem"))
	assertF64PtrEq(t, "MRI GrossCharge", r.GrossCharge, f64Ptr(3500.00))

	// ── Round-trip integrity (order-independent) ─────────────────────
	sortRowsByCPT(csvRows)
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
	}
}

// writeV3TallCSV creates a V3 Tall-format CSV with V3-specific columns.
func writeV3TallCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "v3_tall.csv")

	content := `hospital_name,last_updated_on,version,location_name,hospital_address,license_number|NY,type_2_npi,To the best of its knowledge and belief,attester_name,financial_aid_policy
V3 Tall Hospital,2026-01-15,3.0.0,Main Campus|123 Health St,123 Health St|Suite 100,LIC-9988,1234567890|9876543210,true,Dr. John Doe,https://example.org/aid
description,setting,code|1,code|1|type,code|2,code|2|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|negotiated_percentage,standard_charge|negotiated_algorithm,standard_charge|methodology,median_amount,10th_percentile,90th_percentile,count,billing_class,additional_generic_notes,drug_unit_of_measurement,drug_type_of_measurement
CT SCAN ABDOMEN,outpatient,74177,CPT,,,2500.00,1250.00,900.00,3200.00,Aetna,Aetna PPO,1800.00,,,fee_schedule,1750.00,1100.00,2900.00,312,facility,,,,
CT SCAN ABDOMEN,outpatient,74177,CPT,,,2500.00,1250.00,900.00,3200.00,Blue Cross,BC PPO,,72.5,72.5% of billed,percent_of_total_billed_charges,1600.00,950.00,2800.00,1 through 10,facility,,,,
REHAB GROUP THERAPY,inpatient,0024,CMG,,,600.00,300.00,250.00,900.00,Cigna,Cigna HMO,450.00,,,case_rate,400.00,280.00,700.00,55,professional,,,,
LTC RESPIRATORY,inpatient,189,MS-LTC-DRG,,,18000.00,9000.00,,,,,,,,,,,,,both,,100.0,UN
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write V3 tall CSV: %v", err)
	}
	return path
}

func TestCSVReaderV3TallToParquet(t *testing.T) {
	csvPath := writeV3TallCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// 4 data rows (2 CT payer rows + 1 REHAB + 1 LTC)
	if len(csvRows) != 4 {
		t.Fatalf("CSV produced %d rows, want 4", len(csvRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "tall" {
		t.Errorf("format = %q, want %q", reader.Format(), "tall")
	}

	// ── Verify V3 metadata ───────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "V3 Tall Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.Version != "3.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		// V3 uses location_name header
		if row.HospitalLocation != "Main Campus|123 Health St" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
		assertStrPtrEq(t, "LicenseNumber", row.LicenseNumber, strPtr("LIC-9988"))
		assertStrPtrEq(t, "LicenseState", row.LicenseState, strPtr("NY"))
		if !row.Affirmation {
			t.Errorf("row[%d].Affirmation = false, want true", i)
		}
		assertStrPtrEq(t, "FinancialAidPolicy", row.FinancialAidPolicy, strPtr("https://example.org/aid"))
	}

	find := func(desc string, payer *string) *HospitalChargeRow {
		for i := range pqRows {
			r := &pqRows[i]
			payerMatch := (payer == nil && r.PayerName == nil) ||
				(payer != nil && r.PayerName != nil && *payer == *r.PayerName)
			if r.Description == desc && payerMatch {
				return r
			}
		}
		t.Fatalf("row not found: desc=%q payer=%v", desc, payer)
		return nil
	}

	// ── CT SCAN / Aetna — V3 dollar + allowed amounts ────────────────
	r := find("CT SCAN ABDOMEN", strPtr("Aetna"))
	assertStrPtrEq(t, "CT/Aetna CPTCode", r.CPTCode, strPtr("74177"))
	assertF64PtrEq(t, "CT/Aetna GrossCharge", r.GrossCharge, f64Ptr(2500.00))
	assertF64PtrEq(t, "CT/Aetna NegotiatedDollar", r.NegotiatedDollar, f64Ptr(1800.00))
	assertStrPtrEq(t, "CT/Aetna Methodology", r.Methodology, strPtr("fee_schedule"))
	assertF64PtrEq(t, "CT/Aetna MedianAmount", r.MedianAmount, f64Ptr(1750.00))
	assertF64PtrEq(t, "CT/Aetna Pct10Amount", r.Pct10Amount, f64Ptr(1100.00))
	assertF64PtrEq(t, "CT/Aetna Pct90Amount", r.Pct90Amount, f64Ptr(2900.00))
	assertStrPtrEq(t, "CT/Aetna AllowedCount", r.AllowedCount, strPtr("312"))
	assertStrPtrEq(t, "CT/Aetna BillingClass", r.BillingClass, strPtr("facility"))

	// ── CT SCAN / BC — V3 percentage + algorithm + allowed amounts ───
	r = find("CT SCAN ABDOMEN", strPtr("Blue Cross"))
	assertF64PtrEq(t, "CT/BC NegotiatedPercentage", r.NegotiatedPercentage, f64Ptr(72.5))
	assertStrPtrEq(t, "CT/BC NegotiatedAlgorithm", r.NegotiatedAlgorithm, strPtr("72.5% of billed"))
	assertStrPtrEq(t, "CT/BC Methodology", r.Methodology, strPtr("percent_of_total_billed_charges"))
	assertF64PtrEq(t, "CT/BC MedianAmount", r.MedianAmount, f64Ptr(1600.00))
	assertF64PtrEq(t, "CT/BC Pct10Amount", r.Pct10Amount, f64Ptr(950.00))
	assertF64PtrEq(t, "CT/BC Pct90Amount", r.Pct90Amount, f64Ptr(2800.00))
	assertStrPtrEq(t, "CT/BC AllowedCount", r.AllowedCount, strPtr("1 through 10"))

	// ── REHAB / Cigna — V3 CMG code type ─────────────────────────────
	r = find("REHAB GROUP THERAPY", strPtr("Cigna"))
	assertStrPtrEq(t, "REHAB CMGCode", r.CMGCode, strPtr("0024"))
	assertStrPtrEq(t, "REHAB CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "REHAB NegotiatedDollar", r.NegotiatedDollar, f64Ptr(450.00))
	assertF64PtrEq(t, "REHAB MedianAmount", r.MedianAmount, f64Ptr(400.00))
	assertStrPtrEq(t, "REHAB AllowedCount", r.AllowedCount, strPtr("55"))
	assertStrPtrEq(t, "REHAB BillingClass", r.BillingClass, strPtr("professional"))

	// ── LTC / no payer — V3 MS-LTC-DRG code type + drug info ─────────
	r = find("LTC RESPIRATORY", nil)
	assertStrPtrEq(t, "LTC MSLTCDRGCode", r.MSLTCDRGCode, strPtr("189"))
	assertStrPtrEq(t, "LTC CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "LTC GrossCharge", r.GrossCharge, f64Ptr(18000.00))
	assertStrPtrEq(t, "LTC PayerName", r.PayerName, nil)
	assertStrPtrEq(t, "LTC BillingClass", r.BillingClass, strPtr("both"))
	assertF64PtrEq(t, "LTC DrugUnit", r.DrugUnitOfMeasurement, f64Ptr(100.0))
	assertStrPtrEq(t, "LTC DrugType", r.DrugTypeOfMeasurement, strPtr("UN"))

	// ── Round-trip ───────────────────────────────────────────────────
	sortRowsByCPT(csvRows)
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
		// V3 fields round-trip
		assertF64PtrEq(t, "roundtrip MedianAmount", pq.MedianAmount, csv.MedianAmount)
		assertF64PtrEq(t, "roundtrip Pct10Amount", pq.Pct10Amount, csv.Pct10Amount)
		assertF64PtrEq(t, "roundtrip Pct90Amount", pq.Pct90Amount, csv.Pct90Amount)
		assertStrPtrEq(t, "roundtrip AllowedCount", pq.AllowedCount, csv.AllowedCount)
		assertStrPtrEq(t, "roundtrip CMGCode", pq.CMGCode, csv.CMGCode)
		assertStrPtrEq(t, "roundtrip MSLTCDRGCode", pq.MSLTCDRGCode, csv.MSLTCDRGCode)
		assertStrPtrEq(t, "roundtrip BillingClass", pq.BillingClass, csv.BillingClass)
	}
}

// writeV3WideCSV creates a V3 Wide-format CSV with V3-specific columns.
func writeV3WideCSV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "v3_wide.csv")

	content := `hospital_name,last_updated_on,version,location_name,hospital_address
V3 Wide Hospital,2026-01-15,3.0.0,Wide Campus,456 Wide St
description,setting,code|1,code|1|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,billing_class,standard_charge|Aetna|PPO|negotiated_dollar,standard_charge|Aetna|PPO|methodology,median_amount|Aetna|PPO,10th_percentile|Aetna|PPO,90th_percentile|Aetna|PPO,count|Aetna|PPO,standard_charge|BCBS|HMO|negotiated_percentage,standard_charge|BCBS|HMO|negotiated_algorithm,standard_charge|BCBS|HMO|methodology,median_amount|BCBS|HMO,10th_percentile|BCBS|HMO,90th_percentile|BCBS|HMO,count|BCBS|HMO,additional_payer_notes|BCBS|HMO
ULTRASOUND ABDOM,outpatient,76700,CPT,1200.00,600.00,400.00,1800.00,facility,850.00,fee_schedule,820.00,550.00,1500.00,198,70.0,70% of billed,percent_of_total_billed_charges,780.00,500.00,1400.00,1 through 10,Algorithm based
HIP REPLACEMENT,inpatient,0023,CMG,45000.00,22500.00,18000.00,65000.00,,35000.00,case_rate,33000.00,25000.00,55000.00,42,,,,,,,,,
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write V3 wide CSV: %v", err)
	}
	return path
}

func TestCSVReaderV3WideToParquet(t *testing.T) {
	csvPath := writeV3WideCSV(t)
	parquetPath, csvRows := csvToParquet(t, csvPath)
	pqRows := readParquet(t, parquetPath)

	// Wide CSV: 2 data rows
	// Row 1 (ULTRASOUND): Aetna + BCBS have data → 2 rows
	// Row 2 (HIP): Aetna has data, BCBS has no data → 1 row
	// Total: 3 rows
	if len(csvRows) != 3 {
		t.Fatalf("CSV produced %d rows, want 3", len(csvRows))
	}
	if len(pqRows) != 3 {
		t.Fatalf("parquet has %d rows, want 3", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewCSVReader(csvPath)
	if err != nil {
		t.Fatalf("NewCSVReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "wide" {
		t.Errorf("format = %q, want %q", reader.Format(), "wide")
	}

	// ── Verify V3 metadata ───────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "V3 Wide Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.Version != "3.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		if row.HospitalLocation != "Wide Campus" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
	}

	find := func(desc string, payer *string) *HospitalChargeRow {
		for i := range pqRows {
			r := &pqRows[i]
			payerMatch := (payer == nil && r.PayerName == nil) ||
				(payer != nil && r.PayerName != nil && *payer == *r.PayerName)
			if r.Description == desc && payerMatch {
				return r
			}
		}
		t.Fatalf("row not found: desc=%q payer=%v", desc, payer)
		return nil
	}

	// ── ULTRASOUND / Aetna — dollar + V3 allowed amounts ────────────
	r := find("ULTRASOUND ABDOM", strPtr("Aetna"))
	assertStrPtrEq(t, "US/Aetna CPTCode", r.CPTCode, strPtr("76700"))
	assertF64PtrEq(t, "US/Aetna GrossCharge", r.GrossCharge, f64Ptr(1200.00))
	assertF64PtrEq(t, "US/Aetna NegotiatedDollar", r.NegotiatedDollar, f64Ptr(850.00))
	assertStrPtrEq(t, "US/Aetna Methodology", r.Methodology, strPtr("fee_schedule"))
	assertStrPtrEq(t, "US/Aetna BillingClass", r.BillingClass, strPtr("facility"))
	assertF64PtrEq(t, "US/Aetna MedianAmount", r.MedianAmount, f64Ptr(820.00))
	assertF64PtrEq(t, "US/Aetna Pct10Amount", r.Pct10Amount, f64Ptr(550.00))
	assertF64PtrEq(t, "US/Aetna Pct90Amount", r.Pct90Amount, f64Ptr(1500.00))
	assertStrPtrEq(t, "US/Aetna AllowedCount", r.AllowedCount, strPtr("198"))

	// ── ULTRASOUND / BCBS — percentage + algorithm + V3 allowed ──────
	r = find("ULTRASOUND ABDOM", strPtr("BCBS"))
	assertF64PtrEq(t, "US/BCBS NegotiatedPercentage", r.NegotiatedPercentage, f64Ptr(70.0))
	assertStrPtrEq(t, "US/BCBS NegotiatedAlgorithm", r.NegotiatedAlgorithm, strPtr("70% of billed"))
	assertStrPtrEq(t, "US/BCBS Methodology", r.Methodology, strPtr("percent_of_total_billed_charges"))
	assertF64PtrEq(t, "US/BCBS MedianAmount", r.MedianAmount, f64Ptr(780.00))
	assertF64PtrEq(t, "US/BCBS Pct10Amount", r.Pct10Amount, f64Ptr(500.00))
	assertF64PtrEq(t, "US/BCBS Pct90Amount", r.Pct90Amount, f64Ptr(1400.00))
	assertStrPtrEq(t, "US/BCBS AllowedCount", r.AllowedCount, strPtr("1 through 10"))
	assertStrPtrEq(t, "US/BCBS AdditionalPayerNotes", r.AdditionalPayerNotes, strPtr("Algorithm based"))

	// ── HIP REPLACEMENT / Aetna — CMG code type + V3 allowed ────────
	r = find("HIP REPLACEMENT", strPtr("Aetna"))
	assertStrPtrEq(t, "HIP CMGCode", r.CMGCode, strPtr("0023"))
	assertStrPtrEq(t, "HIP CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "HIP NegotiatedDollar", r.NegotiatedDollar, f64Ptr(35000.00))
	assertStrPtrEq(t, "HIP Methodology", r.Methodology, strPtr("case_rate"))
	assertF64PtrEq(t, "HIP MedianAmount", r.MedianAmount, f64Ptr(33000.00))
	assertF64PtrEq(t, "HIP Pct10Amount", r.Pct10Amount, f64Ptr(25000.00))
	assertF64PtrEq(t, "HIP Pct90Amount", r.Pct90Amount, f64Ptr(55000.00))
	assertStrPtrEq(t, "HIP AllowedCount", r.AllowedCount, strPtr("42"))

	// ── Round-trip ───────────────────────────────────────────────────
	sortRowsByCPT(csvRows)
	for i := range csvRows {
		csv := csvRows[i]
		pq := pqRows[i]
		if csv.Description != pq.Description {
			t.Errorf("row[%d] Description mismatch: csv=%q pq=%q", i, csv.Description, pq.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", pq.PayerName, csv.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", pq.GrossCharge, csv.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", pq.NegotiatedDollar, csv.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", pq.Methodology, csv.Methodology)
		// V3 fields round-trip
		assertF64PtrEq(t, "roundtrip MedianAmount", pq.MedianAmount, csv.MedianAmount)
		assertF64PtrEq(t, "roundtrip Pct10Amount", pq.Pct10Amount, csv.Pct10Amount)
		assertF64PtrEq(t, "roundtrip Pct90Amount", pq.Pct90Amount, csv.Pct90Amount)
		assertStrPtrEq(t, "roundtrip AllowedCount", pq.AllowedCount, csv.AllowedCount)
		assertStrPtrEq(t, "roundtrip CMGCode", pq.CMGCode, csv.CMGCode)
	}
}
