package internal

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// findRow looks up a row by description and payer name (order-independent).
func findRow(t *testing.T, rows []HospitalChargeRow, desc string, payer *string) *HospitalChargeRow {
	t.Helper()
	for i := range rows {
		r := &rows[i]
		payerMatch := (payer == nil && r.PayerName == nil) ||
			(payer != nil && r.PayerName != nil && *payer == *r.PayerName)
		if r.Description == desc && payerMatch {
			return r
		}
	}
	t.Fatalf("row not found: desc=%q payer=%v", desc, payer)
	return nil
}

// jsonToParquet reads a JSON file via JSONReader, writes all rows to a parquet
// file via ChargeWriter, and returns the parquet path and collected rows.
func jsonToParquet(t *testing.T, jsonPath string) (string, []HospitalChargeRow) {
	t.Helper()

	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader(%s): %v", jsonPath, err)
	}
	defer reader.Close()

	var allRows []HospitalChargeRow
	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("JSONReader.Next: %v", err)
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

// readParquetJSON reads all HospitalChargeRow records from a parquet file.
// Named differently to avoid collision with readParquet in csv_reader_test.go.
func readParquetJSON(t *testing.T, path string) []HospitalChargeRow {
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

func TestJSONReaderV2ToParquet(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v2.json")
	parquetPath, jsonRows := jsonToParquet(t, jsonPath)
	pqRows := readParquetJSON(t, parquetPath)

	// V2 JSON: 3 items
	//   Item 1 (X-RAY): 1 charge setting × 2 payers = 2 rows
	//   Item 2 (IBUPROFEN): 1 charge setting × 1 payer = 1 row
	//   Item 3 (KNEE): 1 charge setting × 0 payers = 1 row (gross/discounted only)
	// Total: 4 rows
	if len(jsonRows) != 4 {
		t.Fatalf("JSON produced %d rows, want 4", len(jsonRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "json-v2" {
		t.Errorf("format = %q, want %q", reader.Format(), "json-v2")
	}

	// ── Verify hospital metadata (same for all rows) ─────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Test Community Hospital" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.LastUpdatedOn != "2024-06-01" {
			t.Errorf("row[%d].LastUpdatedOn = %q", i, row.LastUpdatedOn)
		}
		if row.Version != "2.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		// hospital_location is joined with "; "
		if row.HospitalLocation != "Test Community Hospital; 456 Oak Ave, Brooklyn, NY 11201" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
		// hospital_address is joined with "; "
		if row.HospitalAddress != "456 Oak Ave; Brooklyn, NY 11201" {
			t.Errorf("row[%d].HospitalAddress = %q", i, row.HospitalAddress)
		}
		assertStrPtrEq(t, "LicenseNumber", row.LicenseNumber, strPtr("H-99887"))
		assertStrPtrEq(t, "LicenseState", row.LicenseState, strPtr("NY"))
		if !row.Affirmation {
			t.Errorf("row[%d].Affirmation = false, want true", i)
		}
	}

	// ── X-RAY / Aetna ───────────────────────────────────────────────
	r := findRow(t, pqRows, "X-RAY CHEST 2 VIEWS", strPtr("Aetna"))
	if r.Setting != "outpatient" {
		t.Errorf("XRAY/Aetna Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "XRAY/Aetna CPTCode", r.CPTCode, strPtr("71046"))
	assertStrPtrEq(t, "XRAY/Aetna RCCode", r.RCCode, strPtr("0324"))
	assertF64PtrEq(t, "XRAY/Aetna GrossCharge", r.GrossCharge, f64Ptr(1250.00))
	assertF64PtrEq(t, "XRAY/Aetna DiscountedCash", r.DiscountedCash, f64Ptr(625.00))
	assertF64PtrEq(t, "XRAY/Aetna MinCharge", r.MinCharge, f64Ptr(400.00))
	assertF64PtrEq(t, "XRAY/Aetna MaxCharge", r.MaxCharge, f64Ptr(1800.00))
	assertStrPtrEq(t, "XRAY/Aetna Modifiers", r.Modifiers, strPtr("26|TC"))
	assertStrPtrEq(t, "XRAY/Aetna GenericNotes", r.AdditionalGenericNotes, strPtr("Includes interpretation"))
	assertStrPtrEq(t, "XRAY/Aetna PlanName", r.PlanName, strPtr("Aetna PPO"))
	assertF64PtrEq(t, "XRAY/Aetna NegotiatedDollar", r.NegotiatedDollar, f64Ptr(800.00))
	assertStrPtrEq(t, "XRAY/Aetna Methodology", r.Methodology, strPtr("fee_schedule"))
	assertStrPtrEq(t, "XRAY/Aetna PayerNotes", r.AdditionalPayerNotes, strPtr("Network rate"))

	// ── X-RAY / Blue Cross (percentage + estimated) ──────────────────
	r = findRow(t, pqRows, "X-RAY CHEST 2 VIEWS", strPtr("Blue Cross"))
	assertStrPtrEq(t, "XRAY/BC PlanName", r.PlanName, strPtr("BC Standard"))
	assertStrPtrEq(t, "XRAY/BC Methodology", r.Methodology, strPtr("percent_of_total_billed_charges"))
	assertF64PtrEq(t, "XRAY/BC NegotiatedPercentage", r.NegotiatedPercentage, f64Ptr(75.5))
	assertF64PtrEq(t, "XRAY/BC EstimatedAmount", r.EstimatedAmount, f64Ptr(943.75))
	assertF64PtrEq(t, "XRAY/BC NegotiatedDollar", r.NegotiatedDollar, nil)
	assertF64PtrEq(t, "XRAY/BC GrossCharge", r.GrossCharge, f64Ptr(1250.00))
	assertStrPtrEq(t, "XRAY/BC Modifiers", r.Modifiers, strPtr("26|TC"))

	// ── IBUPROFEN / drug info + algorithm ────────────────────────────
	r = findRow(t, pqRows, "IBUPROFEN 200MG TABLET", strPtr("UnitedHealthcare"))
	if r.Setting != "inpatient" {
		t.Errorf("IBU Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "IBU NDCCode", r.NDCCode, strPtr("00573-0150-20"))
	assertStrPtrEq(t, "IBU HCPCSCode", r.HCPCSCode, strPtr("J3490"))
	assertStrPtrEq(t, "IBU CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "IBU DrugUnit", r.DrugUnitOfMeasurement, f64Ptr(200.0))
	assertStrPtrEq(t, "IBU DrugType", r.DrugTypeOfMeasurement, strPtr("ME"))
	assertF64PtrEq(t, "IBU GrossCharge", r.GrossCharge, f64Ptr(12.50))
	assertF64PtrEq(t, "IBU DiscountedCash", r.DiscountedCash, f64Ptr(6.25))
	assertStrPtrEq(t, "IBU PlanName", r.PlanName, strPtr("UHC Choice Plus"))
	assertF64PtrEq(t, "IBU NegotiatedDollar", r.NegotiatedDollar, f64Ptr(8.00))
	assertStrPtrEq(t, "IBU Methodology", r.Methodology, strPtr("per_diem"))
	assertStrPtrEq(t, "IBU Algorithm", r.NegotiatedAlgorithm, strPtr("per diem rate table v3"))

	// ── KNEE REPLACEMENT / no payer, gross_charges string ────────────
	r = findRow(t, pqRows, "KNEE REPLACEMENT", nil)
	assertStrPtrEq(t, "KNEE MSDRGCode", r.MSDRGCode, strPtr("470"))
	assertStrPtrEq(t, "KNEE PayerName", r.PayerName, nil)
	assertF64PtrEq(t, "KNEE GrossCharge", r.GrossCharge, f64Ptr(45000.00))
	assertF64PtrEq(t, "KNEE DiscountedCash", r.DiscountedCash, f64Ptr(22500.00))
	assertF64PtrEq(t, "KNEE MinCharge", r.MinCharge, f64Ptr(20000.00))
	assertF64PtrEq(t, "KNEE MaxCharge", r.MaxCharge, f64Ptr(65000.00))

	// ── Round-trip: JSON rows match parquet rows (order-independent) ─
	sortRowsByCPT(jsonRows)
	for i := range jsonRows {
		j := jsonRows[i]
		p := pqRows[i]
		if j.Description != p.Description {
			t.Errorf("row[%d] Description mismatch: json=%q pq=%q", i, j.Description, p.Description)
		}
		if j.Setting != p.Setting {
			t.Errorf("row[%d] Setting mismatch: json=%q pq=%q", i, j.Setting, p.Setting)
		}
		assertStrPtrEq(t, "roundtrip PayerName", p.PayerName, j.PayerName)
		assertStrPtrEq(t, "roundtrip PlanName", p.PlanName, j.PlanName)
		assertF64PtrEq(t, "roundtrip GrossCharge", p.GrossCharge, j.GrossCharge)
		assertF64PtrEq(t, "roundtrip DiscountedCash", p.DiscountedCash, j.DiscountedCash)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", p.NegotiatedDollar, j.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", p.Methodology, j.Methodology)
		assertF64PtrEq(t, "roundtrip DrugUnit", p.DrugUnitOfMeasurement, j.DrugUnitOfMeasurement)
		assertStrPtrEq(t, "roundtrip DrugType", p.DrugTypeOfMeasurement, j.DrugTypeOfMeasurement)
	}
}

func TestJSONReaderV3ToParquet(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v3.json")
	parquetPath, jsonRows := jsonToParquet(t, jsonPath)
	pqRows := readParquetJSON(t, parquetPath)

	// V3 JSON: 4 items
	//   Item 1 (MRI): 1 charge setting × 1 payer = 1 row
	//   Item 2 (ER VISIT): 1 charge setting × 0 payers = 1 row
	//   Item 3 (REHAB): 1 charge setting × 1 payer = 1 row
	//   Item 4 (LTC): 1 charge setting × 0 payers = 1 row
	// Total: 4 rows
	if len(jsonRows) != 4 {
		t.Fatalf("JSON produced %d rows, want 4", len(jsonRows))
	}
	if len(pqRows) != 4 {
		t.Fatalf("parquet has %d rows, want 4", len(pqRows))
	}

	// ── Verify format detection ──────────────────────────────────────
	reader, err := NewJSONReader(jsonPath)
	if err != nil {
		t.Fatalf("NewJSONReader: %v", err)
	}
	defer reader.Close()
	if reader.Format() != "json-v3" {
		t.Errorf("format = %q, want %q", reader.Format(), "json-v3")
	}

	// ── Verify V3 metadata ───────────────────────────────────────────
	for i, row := range pqRows {
		if row.HospitalName != "Metro Health Center" {
			t.Errorf("row[%d].HospitalName = %q", i, row.HospitalName)
		}
		if row.LastUpdatedOn != "2025-01-15" {
			t.Errorf("row[%d].LastUpdatedOn = %q", i, row.LastUpdatedOn)
		}
		if row.Version != "3.0.0" {
			t.Errorf("row[%d].Version = %q", i, row.Version)
		}
		// V3 uses location_name instead of hospital_location
		if row.HospitalLocation != "Metro Health Center; 789 Elm St, Manhattan, NY 10010" {
			t.Errorf("row[%d].HospitalLocation = %q", i, row.HospitalLocation)
		}
		assertStrPtrEq(t, "LicenseNumber", row.LicenseNumber, strPtr("MHC-5544"))
		assertStrPtrEq(t, "LicenseState", row.LicenseState, strPtr("NY"))
		// V3 uses attestation instead of affirmation
		if !row.Affirmation {
			t.Errorf("row[%d].Affirmation = false, want true", i)
		}
		// V3 header-level metadata
		assertStrPtrEq(t, "FinancialAidPolicy", row.FinancialAidPolicy, strPtr("https://metrohealthcenter.org/financial-aid"))
		assertStrPtrEq(t, "GeneralContractProvisions", row.GeneralContractProvisions, strPtr("Stop-loss at $500,000 per case"))
	}

	// ── MRI BRAIN / Cigna — V3 allowed-amount fields ─────────────────
	r := findRow(t, pqRows, "MRI BRAIN WITHOUT CONTRAST", strPtr("Cigna"))
	if r.Setting != "outpatient" {
		t.Errorf("MRI Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "MRI CPTCode", r.CPTCode, strPtr("70551"))
	assertF64PtrEq(t, "MRI GrossCharge", r.GrossCharge, f64Ptr(3500.00))
	assertF64PtrEq(t, "MRI DiscountedCash", r.DiscountedCash, f64Ptr(1750.00))
	assertF64PtrEq(t, "MRI MinCharge", r.MinCharge, f64Ptr(1200.00))
	assertF64PtrEq(t, "MRI MaxCharge", r.MaxCharge, f64Ptr(4500.00))
	assertStrPtrEq(t, "MRI PlanName", r.PlanName, strPtr("Cigna Open Access"))
	assertF64PtrEq(t, "MRI NegotiatedDollar", r.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "MRI Methodology", r.Methodology, strPtr("case_rate"))
	// V3 allowed-amount statistics
	assertF64PtrEq(t, "MRI MedianAmount", r.MedianAmount, f64Ptr(2100.00))
	assertF64PtrEq(t, "MRI Pct10Amount", r.Pct10Amount, f64Ptr(1500.00))
	assertF64PtrEq(t, "MRI Pct90Amount", r.Pct90Amount, f64Ptr(3800.00))
	assertStrPtrEq(t, "MRI AllowedCount", r.AllowedCount, strPtr("245"))
	// V3 item-level billing_class
	assertStrPtrEq(t, "MRI BillingClass", r.BillingClass, strPtr("facility"))

	// ── ER VISIT / no payer — charge-level billing_class ─────────────
	r = findRow(t, pqRows, "EMERGENCY ROOM VISIT LEVEL 3", nil)
	if r.Setting != "outpatient" {
		t.Errorf("ER Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "ER CPTCode", r.CPTCode, strPtr("99283"))
	assertStrPtrEq(t, "ER RCCode", r.RCCode, strPtr("0450"))
	assertF64PtrEq(t, "ER GrossCharge", r.GrossCharge, f64Ptr(950.00))
	assertF64PtrEq(t, "ER DiscountedCash", r.DiscountedCash, f64Ptr(475.00))
	assertF64PtrEq(t, "ER MinCharge", r.MinCharge, nil)
	assertF64PtrEq(t, "ER MaxCharge", r.MaxCharge, nil)
	assertStrPtrEq(t, "ER PayerName", r.PayerName, nil)
	// V3 charge-level billing_class
	assertStrPtrEq(t, "ER BillingClass", r.BillingClass, strPtr("both"))
	// No payer → no V3 allowed-amount fields
	assertF64PtrEq(t, "ER MedianAmount", r.MedianAmount, nil)
	assertF64PtrEq(t, "ER Pct10Amount", r.Pct10Amount, nil)
	assertF64PtrEq(t, "ER Pct90Amount", r.Pct90Amount, nil)
	assertStrPtrEq(t, "ER AllowedCount", r.AllowedCount, nil)

	// ── REHAB THERAPY / Aetna — V3 new code type CMG + percentage ────
	r = findRow(t, pqRows, "REHAB THERAPY SESSION", strPtr("Aetna"))
	if r.Setting != "inpatient" {
		t.Errorf("REHAB Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "REHAB CMGCode", r.CMGCode, strPtr("0023"))
	assertStrPtrEq(t, "REHAB CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "REHAB GrossCharge", r.GrossCharge, f64Ptr(800.00))
	assertF64PtrEq(t, "REHAB DiscountedCash", r.DiscountedCash, f64Ptr(400.00))
	assertF64PtrEq(t, "REHAB MinCharge", r.MinCharge, f64Ptr(350.00))
	assertF64PtrEq(t, "REHAB MaxCharge", r.MaxCharge, f64Ptr(1200.00))
	assertStrPtrEq(t, "REHAB PlanName", r.PlanName, strPtr("Aetna PPO"))
	assertStrPtrEq(t, "REHAB Methodology", r.Methodology, strPtr("percent_of_total_billed_charges"))
	assertF64PtrEq(t, "REHAB NegotiatedPercentage", r.NegotiatedPercentage, f64Ptr(65.0))
	assertStrPtrEq(t, "REHAB NegotiatedAlgorithm", r.NegotiatedAlgorithm, strPtr("65% of billed charges"))
	assertF64PtrEq(t, "REHAB MedianAmount", r.MedianAmount, f64Ptr(520.00))
	assertF64PtrEq(t, "REHAB Pct10Amount", r.Pct10Amount, f64Ptr(380.00))
	assertF64PtrEq(t, "REHAB Pct90Amount", r.Pct90Amount, f64Ptr(750.00))
	assertStrPtrEq(t, "REHAB AllowedCount", r.AllowedCount, strPtr("1 through 10"))
	assertStrPtrEq(t, "REHAB AdditionalPayerNotes", r.AdditionalPayerNotes, strPtr("Subject to stop-loss"))

	// ── LONG TERM CARE / no payer — V3 new code type MS-LTC-DRG ──────
	r = findRow(t, pqRows, "LONG TERM CARE STAY", nil)
	if r.Setting != "inpatient" {
		t.Errorf("LTC Setting = %q", r.Setting)
	}
	assertStrPtrEq(t, "LTC MSLTCDRGCode", r.MSLTCDRGCode, strPtr("189"))
	assertStrPtrEq(t, "LTC CPTCode", r.CPTCode, nil)
	assertF64PtrEq(t, "LTC GrossCharge", r.GrossCharge, f64Ptr(25000.00))
	assertF64PtrEq(t, "LTC DiscountedCash", r.DiscountedCash, f64Ptr(12500.00))
	assertF64PtrEq(t, "LTC DrugUnit", r.DrugUnitOfMeasurement, f64Ptr(100.0))
	assertStrPtrEq(t, "LTC DrugType", r.DrugTypeOfMeasurement, strPtr("UN"))
	assertStrPtrEq(t, "LTC PayerName", r.PayerName, nil)

	// ── Round-trip integrity (order-independent) ─────────────────────
	sortRowsByCPT(jsonRows)
	for i := range jsonRows {
		j := jsonRows[i]
		p := pqRows[i]
		if j.Description != p.Description {
			t.Errorf("row[%d] Description mismatch: json=%q pq=%q", i, j.Description, p.Description)
		}
		assertStrPtrEq(t, "roundtrip PayerName", p.PayerName, j.PayerName)
		assertF64PtrEq(t, "roundtrip GrossCharge", p.GrossCharge, j.GrossCharge)
		assertF64PtrEq(t, "roundtrip NegotiatedDollar", p.NegotiatedDollar, j.NegotiatedDollar)
		assertStrPtrEq(t, "roundtrip Methodology", p.Methodology, j.Methodology)
		// V3 fields round-trip
		assertF64PtrEq(t, "roundtrip MedianAmount", p.MedianAmount, j.MedianAmount)
		assertF64PtrEq(t, "roundtrip Pct10Amount", p.Pct10Amount, j.Pct10Amount)
		assertF64PtrEq(t, "roundtrip Pct90Amount", p.Pct90Amount, j.Pct90Amount)
		assertStrPtrEq(t, "roundtrip AllowedCount", p.AllowedCount, j.AllowedCount)
		assertStrPtrEq(t, "roundtrip CMGCode", p.CMGCode, j.CMGCode)
		assertStrPtrEq(t, "roundtrip MSLTCDRGCode", p.MSLTCDRGCode, j.MSLTCDRGCode)
		assertStrPtrEq(t, "roundtrip BillingClass", p.BillingClass, j.BillingClass)
		assertF64PtrEq(t, "roundtrip DrugUnit", p.DrugUnitOfMeasurement, j.DrugUnitOfMeasurement)
		assertStrPtrEq(t, "roundtrip DrugType", p.DrugTypeOfMeasurement, j.DrugTypeOfMeasurement)
	}
}
