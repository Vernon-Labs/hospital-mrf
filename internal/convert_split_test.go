package internal

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// ── Parquet readers for split types ──────────────────────────────────

func readItemRows(t *testing.T, path string) []ItemRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open items parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[ItemRow](f)
	defer reader.Close()

	rows := make([]ItemRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read items parquet: %v", err)
	}
	return rows[:n]
}

func readPayerRows(t *testing.T, path string) []PayerRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open payer parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[PayerRow](f)
	defer reader.Close()

	rows := make([]PayerRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read payer parquet: %v", err)
	}
	return rows[:n]
}

func readCodeInfoRows(t *testing.T, path string) []CodeInfoRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open code-info parquet: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[CodeInfoRow](f)
	defer reader.Close()

	rows := make([]CodeInfoRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read code-info parquet: %v", err)
	}
	return rows[:n]
}

// ── Lookup helpers ───────────────────────────────────────────────────

func findItem(t *testing.T, items []ItemRow, desc string) *ItemRow {
	t.Helper()
	for i := range items {
		if items[i].Description == desc {
			return &items[i]
		}
	}
	t.Fatalf("item not found: %q", desc)
	return nil
}

func findPayer(t *testing.T, payers []PayerRow, desc string, payerName string) *PayerRow {
	t.Helper()
	for i := range payers {
		if payers[i].Description == desc && payers[i].PayerName != nil && *payers[i].PayerName == payerName {
			return &payers[i]
		}
	}
	t.Fatalf("payer row not found: desc=%q payer=%q", desc, payerName)
	return nil
}

func findCode(t *testing.T, codes []CodeInfoRow, codeType, codeValue string) *CodeInfoRow {
	t.Helper()
	for i := range codes {
		if codes[i].CodeType == codeType && codes[i].CodeValue == codeValue {
			return &codes[i]
		}
	}
	t.Fatalf("code not found: type=%q value=%q", codeType, codeValue)
	return nil
}

func hasCode(codes []CodeInfoRow, codeType, codeValue string) bool {
	for i := range codes {
		if codes[i].CodeType == codeType && codes[i].CodeValue == codeValue {
			return true
		}
	}
	return false
}

func countPayersByDesc(payers []PayerRow, desc string) int {
	n := 0
	for _, p := range payers {
		if p.Description == desc {
			n++
		}
	}
	return n
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// ── Tests ────────────────────────────────────────────────────────────

func TestConvertSplitJSONV2(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v2.json")
	basePath := filepath.Join(t.TempDir(), "v2_split")

	result, err := ConvertSplit(discardLogger, jsonPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// Verify file paths exist
	for _, path := range []string{result.ItemsPath, result.PayerPath, result.CodeInfoPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("output file missing: %s", path)
		}
	}

	// ── Items file ──
	// test_v2.json has 3 items: X-RAY CHEST, IBUPROFEN, KNEE REPLACEMENT
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 3 {
		t.Errorf("items count = %d, want 3", len(items))
	}
	if result.ItemCount != 3 {
		t.Errorf("result.ItemCount = %d, want 3", result.ItemCount)
	}

	xray := findItem(t, items, "X-RAY CHEST 2 VIEWS")
	assertF64PtrEq(t, "xray.GrossCharge", xray.GrossCharge, f64Ptr(1250.00))
	assertF64PtrEq(t, "xray.DiscountedCash", xray.DiscountedCash, f64Ptr(625.00))
	assertF64PtrEq(t, "xray.MinCharge", xray.MinCharge, f64Ptr(400.00))
	assertF64PtrEq(t, "xray.MaxCharge", xray.MaxCharge, f64Ptr(1800.00))
	assertStrPtrEq(t, "xray.CPTCode", xray.CPTCode, strPtr("71046"))
	assertStrPtrEq(t, "xray.RCCode", xray.RCCode, strPtr("0324"))
	assertStrPtrEq(t, "xray.Modifiers", xray.Modifiers, strPtr("26|TC"))
	if xray.Setting != "outpatient" {
		t.Errorf("xray.Setting = %q, want outpatient", xray.Setting)
	}

	ibu := findItem(t, items, "IBUPROFEN 200MG TABLET")
	assertStrPtrEq(t, "ibu.NDCCode", ibu.NDCCode, strPtr("00573-0150-20"))
	assertStrPtrEq(t, "ibu.HCPCSCode", ibu.HCPCSCode, strPtr("J3490"))
	assertF64PtrEq(t, "ibu.DrugUnitOfMeasurement", ibu.DrugUnitOfMeasurement, f64Ptr(200.0))
	assertStrPtrEq(t, "ibu.DrugTypeOfMeasurement", ibu.DrugTypeOfMeasurement, strPtr("ME"))

	knee := findItem(t, items, "KNEE REPLACEMENT")
	assertStrPtrEq(t, "knee.MSDRGCode", knee.MSDRGCode, strPtr("470"))
	assertF64PtrEq(t, "knee.GrossCharge", knee.GrossCharge, f64Ptr(45000.00))
	// Knee has no payer info, but items file should still have it
	if knee.Setting != "inpatient" {
		t.Errorf("knee.Setting = %q, want inpatient", knee.Setting)
	}

	// Items should NOT have payer fields (they are ItemRow, no PayerName field)
	// This is enforced by the type system.

	// ── Payer file ──
	// X-RAY has 2 payers (Aetna, Blue Cross), IBUPROFEN has 1 (UHC), KNEE has 0
	payers := readPayerRows(t, result.PayerPath)
	if len(payers) != 3 {
		t.Errorf("payer count = %d, want 3", len(payers))
	}
	if result.PayerCount != 3 {
		t.Errorf("result.PayerCount = %d, want 3", result.PayerCount)
	}

	aetna := findPayer(t, payers, "X-RAY CHEST 2 VIEWS", "Aetna")
	assertStrPtrEq(t, "aetna.PlanName", aetna.PlanName, strPtr("Aetna PPO"))
	assertF64PtrEq(t, "aetna.NegotiatedDollar", aetna.NegotiatedDollar, f64Ptr(800.00))
	assertStrPtrEq(t, "aetna.Methodology", aetna.Methodology, strPtr("fee_schedule"))
	assertStrPtrEq(t, "aetna.CPTCode", aetna.CPTCode, strPtr("71046"))
	// Payer file should also have gross/discounted for overlay summary
	assertF64PtrEq(t, "aetna.GrossCharge", aetna.GrossCharge, f64Ptr(1250.00))

	bc := findPayer(t, payers, "X-RAY CHEST 2 VIEWS", "Blue Cross")
	assertStrPtrEq(t, "bc.PlanName", bc.PlanName, strPtr("BC Standard"))
	assertF64PtrEq(t, "bc.NegotiatedPercentage", bc.NegotiatedPercentage, f64Ptr(75.5))
	assertF64PtrEq(t, "bc.EstimatedAmount", bc.EstimatedAmount, f64Ptr(943.75))

	uhc := findPayer(t, payers, "IBUPROFEN 200MG TABLET", "UnitedHealthcare")
	assertF64PtrEq(t, "uhc.NegotiatedDollar", uhc.NegotiatedDollar, f64Ptr(8.00))
	assertStrPtrEq(t, "uhc.NegotiatedAlgorithm", uhc.NegotiatedAlgorithm, strPtr("per diem rate table v3"))

	// KNEE REPLACEMENT should have 0 payer rows
	if n := countPayersByDesc(payers, "KNEE REPLACEMENT"); n != 0 {
		t.Errorf("KNEE REPLACEMENT payer rows = %d, want 0", n)
	}

	// ── Code-info file ──
	// Unique codes: CPT/71046, RC/0324, NDC/00573-0150-20, HCPCS/J3490, MS-DRG/470
	// Plus modifiers: MODIFIER/26, MODIFIER/TC
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if len(codes) != 7 {
		t.Errorf("code-info count = %d, want 7", len(codes))
	}

	findCode(t, codes, "CPT", "71046")
	findCode(t, codes, "RC", "0324")
	findCode(t, codes, "NDC", "00573-0150-20")
	findCode(t, codes, "HCPCS", "J3490")
	findCode(t, codes, "MS-DRG", "470")
	findCode(t, codes, "MODIFIER", "26")
	findCode(t, codes, "MODIFIER", "TC")
}

func TestConvertSplitJSONV3(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v3.json")
	basePath := filepath.Join(t.TempDir(), "v3_split")

	result, err := ConvertSplit(discardLogger, jsonPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// ── Items file ──
	// 4 items: MRI BRAIN, ER VISIT, REHAB THERAPY, LONG TERM CARE
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 4 {
		t.Errorf("items count = %d, want 4", len(items))
	}

	mri := findItem(t, items, "MRI BRAIN WITHOUT CONTRAST")
	assertStrPtrEq(t, "mri.CPTCode", mri.CPTCode, strPtr("70551"))
	assertF64PtrEq(t, "mri.GrossCharge", mri.GrossCharge, f64Ptr(3500.00))
	assertF64PtrEq(t, "mri.DiscountedCash", mri.DiscountedCash, f64Ptr(1750.00))
	assertF64PtrEq(t, "mri.MinCharge", mri.MinCharge, f64Ptr(1200.00))
	assertF64PtrEq(t, "mri.MaxCharge", mri.MaxCharge, f64Ptr(4500.00))
	assertStrPtrEq(t, "mri.BillingClass", mri.BillingClass, strPtr("facility"))

	er := findItem(t, items, "EMERGENCY ROOM VISIT LEVEL 3")
	assertStrPtrEq(t, "er.CPTCode", er.CPTCode, strPtr("99283"))
	assertStrPtrEq(t, "er.RCCode", er.RCCode, strPtr("0450"))
	assertF64PtrEq(t, "er.GrossCharge", er.GrossCharge, f64Ptr(950.00))

	ltc := findItem(t, items, "LONG TERM CARE STAY")
	assertStrPtrEq(t, "ltc.MSLTCDRGCode", ltc.MSLTCDRGCode, strPtr("189"))
	assertF64PtrEq(t, "ltc.DrugUnitOfMeasurement", ltc.DrugUnitOfMeasurement, f64Ptr(100.0))
	assertStrPtrEq(t, "ltc.DrugTypeOfMeasurement", ltc.DrugTypeOfMeasurement, strPtr("UN"))

	// ── Payer file ──
	// MRI has 1 payer (Cigna), REHAB has 1 (Aetna), ER and LTC have 0
	payers := readPayerRows(t, result.PayerPath)
	if len(payers) != 2 {
		t.Errorf("payer count = %d, want 2", len(payers))
	}

	cigna := findPayer(t, payers, "MRI BRAIN WITHOUT CONTRAST", "Cigna")
	assertStrPtrEq(t, "cigna.PlanName", cigna.PlanName, strPtr("Cigna Open Access"))
	assertF64PtrEq(t, "cigna.NegotiatedDollar", cigna.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "cigna.Methodology", cigna.Methodology, strPtr("case_rate"))
	// V3 allowed-amount statistics
	assertF64PtrEq(t, "cigna.MedianAmount", cigna.MedianAmount, f64Ptr(2100.00))
	assertF64PtrEq(t, "cigna.Pct10Amount", cigna.Pct10Amount, f64Ptr(1500.00))
	assertF64PtrEq(t, "cigna.Pct90Amount", cigna.Pct90Amount, f64Ptr(3800.00))
	assertStrPtrEq(t, "cigna.AllowedCount", cigna.AllowedCount, strPtr("245"))
	// Code columns duplicated in payer file
	assertStrPtrEq(t, "cigna.CPTCode", cigna.CPTCode, strPtr("70551"))

	aetna := findPayer(t, payers, "REHAB THERAPY SESSION", "Aetna")
	assertF64PtrEq(t, "aetna.NegotiatedPercentage", aetna.NegotiatedPercentage, f64Ptr(65.0))
	assertStrPtrEq(t, "aetna.NegotiatedAlgorithm", aetna.NegotiatedAlgorithm, strPtr("65% of billed charges"))
	assertStrPtrEq(t, "aetna.AdditionalPayerNotes", aetna.AdditionalPayerNotes, strPtr("Subject to stop-loss"))
	assertStrPtrEq(t, "aetna.AllowedCount", aetna.AllowedCount, strPtr("1 through 10"))

	// ── Code-info file ──
	// CPT/70551, CPT/99283, RC/0450, CMG/0023, MS-LTC-DRG/189
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if len(codes) != 5 {
		t.Errorf("code-info count = %d, want 5", len(codes))
	}
	findCode(t, codes, "CPT", "70551")
	findCode(t, codes, "CPT", "99283")
	findCode(t, codes, "RC", "0450")
	findCode(t, codes, "CMG", "0023")
	findCode(t, codes, "MS-LTC-DRG", "189")
}

func TestConvertSplitTallCSV(t *testing.T) {
	csvPath := writeTallCSV(t)
	basePath := filepath.Join(t.TempDir(), "tall_split")

	result, err := ConvertSplit(discardLogger, csvPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// ── Items file ──
	// Tall CSV has 4 rows but only 3 unique items (ECHOCARDIOGRAM appears twice
	// with different payers — should deduplicate to 1 item row)
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 3 {
		t.Errorf("items count = %d, want 3", len(items))
	}

	echo := findItem(t, items, "ECHOCARDIOGRAM COMPLETE")
	assertStrPtrEq(t, "echo.CPTCode", echo.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "echo.HCPCSCode", echo.HCPCSCode, strPtr("G0389"))
	assertF64PtrEq(t, "echo.GrossCharge", echo.GrossCharge, f64Ptr(1500.00))
	assertF64PtrEq(t, "echo.DiscountedCash", echo.DiscountedCash, f64Ptr(750.00))
	assertF64PtrEq(t, "echo.MinCharge", echo.MinCharge, f64Ptr(500.00))
	assertF64PtrEq(t, "echo.MaxCharge", echo.MaxCharge, f64Ptr(2000.00))

	acet := findItem(t, items, "ACETAMINOPHEN 500MG TABLET")
	assertStrPtrEq(t, "acet.NDCCode", acet.NDCCode, strPtr("00456-0422-01"))
	assertF64PtrEq(t, "acet.DrugUnitOfMeasurement", acet.DrugUnitOfMeasurement, f64Ptr(500.0))
	assertStrPtrEq(t, "acet.DrugTypeOfMeasurement", acet.DrugTypeOfMeasurement, strPtr("ME"))

	heart := findItem(t, items, "HEART TRANSPLANT WITH MCC")
	assertStrPtrEq(t, "heart.MSDRGCode", heart.MSDRGCode, strPtr("001"))
	assertStrPtrEq(t, "heart.Modifiers", heart.Modifiers, strPtr("26 59"))

	// ── Payer file ──
	// ECHOCARDIOGRAM: 2 payers (Aetna, UHC), ACETAMINOPHEN: 1 (Cigna), HEART: 0
	payers := readPayerRows(t, result.PayerPath)
	if len(payers) != 3 {
		t.Errorf("payer count = %d, want 3", len(payers))
	}

	echoAetna := findPayer(t, payers, "ECHOCARDIOGRAM COMPLETE", "Aetna")
	assertF64PtrEq(t, "echoAetna.NegotiatedDollar", echoAetna.NegotiatedDollar, f64Ptr(900.00))
	assertStrPtrEq(t, "echoAetna.Methodology", echoAetna.Methodology, strPtr("fee_schedule"))
	// Verify code columns are duplicated in payer file
	assertStrPtrEq(t, "echoAetna.CPTCode", echoAetna.CPTCode, strPtr("93306"))
	assertStrPtrEq(t, "echoAetna.HCPCSCode", echoAetna.HCPCSCode, strPtr("G0389"))

	echoUHC := findPayer(t, payers, "ECHOCARDIOGRAM COMPLETE", "UnitedHealthcare")
	assertF64PtrEq(t, "echoUHC.NegotiatedDollar", echoUHC.NegotiatedDollar, f64Ptr(1100.00))
	assertStrPtrEq(t, "echoUHC.Methodology", echoUHC.Methodology, strPtr("case_rate"))

	if n := countPayersByDesc(payers, "HEART TRANSPLANT WITH MCC"); n != 0 {
		t.Errorf("HEART TRANSPLANT payer rows = %d, want 0", n)
	}

	// ── Code-info file ──
	// CPT/93306, HCPCS/G0389, NDC/00456-0422-01, MS-DRG/001
	// Plus MODIFIER/26, MODIFIER/59 from "26 59" modifiers on HEART TRANSPLANT
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if !hasCode(codes, "CPT", "93306") {
		t.Error("missing CPT/93306")
	}
	if !hasCode(codes, "HCPCS", "G0389") {
		t.Error("missing HCPCS/G0389")
	}
	if !hasCode(codes, "NDC", "00456-0422-01") {
		t.Error("missing NDC/00456-0422-01")
	}
	if !hasCode(codes, "MS-DRG", "001") {
		t.Error("missing MS-DRG/001")
	}
	if !hasCode(codes, "MODIFIER", "26") {
		t.Error("missing MODIFIER/26")
	}
	if !hasCode(codes, "MODIFIER", "59") {
		t.Error("missing MODIFIER/59")
	}
}

func TestConvertSplitWideCSV(t *testing.T) {
	csvPath := writeWideCSV(t)
	basePath := filepath.Join(t.TempDir(), "wide_split")

	result, err := ConvertSplit(discardLogger, csvPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// ── Items file ──
	// Wide CSV has 2 items: X-RAY CHEST, MRI BRAIN
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 2 {
		t.Errorf("items count = %d, want 2", len(items))
	}

	xray := findItem(t, items, "X-RAY CHEST")
	assertF64PtrEq(t, "xray.GrossCharge", xray.GrossCharge, f64Ptr(250.00))
	assertF64PtrEq(t, "xray.DiscountedCash", xray.DiscountedCash, f64Ptr(125.00))
	assertF64PtrEq(t, "xray.MinCharge", xray.MinCharge, f64Ptr(80.00))
	assertF64PtrEq(t, "xray.MaxCharge", xray.MaxCharge, f64Ptr(300.00))

	mri := findItem(t, items, "MRI BRAIN")
	assertF64PtrEq(t, "mri.GrossCharge", mri.GrossCharge, f64Ptr(3500.00))

	// ── Payer file ──
	// X-RAY: 2 payers (Aetna, UHC), MRI: 1 payer (Aetna only; UHC has empty dollar)
	payers := readPayerRows(t, result.PayerPath)

	xrayAetna := findPayer(t, payers, "X-RAY CHEST", "Aetna")
	assertF64PtrEq(t, "xrayAetna.NegotiatedDollar", xrayAetna.NegotiatedDollar, f64Ptr(150.00))
	assertStrPtrEq(t, "xrayAetna.Methodology", xrayAetna.Methodology, strPtr("fee_schedule"))

	xrayUHC := findPayer(t, payers, "X-RAY CHEST", "UHC")
	assertF64PtrEq(t, "xrayUHC.NegotiatedDollar", xrayUHC.NegotiatedDollar, f64Ptr(175.00))
	assertStrPtrEq(t, "xrayUHC.Methodology", xrayUHC.Methodology, strPtr("case_rate"))

	mriAetna := findPayer(t, payers, "MRI BRAIN", "Aetna")
	assertF64PtrEq(t, "mriAetna.NegotiatedDollar", mriAetna.NegotiatedDollar, f64Ptr(2200.00))
	assertStrPtrEq(t, "mriAetna.Methodology", mriAetna.Methodology, strPtr("per_diem"))

	// ── Code-info file ──
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if !hasCode(codes, "CPT", "71046") {
		t.Error("missing CPT/71046")
	}
	if !hasCode(codes, "CPT", "70553") {
		t.Error("missing CPT/70553")
	}
}

func TestConvertSplitCodeTypeFilter(t *testing.T) {
	// Test that code type filtering works with split
	jsonPath := filepath.Join("testdata", "test_v3.json")
	basePath := filepath.Join(t.TempDir(), "filtered_split")

	// Only include CPT codes
	codeTypes := map[string]bool{"CPT": true}

	result, err := ConvertSplit(discardLogger, jsonPath, basePath, 1000, codeTypes)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// Items with CPT codes: MRI BRAIN (70551), ER VISIT (99283)
	// REHAB (CMG only) and LONG TERM CARE (MS-LTC-DRG only) should be excluded
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 2 {
		t.Errorf("filtered items count = %d, want 2", len(items))
	}

	// MRI and ER should be present
	findItem(t, items, "MRI BRAIN WITHOUT CONTRAST")
	findItem(t, items, "EMERGENCY ROOM VISIT LEVEL 3")
}

func TestConvertSplitDefaultCodeTypes(t *testing.T) {
	// Verify that the default code-types flag value from convert-s3
	// ("CPT,HCPCS,CDM,LOCAL,DRG,MS-DRG,R-DRG,S-DRG,APS-DRG,AP-DRG,APR-DRG,TRIS-DRG,MS-LTC-DRG,NDC")
	// correctly includes/excludes items during conversion.
	//
	// test_v3.json contains:
	//   - MRI BRAIN WITHOUT CONTRAST  → CPT 70551          (included: CPT in defaults)
	//   - EMERGENCY ROOM VISIT LEVEL 3 → CPT 99283 + RC 0450 (included: CPT in defaults)
	//   - REHAB THERAPY SESSION        → CMG 0023           (excluded: CMG NOT in defaults)
	//   - LONG TERM CARE STAY          → MS-LTC-DRG 189    (included: MS-LTC-DRG in defaults)
	jsonPath := filepath.Join("testdata", "test_v3.json")
	basePath := filepath.Join(t.TempDir(), "default_code_types")

	// Mirror the default --code-types flag from convert-s3.
	defaultCodeTypesStr := "CPT,HCPCS,CDM,LOCAL,DRG,MS-DRG,R-DRG,S-DRG,APS-DRG,AP-DRG,APR-DRG,TRIS-DRG,MS-LTC-DRG,NDC"
	codeTypes := make(map[string]bool)
	for _, ct := range strings.Split(defaultCodeTypesStr, ",") {
		codeTypes[strings.TrimSpace(ct)] = true
	}

	result, err := ConvertSplit(discardLogger, jsonPath, basePath, 1000, codeTypes)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// ── Items: 3 of 4 should be included ──
	// MRI (CPT), ER (CPT), LONG TERM CARE (MS-LTC-DRG) → included
	// REHAB (CMG only) → excluded (CMG is not in the default set)
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 3 {
		var descs []string
		for _, it := range items {
			descs = append(descs, it.Description)
		}
		t.Fatalf("default code-types: items count = %d, want 3; got %v", len(items), descs)
	}

	findItem(t, items, "MRI BRAIN WITHOUT CONTRAST")
	findItem(t, items, "EMERGENCY ROOM VISIT LEVEL 3")
	findItem(t, items, "LONG TERM CARE STAY")

	// REHAB should NOT be present (CMG is excluded by defaults).
	for _, it := range items {
		if it.Description == "REHAB THERAPY SESSION" {
			t.Error("REHAB THERAPY SESSION should be excluded by default code-types (CMG not in defaults)")
		}
	}

	// ── Code-info: verify included code types appear ──
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if !hasCode(codes, "CPT", "70551") {
		t.Error("missing CPT/70551 (MRI)")
	}
	if !hasCode(codes, "CPT", "99283") {
		t.Error("missing CPT/99283 (ER)")
	}
	if !hasCode(codes, "MS-LTC-DRG", "189") {
		t.Error("missing MS-LTC-DRG/189 (LONG TERM CARE)")
	}
	// RC is NOT in the default set, but ER VISIT has both CPT and RC codes.
	// The item is included (CPT matches), but the RC code-info row should
	// still appear since code-info captures all codes for included items.
	if !hasCode(codes, "RC", "0450") {
		t.Error("missing RC/0450 — code-info should include all codes for items that pass the filter")
	}
	// CMG should NOT appear (REHAB was filtered out entirely).
	if hasCode(codes, "CMG", "0023") {
		t.Error("CMG/0023 should not appear — REHAB was excluded by code-type filter")
	}
}

func TestConvertSplitItemDeduplication(t *testing.T) {
	// Create a tall CSV where the same item appears with multiple payers.
	// Verify that the items file deduplicates to one row.
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "dedup.csv")

	content := `hospital_name,last_updated_on,version,hospital_location,hospital_address
Dedup Test Hospital,2024-01-01,2.0.0,Test Location,Test Address
description,setting,code|1,code|1|type,standard_charge|gross,standard_charge|discounted_cash,standard_charge|min,standard_charge|max,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|methodology
BLOOD TEST,outpatient,85025,CPT,100.00,50.00,30.00,150.00,Aetna,Aetna PPO,60.00,fee_schedule
BLOOD TEST,outpatient,85025,CPT,100.00,50.00,30.00,150.00,Cigna,Cigna HMO,55.00,fee_schedule
BLOOD TEST,outpatient,85025,CPT,100.00,50.00,30.00,150.00,UHC,UHC Choice,70.00,case_rate
`
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("write CSV: %v", err)
	}

	basePath := filepath.Join(dir, "dedup_split")
	result, err := ConvertSplit(discardLogger, csvPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// Items: 1 unique item
	items := readItemRows(t, result.ItemsPath)
	if len(items) != 1 {
		t.Errorf("items count = %d, want 1 (should deduplicate)", len(items))
	}
	assertF64PtrEq(t, "GrossCharge", items[0].GrossCharge, f64Ptr(100.00))

	// Payers: 3 payer rows
	payers := readPayerRows(t, result.PayerPath)
	if len(payers) != 3 {
		t.Errorf("payer count = %d, want 3", len(payers))
	}

	// Code-info: CPT/85025 (1 unique code)
	codes := readCodeInfoRows(t, result.CodeInfoPath)
	if len(codes) != 1 {
		t.Errorf("code-info count = %d, want 1", len(codes))
	}
	findCode(t, codes, "CPT", "85025")
}

func TestConvertSplitOutputPaths(t *testing.T) {
	jsonPath := filepath.Join("testdata", "test_v2.json")
	basePath := filepath.Join(t.TempDir(), "paths_test")

	result, err := ConvertSplit(discardLogger, jsonPath, basePath, 1000, nil)
	if err != nil {
		t.Fatalf("ConvertSplit: %v", err)
	}

	// Verify file naming convention
	wantItems := basePath + "-items.parquet"
	wantPayer := basePath + "-payer.parquet"
	wantCodeInfo := basePath + "-codeinfo.parquet"

	if result.ItemsPath != wantItems {
		t.Errorf("ItemsPath = %q, want %q", result.ItemsPath, wantItems)
	}
	if result.PayerPath != wantPayer {
		t.Errorf("PayerPath = %q, want %q", result.PayerPath, wantPayer)
	}
	if result.CodeInfoPath != wantCodeInfo {
		t.Errorf("CodeInfoPath = %q, want %q", result.CodeInfoPath, wantCodeInfo)
	}

	// Verify metadata is populated
	if result.Meta.HospitalName != "Test Community Hospital" {
		t.Errorf("Meta.HospitalName = %q, want %q", result.Meta.HospitalName, "Test Community Hospital")
	}
}
