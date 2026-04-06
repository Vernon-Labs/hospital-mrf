package internal

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	codeColRe = regexp.MustCompile(`^code\|(\d+)$`)
)

type csvFormat int

const (
	formatTall csvFormat = iota
	formatWide
)

type hospitalMeta struct {
	hospitalName              string
	lastUpdatedOn             string
	version                   string
	hospitalLocation          string
	hospitalAddress           string
	hospitalLocations         []string
	hospitalAddresses         []string
	type2NPIs                 []string
	licenseNumber             *string
	licenseState              *string
	affirmation               bool
	financialAidPolicy        *string
	generalContractProvisions *string
}

type codeColPair struct {
	codeIdx int
	typeIdx int // -1 if no matching type column
}

// payerPlanCols holds column indices for one payer/plan in Wide format.
// -1 means column not present.
type payerPlanCols struct {
	payer     string
	plan      string
	dollarIdx int
	pctIdx    int
	algoIdx   int
	estIdx    int
	methodIdx int
	notesIdx  int
	// V3 allowed-amount statistics
	medianIdx int
	p10Idx    int
	p90Idx    int
	countIdx  int
}

// CSVReader streams a CMS V2.x CSV file (Tall or Wide) and emits
// HospitalChargeRow records one CSV row at a time.
type CSVReader struct {
	file    *os.File
	csv     *csv.Reader
	format  csvFormat
	rowNum  int64
	colIdx  map[string]int // lowercase normalized key → column index
	headers []string       // normalized (trimmed pipe-segments), original case
	meta    hospitalMeta

	codeCols         []codeColPair
	payerPlans       []payerPlanCols // Wide format only
	SkipPayerCharges bool
	seenItems        map[string]bool // Tall dedup when SkipPayerCharges
}

func NewCSVReader(filepath string) (*CSVReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filepath, err)
	}

	bufReader := bufio.NewReaderSize(file, 256*1024)

	// Skip UTF-8 BOM if present
	bom, err := bufReader.Peek(3)
	if err == nil && len(bom) >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		bufReader.Discard(3)
	}

	reader := csv.NewReader(bufReader)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	r := &CSVReader{
		file:   file,
		csv:    reader,
		colIdx: make(map[string]int),
	}

	if err := r.readHeaders(); err != nil {
		file.Close()
		return nil, err
	}

	return r, nil
}

// normalizeHeader trims spaces around each pipe-separated segment.
// "code|1| type" → "code|1|type"
// "standard_charge|Aetna|PPO Gold|negotiated_dollar" → unchanged
func normalizeHeader(h string) string {
	parts := strings.Split(h, "|")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return strings.Join(parts, "|")
}

func (r *CSVReader) readHeaders() error {
	// Row 1: header field names
	headerRow, err := r.csv.Read()
	if err != nil {
		return fmt.Errorf("read header row 1: %w", err)
	}
	r.rowNum++
	if len(headerRow) > 0 {
		headerRow[0] = strings.TrimPrefix(headerRow[0], "\ufeff")
	}

	// Row 2: header values
	valueRow, err := r.csv.Read()
	if err != nil {
		return fmt.Errorf("read header row 2: %w", err)
	}
	r.rowNum++

	r.parseHeaderMeta(headerRow, valueRow)

	// Row 3: data column headers
	r.headers, err = r.csv.Read()
	if err != nil {
		return fmt.Errorf("read column headers row 3: %w", err)
	}
	r.rowNum++

	// Normalize headers and build lowercase index for case-insensitive lookup.
	// r.headers keeps normalized original case (for payer name extraction).
	// r.colIdx uses lowercase keys (for structural lookups like "description").
	for i, h := range r.headers {
		h = normalizeHeader(strings.TrimSpace(h))
		r.headers[i] = h
		r.colIdx[strings.ToLower(h)] = i
	}

	r.format = r.detectFormat()
	r.extractCodeCols()
	if r.format == formatWide {
		r.extractPayerPlans()
	}

	return nil
}

// stripQuotes removes a matched pair of surrounding double quotes from a
// string. Some hospital CSVs double-quote fields that contain commas, and
// Go's csv.Reader with LazyQuotes may leave the inner quotes intact.
func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func (r *CSVReader) parseHeaderMeta(headerRow, valueRow []string) {
	for i, col := range headerRow {
		if i >= len(valueRow) {
			break
		}
		col = strings.TrimSpace(col)
		val := stripQuotes(strings.TrimSpace(valueRow[i]))

		switch {
		case strings.EqualFold(col, "hospital_name"):
			r.meta.hospitalName = val
		case strings.EqualFold(col, "last_updated_on"):
			r.meta.lastUpdatedOn = val
		case strings.EqualFold(col, "version"):
			r.meta.version = val
		case strings.EqualFold(col, "hospital_location"), strings.EqualFold(col, "location_name"):
			r.meta.hospitalLocation = val
			if val != "" {
				r.meta.hospitalLocations = strings.Split(val, "|")
				for i := range r.meta.hospitalLocations {
					r.meta.hospitalLocations[i] = strings.TrimSpace(r.meta.hospitalLocations[i])
				}
			}
		case strings.EqualFold(col, "hospital_address"):
			r.meta.hospitalAddress = val
			if val != "" {
				r.meta.hospitalAddresses = strings.Split(val, "|")
				for i := range r.meta.hospitalAddresses {
					r.meta.hospitalAddresses[i] = strings.TrimSpace(r.meta.hospitalAddresses[i])
				}
			}
		case strings.EqualFold(col, "type_2_npi"):
			if val != "" {
				r.meta.type2NPIs = strings.Split(val, "|")
				for i := range r.meta.type2NPIs {
					r.meta.type2NPIs[i] = strings.TrimSpace(r.meta.type2NPIs[i])
				}
			}
		case strings.HasPrefix(strings.ToLower(col), "license_number|"):
			parts := strings.SplitN(col, "|", 2)
			if len(parts) == 2 && r.meta.licenseState == nil {
				state := strings.TrimSpace(parts[1])
				if state != "" {
					r.meta.licenseState = &state
				}
				if val != "" {
					r.meta.licenseNumber = &val
				}
			}
		case strings.Contains(strings.ToLower(col), "knowledge and belief"):
			r.meta.affirmation = strings.EqualFold(val, "true")
		case strings.EqualFold(col, "financial_aid_policy"):
			if val != "" {
				r.meta.financialAidPolicy = &val
			}
		case strings.EqualFold(col, "general_contract_provisions"):
			if val != "" {
				r.meta.generalContractProvisions = &val
			}
		}
	}
}

func (r *CSVReader) detectFormat() csvFormat {
	// Tall format has explicit payer_name/plan_name columns.
	// Check via direct map lookup before scanning for Wide indicators,
	// since map iteration order is non-deterministic.
	if _, ok := r.colIdx["payer_name"]; ok {
		return formatTall
	}
	if _, ok := r.colIdx["plan_name"]; ok {
		return formatTall
	}
	for k := range r.colIdx {
		if strings.Contains(k, "|") && strings.Contains(k, "negotiated_dollar") {
			return formatWide
		}
	}
	return formatTall
}

func (r *CSVReader) extractCodeCols() {
	// Match against lowercase keys to handle "Code|1" or "code|1"
	for lk, idx := range r.colIdx {
		if m := codeColRe.FindStringSubmatch(lk); m != nil {
			typeKey := fmt.Sprintf("code|%s|type", m[1])
			pair := codeColPair{codeIdx: idx, typeIdx: -1}
			if tIdx, ok := r.colIdx[typeKey]; ok {
				pair.typeIdx = tIdx
			}
			r.codeCols = append(r.codeCols, pair)
		}
	}
}

// extractPayerPlans finds payer/plan column groups from Wide format headers.
// Uses original-case headers (r.headers) to preserve payer/plan name casing,
// while matching structural prefixes case-insensitively.
func (r *CSVReader) extractPayerPlans() {
	seen := make(map[string]int) // "payer\x00plan" → index

	ensurePP := func(payer, plan string) int {
		payer = strings.ToValidUTF8(payer, "\uFFFD")
		plan = strings.ToValidUTF8(plan, "\uFFFD")
		key := payer + "\x00" + plan
		if idx, ok := seen[key]; ok {
			return idx
		}
		idx := len(r.payerPlans)
		r.payerPlans = append(r.payerPlans, payerPlanCols{
			payer: payer, plan: plan,
			dollarIdx: -1, pctIdx: -1, algoIdx: -1,
			estIdx: -1, methodIdx: -1, notesIdx: -1,
			medianIdx: -1, p10Idx: -1, p90Idx: -1, countIdx: -1,
		})
		seen[key] = idx
		return idx
	}

	for i, h := range r.headers {
		parts := strings.Split(h, "|")

		// standard_charge|<payer>|<plan>|<suffix> (4+ parts)
		if len(parts) >= 4 && strings.EqualFold(parts[0], "standard_charge") {
			// Rejoin middle parts as plan name in case plan contains "|"
			// But typically payer=parts[1], plan=parts[2], suffix=last part
			suffix := strings.ToLower(parts[len(parts)-1])
			payer := parts[1]
			plan := strings.Join(parts[2:len(parts)-1], "|")

			switch suffix {
			case "negotiated_dollar", "negotiated_percentage", "negotiated_algorithm", "methodology":
				idx := ensurePP(payer, plan)
				pp := &r.payerPlans[idx]
				switch suffix {
				case "negotiated_dollar":
					pp.dollarIdx = i
				case "negotiated_percentage":
					pp.pctIdx = i
				case "negotiated_algorithm":
					pp.algoIdx = i
				case "methodology":
					pp.methodIdx = i
				}
			}
		}

		// estimated_amount|<payer>|<plan> (3+ parts)
		if len(parts) >= 3 && strings.EqualFold(parts[0], "estimated_amount") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].estIdx = i
		}

		// additional_payer_notes|<payer>|<plan> (3+ parts)
		if len(parts) >= 3 && strings.EqualFold(parts[0], "additional_payer_notes") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].notesIdx = i
		}

		// V3 Wide: median_amount|<payer>|<plan>
		if len(parts) >= 3 && strings.EqualFold(parts[0], "median_amount") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].medianIdx = i
		}

		// V3 Wide: 10th_percentile|<payer>|<plan>
		if len(parts) >= 3 && strings.EqualFold(parts[0], "10th_percentile") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].p10Idx = i
		}

		// V3 Wide: 90th_percentile|<payer>|<plan>
		if len(parts) >= 3 && strings.EqualFold(parts[0], "90th_percentile") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].p90Idx = i
		}

		// V3 Wide: count|<payer>|<plan>
		if len(parts) >= 3 && strings.EqualFold(parts[0], "count") {
			payer := parts[1]
			plan := strings.Join(parts[2:], "|")
			idx := ensurePP(payer, plan)
			r.payerPlans[idx].countIdx = i
		}
	}
}

// Next returns the HospitalChargeRow(s) for the next CSV data row.
// Tall: 1 row. Wide: N rows (one per payer/plan with data).
// Returns nil, io.EOF when done.
func (r *CSVReader) Next() ([]HospitalChargeRow, error) {
	for {
		row, err := r.csv.Read()
		if err != nil {
			return nil, err
		}
		r.rowNum++

		// Skip empty rows
		if len(row) == 0 || (len(row) == 1 && row[0] == "") {
			continue
		}

		if r.format == formatTall {
			return r.parseTallRow(row), nil
		}
		return r.parseWideRow(row), nil
	}
}

func (r *CSVReader) parseTallRow(row []string) []HospitalChargeRow {
	base := r.baseRow(row)

	if r.SkipPayerCharges {
		// Dedup: Tall format has one row per payer/plan. With payer fields
		// stripped, many rows collapse to the same item. Emit only the first.
		key := chargeDedupKey(&base)
		if r.seenItems == nil {
			r.seenItems = make(map[string]bool)
		}
		if r.seenItems[key] {
			return nil
		}
		r.seenItems[key] = true
		return []HospitalChargeRow{base}
	}

	base.PayerName = optStr(row, r.colIdx, "payer_name")
	base.PlanName = optStr(row, r.colIdx, "plan_name")
	base.NegotiatedDollar = optFloat(row, r.colIdx, "standard_charge|negotiated_dollar")
	base.NegotiatedPercentage = optFloat(row, r.colIdx, "standard_charge|negotiated_percentage")
	base.NegotiatedAlgorithm = optStr(row, r.colIdx, "standard_charge|negotiated_algorithm")
	base.EstimatedAmount = optFloat(row, r.colIdx, "estimated_amount")
	base.Methodology = optStr(row, r.colIdx, "standard_charge|methodology")

	// V3 allowed-amount statistics
	base.MedianAmount = optFloat(row, r.colIdx, "median_amount")
	base.Pct10Amount = optFloat(row, r.colIdx, "10th_percentile")
	base.Pct90Amount = optFloat(row, r.colIdx, "90th_percentile")
	base.AllowedCount = optStr(row, r.colIdx, "count")

	return []HospitalChargeRow{base}
}

func (r *CSVReader) parseWideRow(row []string) []HospitalChargeRow {
	base := r.baseRow(row)

	if r.SkipPayerCharges {
		return []HospitalChargeRow{base}
	}

	var rows []HospitalChargeRow
	for i := range r.payerPlans {
		pp := &r.payerPlans[i]

		dollar := floatAt(row, pp.dollarIdx)
		pct := floatAt(row, pp.pctIdx)
		algo := strAt(row, pp.algoIdx)
		est := floatAt(row, pp.estIdx)
		method := strAt(row, pp.methodIdx)
		notes := strAt(row, pp.notesIdx)
		median := floatAt(row, pp.medianIdx)
		p10 := floatAt(row, pp.p10Idx)
		p90 := floatAt(row, pp.p90Idx)
		cnt := strAt(row, pp.countIdx)

		if dollar == nil && pct == nil && algo == nil && est == nil && method == nil && notes == nil &&
			median == nil && p10 == nil && p90 == nil && cnt == nil {
			continue
		}

		prow := base // struct copy
		payer := strings.ReplaceAll(pp.payer, "_", " ")
		plan := strings.ReplaceAll(pp.plan, "_", " ")
		prow.PayerName = &payer
		prow.PlanName = &plan
		prow.NegotiatedDollar = dollar
		prow.NegotiatedPercentage = pct
		prow.NegotiatedAlgorithm = algo
		prow.EstimatedAmount = est
		prow.Methodology = method
		prow.AdditionalPayerNotes = notes
		prow.MedianAmount = median
		prow.Pct10Amount = p10
		prow.Pct90Amount = p90
		prow.AllowedCount = cnt
		rows = append(rows, prow)
	}

	// Still emit the base row if no payer data (gross/discounted_cash only)
	if len(rows) == 0 {
		rows = append(rows, base)
	}

	return rows
}

// baseRow creates a HospitalChargeRow with fields common to Tall and Wide.
func (r *CSVReader) baseRow(row []string) HospitalChargeRow {
	hr := HospitalChargeRow{
		HospitalName:              r.meta.hospitalName,
		LastUpdatedOn:             r.meta.lastUpdatedOn,
		Version:                   r.meta.version,
		HospitalLocation:          r.meta.hospitalLocation,
		HospitalAddress:           r.meta.hospitalAddress,
		LicenseNumber:             r.meta.licenseNumber,
		LicenseState:              r.meta.licenseState,
		Affirmation:               r.meta.affirmation,
		FinancialAidPolicy:        r.meta.financialAidPolicy,
		GeneralContractProvisions: r.meta.generalContractProvisions,

		Description: valAt(row, r.colIdx, "description"),
		Setting:     valAt(row, r.colIdx, "setting"),

		GrossCharge:    optFloat(row, r.colIdx, "standard_charge|gross"),
		DiscountedCash: optFloat(row, r.colIdx, "standard_charge|discounted_cash"),
		MinCharge:      optFloat(row, r.colIdx, "standard_charge|min"),
		MaxCharge:      optFloat(row, r.colIdx, "standard_charge|max"),

		DrugUnitOfMeasurement: optFloat(row, r.colIdx, "drug_unit_of_measurement"),
		DrugTypeOfMeasurement: optStr(row, r.colIdx, "drug_type_of_measurement"),

		Modifiers:              optStr(row, r.colIdx, "modifiers"),
		AdditionalGenericNotes: optStr(row, r.colIdx, "additional_generic_notes"),
		BillingClass:           optStr(row, r.colIdx, "billing_class"),
	}

	for _, cc := range r.codeCols {
		if cc.codeIdx >= len(row) {
			continue
		}
		codeVal := strings.ToValidUTF8(strings.TrimSpace(row[cc.codeIdx]), "")
		if codeVal == "" {
			continue
		}
		codeType := ""
		if cc.typeIdx >= 0 && cc.typeIdx < len(row) {
			codeType = strings.ToUpper(strings.TrimSpace(row[cc.typeIdx]))
		}
		if codeType != "" {
			hr.SetCode(codeType, codeVal)
		}
	}

	return hr
}

// Format returns "tall" or "wide".
func (r *CSVReader) Format() string {
	if r.format == formatWide {
		return "wide"
	}
	return "tall"
}

// RowNum returns the current CSV row number (1-based).
func (r *CSVReader) RowNum() int64 {
	return r.rowNum
}

// PayerPlanCount returns the number of payer/plan combinations (Wide only).
func (r *CSVReader) PayerPlanCount() int {
	return len(r.payerPlans)
}

// Meta returns the hospital metadata parsed from the CSV header rows.
func (r *CSVReader) Meta() RunMeta {
	return RunMeta{
		HospitalName:      r.meta.hospitalName,
		LocationNames:     r.meta.hospitalLocations,
		HospitalAddresses: r.meta.hospitalAddresses,
		LicenseNumber:     r.meta.licenseNumber,
		LicenseState:      r.meta.licenseState,
		Type2NPIs:         r.meta.type2NPIs,
		LastUpdatedOn:     r.meta.lastUpdatedOn,
		Version:           r.meta.version,
	}
}

func (r *CSVReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// chargeDedupKey builds a dedup key from an item's identity fields
// (description, setting, and billing codes). Used to collapse duplicate
// Tall-format rows when payer fields are stripped.
func chargeDedupKey(r *HospitalChargeRow) string {
	var b strings.Builder
	b.WriteString(r.Description)
	b.WriteByte('|')
	b.WriteString(r.Setting)
	optKey := func(v *string) {
		if v != nil {
			b.WriteByte('|')
			b.WriteString(*v)
		}
	}
	optKey(r.CPTCode)
	optKey(r.HCPCSCode)
	optKey(r.MSDRGCode)
	optKey(r.NDCCode)
	optKey(r.RCCode)
	optKey(r.ICDCode)
	optKey(r.DRGCode)
	optKey(r.CDMCode)
	optKey(r.LOCALCode)
	optKey(r.APCCode)
	optKey(r.EAPGCode)
	optKey(r.HIPPSCode)
	optKey(r.CDTCode)
	optKey(r.RDRGCode)
	optKey(r.SDRGCode)
	optKey(r.APSDRGCode)
	optKey(r.APDRGCode)
	optKey(r.APRDRGCode)
	optKey(r.TRISDRGCode)
	optKey(r.CMGCode)
	optKey(r.MSLTCDRGCode)
	return b.String()
}

// Column access helpers — standalone functions to avoid method receiver overhead
// on the hot path (called millions of times for large files).
// All string helpers sanitize to valid UTF-8 (Parquet requirement) since some
// hospital CSV files use Windows-1252 encoding.

func valAt(row []string, idx map[string]int, col string) string {
	if i, ok := idx[col]; ok && i < len(row) {
		return strings.ToValidUTF8(strings.TrimSpace(row[i]), "\uFFFD")
	}
	return ""
}

func optStr(row []string, idx map[string]int, col string) *string {
	if i, ok := idx[col]; ok && i < len(row) {
		s := strings.ToValidUTF8(strings.TrimSpace(row[i]), "\uFFFD")
		if s != "" {
			return &s
		}
	}
	return nil
}

func optFloat(row []string, idx map[string]int, col string) *float64 {
	if i, ok := idx[col]; ok && i < len(row) {
		return parseFloat(row[i])
	}
	return nil
}

func strAt(row []string, i int) *string {
	if i >= 0 && i < len(row) {
		s := strings.ToValidUTF8(strings.TrimSpace(row[i]), "\uFFFD")
		if s != "" {
			return &s
		}
	}
	return nil
}

func floatAt(row []string, i int) *float64 {
	if i >= 0 && i < len(row) {
		return parseFloat(row[i])
	}
	return nil
}

func parseFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "$", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// Ensure we use io.EOF for the interface contract.
var _ = io.EOF
