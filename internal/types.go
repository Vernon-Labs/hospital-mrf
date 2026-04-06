package internal

// HospitalChargeRow is a denormalized Parquet row representing one
// charge line: one item/service × one payer/plan combination.
// Both Tall and Wide CSV formats normalize into this structure.
//
// Query-speed optimizations:
//
//   - Dedicated column per code type (all 19 CMS-defined types):
//     enables direct predicate pushdown — "WHERE cpt_code = '99213'"
//     scans a single column with min/max statistics skip, vs scanning
//     generic code+type pairs across N slots.
//
//   - Columns ordered by query frequency: codes and identifiers first,
//     charges second, hospital metadata and notes last. Improves
//     page-cache locality when engines read column chunks sequentially.
//
//   - String enums (setting, methodology, billing_class) dictionary-encode
//     automatically; engines like DuckDB resolve equality predicates to
//     integer dictionary-code comparisons — same speed as int8 enums but
//     human-readable in ad-hoc queries.
//
//   - Optional (*type) fields use Parquet native null bitmap — enables
//     IS NULL / IS NOT NULL pushdown and compact encoding.
//
// Compression optimizations:
//
//   - Hospital metadata repeats per row → dictionary + RLE to near-zero.
//   - Dedicated code columns are mostly null → null bitmap RLE (~1 bit/row).
//   - Recommended: Zstd(3) compression, 64MB row groups, bloom filters on
//     code and payer columns, page-level column indexes enabled.
//   - Sort rows by (description, payer_name) for maximum row-group skip
//     on the most common query patterns.
type HospitalChargeRow struct {
	// ── Primary query columns ─────────────────────────────────────────
	// Placed first for page-cache locality on common filter/join patterns.

	Description string `parquet:"description"`
	Setting     string `parquet:"setting"` // inpatient | outpatient | both

	// ── Billing codes (all 19 CMS-defined types) ──────────────────────
	// One dedicated column per code type. A query like
	//   SELECT * FROM charges WHERE cpt_code = '99213'
	// pushes down to a single column with row-group min/max skip and
	// optional bloom filter lookup — no scanning of type discriminators.
	//
	// Mostly-null columns cost ~1 bit/row (null bitmap RLE). A hospital
	// typically populates 2-4 code types per item; the remaining 15-17
	// columns are free.
	CPTCode     *string `parquet:"cpt_code,optional"`      // Current Procedural Terminology
	HCPCSCode   *string `parquet:"hcpcs_code,optional"`    // Healthcare Common Procedure Coding System
	MSDRGCode   *string `parquet:"ms_drg_code,optional"`   // Medicare Severity DRG
	NDCCode     *string `parquet:"ndc_code,optional"`      // National Drug Code
	RCCode      *string `parquet:"rc_code,optional"`       // Revenue Code
	ICDCode     *string `parquet:"icd_code,optional"`      // International Classification of Diseases
	DRGCode     *string `parquet:"drg_code,optional"`      // Diagnosis Related Groups (generic)
	CDMCode     *string `parquet:"cdm_code,optional"`      // Charge Description Master
	LOCALCode   *string `parquet:"local_code,optional"`    // Local/internal accounting code
	APCCode     *string `parquet:"apc_code,optional"`      // Ambulatory Payment Classifications
	EAPGCode    *string `parquet:"eapg_code,optional"`     // Enhanced Ambulatory Patient Grouping
	HIPPSCode   *string `parquet:"hipps_code,optional"`    // Health Insurance Prospective Payment System
	CDTCode     *string `parquet:"cdt_code,optional"`      // Current Dental Terminology
	RDRGCode    *string `parquet:"r_drg_code,optional"`    // Refined DRG
	SDRGCode    *string `parquet:"s_drg_code,optional"`    // Severity DRG
	APSDRGCode  *string `parquet:"aps_drg_code,optional"`  // All Patient Severity-Adjusted DRG
	APDRGCode   *string `parquet:"ap_drg_code,optional"`   // All Patient DRG
	APRDRGCode  *string `parquet:"apr_drg_code,optional"`  // All Patient Refined DRG
	TRISDRGCode    *string `parquet:"tris_drg_code,optional"`    // TriCare DRG
	CMGCode        *string `parquet:"cmg_code,optional"`         // Case Mix Group (V3)
	MSLTCDRGCode   *string `parquet:"ms_ltc_drg_code,optional"`  // Medicare Severity Long-Term Care DRG (V3)

	// ── Payer identification ──────────────────────────────────────────
	// Enable bloom filters on these — high-cardinality but frequently
	// filtered. Nil when the row only carries gross/discounted_cash.
	PayerName *string `parquet:"payer_name,optional"`
	PlanName  *string `parquet:"plan_name,optional"`

	// ── Charge amounts ────────────────────────────────────────────────
	GrossCharge          *float64 `parquet:"gross_charge,optional"`
	DiscountedCash       *float64 `parquet:"discounted_cash,optional"`
	NegotiatedDollar     *float64 `parquet:"negotiated_dollar,optional"`
	NegotiatedPercentage *float64 `parquet:"negotiated_percentage,optional"`
	NegotiatedAlgorithm  *string  `parquet:"negotiated_algorithm,optional"`
	EstimatedAmount      *float64 `parquet:"estimated_amount,optional"`
	MinCharge            *float64 `parquet:"min_charge,optional"`
	MaxCharge            *float64 `parquet:"max_charge,optional"`
	Methodology          *string  `parquet:"methodology,optional"` // case_rate|fee_schedule|percent_of_total_billed_charges|per_diem|other

	// ── V3 allowed-amount statistics (payer-level) ────────────────────
	MedianAmount *float64 `parquet:"median_amount,optional"`
	Pct10Amount  *float64 `parquet:"pct10_amount,optional"`  // 10th percentile
	Pct90Amount  *float64 `parquet:"pct90_amount,optional"`  // 90th percentile
	AllowedCount *string  `parquet:"allowed_count,optional"` // "0", "1 through 10", or whole number ≥11

	// ── Drug information ──────────────────────────────────────────────
	DrugUnitOfMeasurement *float64 `parquet:"drug_unit_of_measurement,optional"`
	DrugTypeOfMeasurement *string  `parquet:"drug_type_of_measurement,optional"` // GR|ME|ML|UN|F2|EA|GM

	// ── Modifiers & notes ─────────────────────────────────────────────
	Modifiers              *string `parquet:"modifiers,optional"`
	AdditionalGenericNotes *string `parquet:"additional_generic_notes,optional"`
	AdditionalPayerNotes   *string `parquet:"additional_payer_notes,optional"`

	// ── Optional fields (v2.1+) ───────────────────────────────────────
	BillingClass              *string `parquet:"billing_class,optional"` // professional|facility|both
	FinancialAidPolicy        *string `parquet:"financial_aid_policy,optional"`
	GeneralContractProvisions *string `parquet:"general_contract_provisions,optional"`

	// ── Hospital metadata (CSV row 1) ─────────────────────────────────
	// Identical across all rows in a single file. Dictionary + RLE
	// compresses to near-zero. Placed last — rarely filtered, only
	// needed in SELECT projections.
	HospitalName     string  `parquet:"hospital_name"`
	LastUpdatedOn    string  `parquet:"last_updated_on"`
	Version          string  `parquet:"version"`
	HospitalLocation string  `parquet:"hospital_location"`
	HospitalAddress  string  `parquet:"hospital_address"`
	LicenseNumber    *string `parquet:"license_number,optional"`
	LicenseState     *string `parquet:"license_state,optional"`
	Affirmation      bool    `parquet:"affirmation"`
}

// codeTypeToField maps CSV code|type values to their dedicated Parquet column.
var codeTypeToField = map[string]func(*HospitalChargeRow, string){
	"CPT":      func(r *HospitalChargeRow, v string) { r.CPTCode = &v },
	"HCPCS":    func(r *HospitalChargeRow, v string) { r.HCPCSCode = &v },
	"MS-DRG":   func(r *HospitalChargeRow, v string) { r.MSDRGCode = &v },
	"NDC":      func(r *HospitalChargeRow, v string) { r.NDCCode = &v },
	"RC":       func(r *HospitalChargeRow, v string) { r.RCCode = &v },
	"ICD":      func(r *HospitalChargeRow, v string) { r.ICDCode = &v },
	"DRG":      func(r *HospitalChargeRow, v string) { r.DRGCode = &v },
	"CDM":      func(r *HospitalChargeRow, v string) { r.CDMCode = &v },
	"LOCAL":    func(r *HospitalChargeRow, v string) { r.LOCALCode = &v },
	"APC":      func(r *HospitalChargeRow, v string) { r.APCCode = &v },
	"EAPG":     func(r *HospitalChargeRow, v string) { r.EAPGCode = &v },
	"HIPPS":    func(r *HospitalChargeRow, v string) { r.HIPPSCode = &v },
	"CDT":      func(r *HospitalChargeRow, v string) { r.CDTCode = &v },
	"R-DRG":    func(r *HospitalChargeRow, v string) { r.RDRGCode = &v },
	"S-DRG":    func(r *HospitalChargeRow, v string) { r.SDRGCode = &v },
	"APS-DRG":  func(r *HospitalChargeRow, v string) { r.APSDRGCode = &v },
	"AP-DRG":   func(r *HospitalChargeRow, v string) { r.APDRGCode = &v },
	"APR-DRG":  func(r *HospitalChargeRow, v string) { r.APRDRGCode = &v },
	"TRIS-DRG":   func(r *HospitalChargeRow, v string) { r.TRISDRGCode = &v },
	"CMG":        func(r *HospitalChargeRow, v string) { r.CMGCode = &v },
	"MS-LTC-DRG": func(r *HospitalChargeRow, v string) { r.MSLTCDRGCode = &v },
}

// SetCode assigns a code value to the dedicated column for its type.
// Returns false only if the code type is completely unknown (not one of
// the 19 CMS-defined types).
func (r *HospitalChargeRow) SetCode(codeType, codeValue string) bool {
	if setter, ok := codeTypeToField[codeType]; ok {
		setter(r, codeValue)
		return true
	}
	return false
}

// HasAnyCodeType returns true if the row has a non-nil value for at least
// one of the code types in the allowed set. The keys in allowed must be
// uppercase CMS code type strings (e.g. "CPT", "HCPCS", "MS-DRG").
func (r *HospitalChargeRow) HasAnyCodeType(allowed map[string]bool) bool {
	for codeType := range allowed {
		switch codeType {
		case "CPT":
			if r.CPTCode != nil {
				return true
			}
		case "HCPCS":
			if r.HCPCSCode != nil {
				return true
			}
		case "MS-DRG":
			if r.MSDRGCode != nil {
				return true
			}
		case "NDC":
			if r.NDCCode != nil {
				return true
			}
		case "RC":
			if r.RCCode != nil {
				return true
			}
		case "ICD":
			if r.ICDCode != nil {
				return true
			}
		case "DRG":
			if r.DRGCode != nil {
				return true
			}
		case "CDM":
			if r.CDMCode != nil {
				return true
			}
		case "LOCAL":
			if r.LOCALCode != nil {
				return true
			}
		case "APC":
			if r.APCCode != nil {
				return true
			}
		case "EAPG":
			if r.EAPGCode != nil {
				return true
			}
		case "HIPPS":
			if r.HIPPSCode != nil {
				return true
			}
		case "CDT":
			if r.CDTCode != nil {
				return true
			}
		case "R-DRG":
			if r.RDRGCode != nil {
				return true
			}
		case "S-DRG":
			if r.SDRGCode != nil {
				return true
			}
		case "APS-DRG":
			if r.APSDRGCode != nil {
				return true
			}
		case "AP-DRG":
			if r.APDRGCode != nil {
				return true
			}
		case "APR-DRG":
			if r.APRDRGCode != nil {
				return true
			}
		case "TRIS-DRG":
			if r.TRISDRGCode != nil {
				return true
			}
		case "CMG":
			if r.CMGCode != nil {
				return true
			}
		case "MS-LTC-DRG":
			if r.MSLTCDRGCode != nil {
				return true
			}
		}
	}
	return false
}
