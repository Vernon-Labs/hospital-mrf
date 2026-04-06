package internal

// ItemRow is the Parquet schema for the items file.
// One row per unique item (description + setting + codes + modifiers + rc_code).
// Contains only high-level pricing — no per-payer data.
type ItemRow struct {
	Description string `parquet:"description"`
	Setting     string `parquet:"setting"`

	// Billing codes (all 19 CMS types)
	CPTCode        *string `parquet:"cpt_code,optional"`
	HCPCSCode      *string `parquet:"hcpcs_code,optional"`
	MSDRGCode      *string `parquet:"ms_drg_code,optional"`
	NDCCode        *string `parquet:"ndc_code,optional"`
	RCCode         *string `parquet:"rc_code,optional"`
	ICDCode        *string `parquet:"icd_code,optional"`
	DRGCode        *string `parquet:"drg_code,optional"`
	CDMCode        *string `parquet:"cdm_code,optional"`
	LOCALCode      *string `parquet:"local_code,optional"`
	APCCode        *string `parquet:"apc_code,optional"`
	EAPGCode       *string `parquet:"eapg_code,optional"`
	HIPPSCode      *string `parquet:"hipps_code,optional"`
	CDTCode        *string `parquet:"cdt_code,optional"`
	RDRGCode       *string `parquet:"r_drg_code,optional"`
	SDRGCode       *string `parquet:"s_drg_code,optional"`
	APSDRGCode     *string `parquet:"aps_drg_code,optional"`
	APDRGCode      *string `parquet:"ap_drg_code,optional"`
	APRDRGCode     *string `parquet:"apr_drg_code,optional"`
	TRISDRGCode    *string `parquet:"tris_drg_code,optional"`
	CMGCode        *string `parquet:"cmg_code,optional"`
	MSLTCDRGCode   *string `parquet:"ms_ltc_drg_code,optional"`

	// Item-level charges
	GrossCharge    *float64 `parquet:"gross_charge,optional"`
	DiscountedCash *float64 `parquet:"discounted_cash,optional"`
	MinCharge      *float64 `parquet:"min_charge,optional"`
	MaxCharge      *float64 `parquet:"max_charge,optional"`

	// Modifiers & notes
	Modifiers              *string `parquet:"modifiers,optional"`
	AdditionalGenericNotes *string `parquet:"additional_generic_notes,optional"`

	// Drug information
	DrugUnitOfMeasurement *float64 `parquet:"drug_unit_of_measurement,optional"`
	DrugTypeOfMeasurement *string  `parquet:"drug_type_of_measurement,optional"`

	BillingClass *string `parquet:"billing_class,optional"`
}

// PayerRow is the Parquet schema for the payer file.
// One row per item × payer/plan combination.
type PayerRow struct {
	Description string `parquet:"description"`
	Setting     string `parquet:"setting"`

	// Billing codes (duplicated for direct filtering without joins)
	CPTCode        *string `parquet:"cpt_code,optional"`
	HCPCSCode      *string `parquet:"hcpcs_code,optional"`
	MSDRGCode      *string `parquet:"ms_drg_code,optional"`
	NDCCode        *string `parquet:"ndc_code,optional"`
	RCCode         *string `parquet:"rc_code,optional"`
	ICDCode        *string `parquet:"icd_code,optional"`
	DRGCode        *string `parquet:"drg_code,optional"`
	CDMCode        *string `parquet:"cdm_code,optional"`
	LOCALCode      *string `parquet:"local_code,optional"`
	APCCode        *string `parquet:"apc_code,optional"`
	EAPGCode       *string `parquet:"eapg_code,optional"`
	HIPPSCode      *string `parquet:"hipps_code,optional"`
	CDTCode        *string `parquet:"cdt_code,optional"`
	RDRGCode       *string `parquet:"r_drg_code,optional"`
	SDRGCode       *string `parquet:"s_drg_code,optional"`
	APSDRGCode     *string `parquet:"aps_drg_code,optional"`
	APDRGCode      *string `parquet:"ap_drg_code,optional"`
	APRDRGCode     *string `parquet:"apr_drg_code,optional"`
	TRISDRGCode    *string `parquet:"tris_drg_code,optional"`
	CMGCode        *string `parquet:"cmg_code,optional"`
	MSLTCDRGCode   *string `parquet:"ms_ltc_drg_code,optional"`

	// Payer identification
	PayerName *string `parquet:"payer_name,optional"`
	PlanName  *string `parquet:"plan_name,optional"`

	// Item-level charges (needed for overlay summary bar)
	GrossCharge    *float64 `parquet:"gross_charge,optional"`
	DiscountedCash *float64 `parquet:"discounted_cash,optional"`

	// Payer-specific charges
	NegotiatedDollar     *float64 `parquet:"negotiated_dollar,optional"`
	NegotiatedPercentage *float64 `parquet:"negotiated_percentage,optional"`
	NegotiatedAlgorithm  *string  `parquet:"negotiated_algorithm,optional"`
	EstimatedAmount      *float64 `parquet:"estimated_amount,optional"`
	MinCharge            *float64 `parquet:"min_charge,optional"`
	MaxCharge            *float64 `parquet:"max_charge,optional"`
	Methodology          *string  `parquet:"methodology,optional"`

	// V3 allowed-amount statistics
	MedianAmount *float64 `parquet:"median_amount,optional"`
	Pct10Amount  *float64 `parquet:"pct10_amount,optional"`
	Pct90Amount  *float64 `parquet:"pct90_amount,optional"`
	AllowedCount *string  `parquet:"allowed_count,optional"`

	// Notes
	Modifiers            *string `parquet:"modifiers,optional"`
	AdditionalPayerNotes *string `parquet:"additional_payer_notes,optional"`
	AdditionalGenericNotes *string `parquet:"additional_generic_notes,optional"`
	BillingClass         *string `parquet:"billing_class,optional"`
}

// CodeInfoRow is the Parquet schema for the code/modifier metadata file.
// A deduplicated lookup table of code descriptions and modifier descriptions.
type CodeInfoRow struct {
	CodeType    string  `parquet:"code_type"`
	CodeValue   string  `parquet:"code_value"`
	Description *string `parquet:"description,optional"`
}

// ToItemRow converts a HospitalChargeRow to an ItemRow.
func (r *HospitalChargeRow) ToItemRow() ItemRow {
	return ItemRow{
		Description:            r.Description,
		Setting:                r.Setting,
		CPTCode:                r.CPTCode,
		HCPCSCode:              r.HCPCSCode,
		MSDRGCode:              r.MSDRGCode,
		NDCCode:                r.NDCCode,
		RCCode:                 r.RCCode,
		ICDCode:                r.ICDCode,
		DRGCode:                r.DRGCode,
		CDMCode:                r.CDMCode,
		LOCALCode:              r.LOCALCode,
		APCCode:                r.APCCode,
		EAPGCode:               r.EAPGCode,
		HIPPSCode:              r.HIPPSCode,
		CDTCode:                r.CDTCode,
		RDRGCode:               r.RDRGCode,
		SDRGCode:               r.SDRGCode,
		APSDRGCode:             r.APSDRGCode,
		APDRGCode:              r.APDRGCode,
		APRDRGCode:             r.APRDRGCode,
		TRISDRGCode:            r.TRISDRGCode,
		CMGCode:                r.CMGCode,
		MSLTCDRGCode:           r.MSLTCDRGCode,
		GrossCharge:            r.GrossCharge,
		DiscountedCash:         r.DiscountedCash,
		MinCharge:              r.MinCharge,
		MaxCharge:              r.MaxCharge,
		Modifiers:              r.Modifiers,
		AdditionalGenericNotes: r.AdditionalGenericNotes,
		DrugUnitOfMeasurement:  r.DrugUnitOfMeasurement,
		DrugTypeOfMeasurement:  r.DrugTypeOfMeasurement,
		BillingClass:           r.BillingClass,
	}
}

// ToPayerRow converts a HospitalChargeRow to a PayerRow.
func (r *HospitalChargeRow) ToPayerRow() PayerRow {
	return PayerRow{
		Description:            r.Description,
		Setting:                r.Setting,
		CPTCode:                r.CPTCode,
		HCPCSCode:              r.HCPCSCode,
		MSDRGCode:              r.MSDRGCode,
		NDCCode:                r.NDCCode,
		RCCode:                 r.RCCode,
		ICDCode:                r.ICDCode,
		DRGCode:                r.DRGCode,
		CDMCode:                r.CDMCode,
		LOCALCode:              r.LOCALCode,
		APCCode:                r.APCCode,
		EAPGCode:               r.EAPGCode,
		HIPPSCode:              r.HIPPSCode,
		CDTCode:                r.CDTCode,
		RDRGCode:               r.RDRGCode,
		SDRGCode:               r.SDRGCode,
		APSDRGCode:             r.APSDRGCode,
		APDRGCode:              r.APDRGCode,
		APRDRGCode:             r.APRDRGCode,
		TRISDRGCode:            r.TRISDRGCode,
		CMGCode:                r.CMGCode,
		MSLTCDRGCode:           r.MSLTCDRGCode,
		PayerName:              r.PayerName,
		PlanName:               r.PlanName,
		GrossCharge:            r.GrossCharge,
		DiscountedCash:         r.DiscountedCash,
		NegotiatedDollar:       r.NegotiatedDollar,
		NegotiatedPercentage:   r.NegotiatedPercentage,
		NegotiatedAlgorithm:    r.NegotiatedAlgorithm,
		EstimatedAmount:        r.EstimatedAmount,
		MinCharge:              r.MinCharge,
		MaxCharge:              r.MaxCharge,
		Methodology:            r.Methodology,
		MedianAmount:           r.MedianAmount,
		Pct10Amount:            r.Pct10Amount,
		Pct90Amount:            r.Pct90Amount,
		AllowedCount:           r.AllowedCount,
		Modifiers:              r.Modifiers,
		AdditionalPayerNotes:   r.AdditionalPayerNotes,
		AdditionalGenericNotes: r.AdditionalGenericNotes,
		BillingClass:           r.BillingClass,
	}
}
