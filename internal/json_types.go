package internal

import (
	"encoding/json"
	"strconv"
	"strings"
)

// FlexibleFloat handles JSON values that may be a number or a string
// (e.g. "24,945.00"). Needed for V2 gross_charges and drug_information.unit.
type FlexibleFloat struct {
	Value *float64
}

func (f *FlexibleFloat) UnmarshalJSON(data []byte) error {
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		f.Value = &num
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		cleaned := strings.TrimSpace(strings.ReplaceAll(str, ",", ""))
		if cleaned == "" {
			f.Value = nil
			return nil
		}
		num, err := strconv.ParseFloat(cleaned, 64)
		if err != nil {
			return err
		}
		f.Value = &num
		return nil
	}
	f.Value = nil
	return nil
}

type jsonLicense struct {
	LicenseNumber *string `json:"license_number,omitempty"`
	State         string  `json:"state"`
}

type jsonAttestation struct {
	ConfirmAttestation bool   `json:"confirm_attestation"`
	ConfirmAffirmation bool   `json:"confirm_affirmation"`
	AttesterName       string `json:"attester_name,omitempty"`
}

type jsonCode struct {
	Code string `json:"code"`
	Type string `json:"type"`
}

type jsonDrug struct {
	Unit FlexibleFloat `json:"unit"`
	Type string        `json:"type"`
}

type jsonPayer struct {
	PayerName                string   `json:"payer_name"`
	PlanName                 string   `json:"plan_name"`
	Methodology              string   `json:"methodology"`
	StandardChargeDollar     *float64 `json:"standard_charge_dollar,omitempty"`
	StandardChargePercentage *float64 `json:"standard_charge_percentage,omitempty"`
	StandardChargeAlgorithm  *string  `json:"standard_charge_algorithm,omitempty"`
	EstimatedAmount          *float64 `json:"estimated_amount,omitempty"`
	AdditionalPayerNotes     *string  `json:"additional_payer_notes,omitempty"`

	// V3 allowed-amount statistics
	MedianAmount *float64 `json:"median_amount,omitempty"`
	Pct10        *float64 `json:"10th_percentile,omitempty"`
	Pct90        *float64 `json:"90th_percentile,omitempty"`
	Count        *string  `json:"count,omitempty"`
}

type jsonCharge struct {
	Setting                string         `json:"setting"`
	GrossCharge            *float64       `json:"gross_charge,omitempty"`
	GrossCharges           *FlexibleFloat `json:"gross_charges,omitempty"`
	DiscountedCash         *float64       `json:"discounted_cash,omitempty"`
	Minimum                *float64       `json:"minimum,omitempty"`
	Maximum                *float64       `json:"maximum,omitempty"`
	ModifierCode           []string       `json:"modifier_code,omitempty"`
	PayersInformation      []jsonPayer    `json:"payers_information,omitempty"`
	AdditionalGenericNotes *string        `json:"additional_generic_notes,omitempty"`
	BillingClass           *string        `json:"billing_class,omitempty"` // V3: professional|facility|both
}

type jsonItem struct {
	Description     string       `json:"description"`
	CodeInformation []jsonCode   `json:"code_information"`
	DrugInformation *jsonDrug    `json:"drug_information,omitempty"`
	StandardCharges []jsonCharge `json:"standard_charges"`
	BillingClass    *string      `json:"billing_class,omitempty"` // V3: item-level billing class
}
