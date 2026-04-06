package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// JSONReader streams a CMS V2/V3 hospital price transparency JSON file
// and emits HospitalChargeRow records one item at a time. Only one jsonItem
// is in memory at a time — decoded, expanded, then discarded.
type JSONReader struct {
	file             *os.File
	decoder          *json.Decoder
	meta             hospitalMeta
	format           string // "json-v2" or "json-v3"
	itemNum          int64
	done             bool
	SkipPayerCharges bool
}

func NewJSONReader(filepath string) (*JSONReader, error) {
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

	decoder := json.NewDecoder(bufReader)

	r := &JSONReader{
		file:    file,
		decoder: decoder,
		format:  "json",
	}

	if err := r.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return r, nil
}

func (r *JSONReader) readHeader() error {
	// Read opening '{'
	tok, err := r.decoder.Token()
	if err != nil {
		return fmt.Errorf("read opening brace: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("expected '{', got %v", tok)
	}

	for r.decoder.More() {
		// Read field name
		tok, err := r.decoder.Token()
		if err != nil {
			return fmt.Errorf("read field name: %w", err)
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("expected string key, got %T", tok)
		}

		switch key {
		case "hospital_name":
			var v string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode hospital_name: %w", err)
			}
			r.meta.hospitalName = v

		case "last_updated_on":
			var v string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode last_updated_on: %w", err)
			}
			r.meta.lastUpdatedOn = v

		case "version":
			var v string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode version: %w", err)
			}
			r.meta.version = v
			if strings.HasPrefix(v, "2") {
				r.format = "json-v2"
			} else if strings.HasPrefix(v, "3") {
				r.format = "json-v3"
			}

		case "hospital_address":
			var v []string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode hospital_address: %w", err)
			}
			r.meta.hospitalAddress = strings.Join(v, "; ")
			r.meta.hospitalAddresses = v

		case "hospital_location":
			var v []string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode hospital_location: %w", err)
			}
			r.meta.hospitalLocation = strings.Join(v, "; ")
			r.meta.hospitalLocations = v

		case "location_name":
			var v []string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode location_name: %w", err)
			}
			r.meta.hospitalLocation = strings.Join(v, "; ")
			r.meta.hospitalLocations = v

		case "type_2_npi":
			var v []string
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode type_2_npi: %w", err)
			}
			r.meta.type2NPIs = v

		case "license_information":
			var v jsonLicense
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode license_information: %w", err)
			}
			r.meta.licenseNumber = v.LicenseNumber
			if v.State != "" {
				s := v.State
				r.meta.licenseState = &s
			}

		case "affirmation":
			var v jsonAttestation
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode affirmation: %w", err)
			}
			r.meta.affirmation = v.ConfirmAffirmation

		case "attestation":
			var v jsonAttestation
			if err := r.decoder.Decode(&v); err != nil {
				return fmt.Errorf("decode attestation: %w", err)
			}
			r.meta.affirmation = v.ConfirmAttestation

		case "financial_aid_policy":
			var raw json.RawMessage
			if err := r.decoder.Decode(&raw); err != nil {
				return fmt.Errorf("decode financial_aid_policy: %w", err)
			}
			var v string
			if len(raw) > 0 && raw[0] == '"' {
				// plain string
				if err := json.Unmarshal(raw, &v); err != nil {
					return fmt.Errorf("decode financial_aid_policy string: %w", err)
				}
			} else if len(raw) > 0 && raw[0] == '[' {
				// array of strings — join with " | "
				var arr []string
				if err := json.Unmarshal(raw, &arr); err != nil {
					return fmt.Errorf("decode financial_aid_policy array: %w", err)
				}
				v = strings.Join(arr, " | ")
			}
			if v != "" {
				r.meta.financialAidPolicy = &v
			}

		case "general_contract_provisions":
			var raw json.RawMessage
			if err := r.decoder.Decode(&raw); err != nil {
				return fmt.Errorf("decode general_contract_provisions: %w", err)
			}
			// V3 JSON: array of objects with payer_name/plan_name/provisions.
			// Flatten to a single string for the Parquet row.
			type provision struct {
				PayerName  string `json:"payer_name"`
				PlanName   string `json:"plan_name"`
				Provisions string `json:"provisions"`
			}
			var provs []provision
			if err := json.Unmarshal(raw, &provs); err == nil && len(provs) > 0 {
				var parts []string
				for _, p := range provs {
					parts = append(parts, p.Provisions)
				}
				s := strings.Join(parts, "; ")
				r.meta.generalContractProvisions = &s
			} else {
				// Fallback: store raw string if it's a simple string
				var str string
				if err := json.Unmarshal(raw, &str); err == nil && str != "" {
					r.meta.generalContractProvisions = &str
				}
			}

		case "standard_charge_information":
			// Read opening '[' — decoder is now positioned at first array element
			tok, err := r.decoder.Token()
			if err != nil {
				return fmt.Errorf("read standard_charge_information '[': %w", err)
			}
			if d, ok := tok.(json.Delim); !ok || d != '[' {
				return fmt.Errorf("expected '[' for standard_charge_information, got %v", tok)
			}
			return nil

		default:
			// Skip unknown fields (modifier_information, etc.)
			var skip json.RawMessage
			if err := r.decoder.Decode(&skip); err != nil {
				return fmt.Errorf("skip field %q: %w", key, err)
			}
		}
	}

	// If we get here, no standard_charge_information was found
	r.done = true
	return nil
}

// Next returns the HospitalChargeRow(s) for the next JSON item.
// Returns nil, io.EOF when done.
func (r *JSONReader) Next() ([]HospitalChargeRow, error) {
	if r.done {
		return nil, io.EOF
	}

	if !r.decoder.More() {
		// Read closing ']'
		r.decoder.Token()
		r.done = true
		return nil, io.EOF
	}

	var item jsonItem
	if err := r.decoder.Decode(&item); err != nil {
		return nil, fmt.Errorf("decode item %d: %w", r.itemNum+1, err)
	}

	rows := r.expandItem(&item)
	r.itemNum++
	return rows, nil
}

func (r *JSONReader) expandItem(item *jsonItem) []HospitalChargeRow {
	// Build base row with hospital metadata
	base := HospitalChargeRow{
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

		Description: strings.ToValidUTF8(item.Description, "\uFFFD"),
	}

	// V3 item-level billing_class
	if item.BillingClass != nil {
		bc := strings.ToValidUTF8(*item.BillingClass, "\uFFFD")
		base.BillingClass = &bc
	}

	// Set codes
	for _, ci := range item.CodeInformation {
		base.SetCode(strings.ToUpper(ci.Type), strings.ToValidUTF8(ci.Code, ""))
	}

	// Set drug information
	if item.DrugInformation != nil {
		base.DrugUnitOfMeasurement = item.DrugInformation.Unit.Value
		if item.DrugInformation.Type != "" {
			t := item.DrugInformation.Type
			base.DrugTypeOfMeasurement = &t
		}
	}

	var rows []HospitalChargeRow

	for i := range item.StandardCharges {
		sc := &item.StandardCharges[i]

		chargeRow := base // struct copy
		chargeRow.Setting = strings.ToValidUTF8(sc.Setting, "\uFFFD")
		chargeRow.MinCharge = sc.Minimum
		chargeRow.MaxCharge = sc.Maximum
		chargeRow.DiscountedCash = sc.DiscountedCash

		// Gross charge: V3 uses gross_charge (float64), V2 uses gross_charges (FlexibleFloat)
		if sc.GrossCharge != nil {
			chargeRow.GrossCharge = sc.GrossCharge
		} else if sc.GrossCharges != nil {
			chargeRow.GrossCharge = sc.GrossCharges.Value
		}

		// Modifiers
		if len(sc.ModifierCode) > 0 {
			m := strings.Join(sc.ModifierCode, "|")
			chargeRow.Modifiers = &m
		}

		// Additional generic notes
		if sc.AdditionalGenericNotes != nil {
			n := strings.ToValidUTF8(*sc.AdditionalGenericNotes, "\uFFFD")
			chargeRow.AdditionalGenericNotes = &n
		}

		// Charge-level billing_class overrides item-level
		if sc.BillingClass != nil {
			bc := strings.ToValidUTF8(*sc.BillingClass, "\uFFFD")
			chargeRow.BillingClass = &bc
		}

		if r.SkipPayerCharges || len(sc.PayersInformation) == 0 {
			// No payer data (or skipping) — emit one row with gross/discounted only
			rows = append(rows, chargeRow)
			continue
		}

		for j := range sc.PayersInformation {
			p := &sc.PayersInformation[j]

			prow := chargeRow // struct copy
			pn := strings.ToValidUTF8(p.PayerName, "\uFFFD")
			pl := strings.ToValidUTF8(p.PlanName, "\uFFFD")
			prow.PayerName = &pn
			prow.PlanName = &pl
			prow.NegotiatedDollar = p.StandardChargeDollar
			prow.NegotiatedPercentage = p.StandardChargePercentage
			prow.NegotiatedAlgorithm = p.StandardChargeAlgorithm
			prow.EstimatedAmount = p.EstimatedAmount

			if p.Methodology != "" {
				m := strings.ToValidUTF8(p.Methodology, "\uFFFD")
				prow.Methodology = &m
			}
			if p.AdditionalPayerNotes != nil {
				n := strings.ToValidUTF8(*p.AdditionalPayerNotes, "\uFFFD")
				prow.AdditionalPayerNotes = &n
			}

			// V3 allowed-amount statistics
			prow.MedianAmount = p.MedianAmount
			prow.Pct10Amount = p.Pct10
			prow.Pct90Amount = p.Pct90
			prow.AllowedCount = p.Count

			rows = append(rows, prow)
		}
	}

	return rows
}

// ItemNum returns the number of items read so far.
func (r *JSONReader) ItemNum() int64 {
	return r.itemNum
}

// Format returns "json-v2", "json-v3", or "json".
func (r *JSONReader) Format() string {
	return r.format
}

// Meta returns the hospital metadata parsed from the JSON header fields.
func (r *JSONReader) Meta() RunMeta {
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

// Close closes the underlying file.
func (r *JSONReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
