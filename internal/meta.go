package internal

import (
	"encoding/json"
	"fmt"
	"os"
)

// RunMeta contains hospital metadata extracted from MRF headers,
// exposed for logging and external consumption. It is also serialized
// to meta.json alongside parquet output files so that metadata can be
// recovered without re-reading the source MRF.
type RunMeta struct {
	HospitalName      string   `json:"hospital_name"`
	LocationNames     []string `json:"location_names"`
	HospitalAddresses []string `json:"hospital_addresses"`
	LicenseNumber     *string  `json:"license_number"`
	LicenseState      *string  `json:"license_state"`
	Type2NPIs         []string `json:"type_2_npis"`
	LastUpdatedOn     string   `json:"last_updated_on"`
	Version           string   `json:"schema_version"`
	PriceCount        int      `json:"price_count"`
	InputSource       string   `json:"input_source"` // original input path/URI, used to disambiguate output filenames
}

// WriteMetaJSON marshals meta to indented JSON and writes it to path.
func WriteMetaJSON(path string, meta RunMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	return nil
}

// ReadMetaJSON reads a meta.json file and returns the parsed RunMeta.
func ReadMetaJSON(path string) (RunMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RunMeta{}, fmt.Errorf("read meta.json: %w", err)
	}
	var meta RunMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return RunMeta{}, fmt.Errorf("unmarshal meta.json: %w", err)
	}
	return meta, nil
}

// ValidateMeta checks that a RunMeta has the minimum required fields to be
// considered a valid, complete conversion result. This is used during retry
// to distinguish a fully-written meta.json from a corrupted or truncated one.
func ValidateMeta(meta RunMeta) error {
	if meta.HospitalName == "" {
		return fmt.Errorf("missing hospital_name")
	}
	if meta.LastUpdatedOn == "" {
		return fmt.Errorf("missing last_updated_on")
	}
	if meta.Version == "" {
		return fmt.Errorf("missing schema_version")
	}
	if meta.InputSource == "" {
		return fmt.Errorf("missing input_source")
	}
	if meta.PriceCount <= 0 {
		return fmt.Errorf("invalid price_count: %d", meta.PriceCount)
	}
	return nil
}
