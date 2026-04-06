package internal

import (
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"
)

// SplitResult holds the output paths and counts from a split conversion.
type SplitResult struct {
	Meta          RunMeta
	ItemsPath     string
	PayerPath     string
	CodeInfoPath  string
	ItemCount     int
	PayerCount    int
	CodeInfoCount int
}

// ConvertSplit reads an MRF file and writes three separate parquet files:
//   - Items: one row per unique item (description+setting+codes+modifiers+rc_code)
//   - Payer: one row per item × payer/plan combination
//   - CodeInfo: deduplicated code type/value/description lookup table
//
// The basePath should be the output path without extension, e.g. "/tmp/nyu_langone".
// Output files: basePath-items.parquet, basePath-payer.parquet, basePath-codeinfo.parquet
func ConvertSplit(logger *slog.Logger, inputPath string, basePath string, batchSize int, codeTypes map[string]bool) (*SplitResult, error) {
	start := time.Now()

	isJSON := strings.EqualFold(filepath.Ext(inputPath), ".json")

	// Set up reader — always include payer charges for the split
	var reader chargeReader
	var csvReader *CSVReader
	var jsonReader *JSONReader
	var err error

	if isJSON {
		jsonReader, err = NewJSONReader(inputPath)
		if err != nil {
			return nil, fmt.Errorf("open JSON: %w", err)
		}
		jsonReader.SkipPayerCharges = false // always include payer data
		reader = jsonReader
	} else {
		csvReader, err = NewCSVReader(inputPath)
		if err != nil {
			return nil, fmt.Errorf("open CSV: %w", err)
		}
		csvReader.SkipPayerCharges = false // always include payer data
		reader = csvReader
	}
	defer reader.Close()

	var meta RunMeta
	if jsonReader != nil {
		meta = jsonReader.Meta()
	} else {
		meta = csvReader.Meta()
	}

	// Create the three writers
	itemsPath := basePath + "-items.parquet"
	payerPath := basePath + "-payer.parquet"
	codeInfoPath := basePath + "-codeinfo.parquet"

	itemWriter, err := NewItemWriter(itemsPath)
	if err != nil {
		return nil, fmt.Errorf("create items writer: %w", err)
	}

	payerWriter, err := NewPayerWriter(payerPath)
	if err != nil {
		return nil, fmt.Errorf("create payer writer: %w", err)
	}

	codeInfoWriter, err := NewCodeInfoWriter(codeInfoPath)
	if err != nil {
		return nil, fmt.Errorf("create code-info writer: %w", err)
	}

	logger.Info("split-converting",
		"input", inputPath,
		"items_output", itemsPath,
		"payer_output", payerPath,
		"codeinfo_output", codeInfoPath,
		"format", reader.Format(),
	)

	// Track seen items for deduplication (items file) and codes (code-info file).
	// Use FNV-1a hashes instead of full strings to reduce memory ~15-20x
	// (8 bytes per entry vs 100-200+ byte string keys).
	seenItems := make(map[uint64]struct{})
	seenCodes := make(map[uint64]struct{})

	var itemBatch []ItemRow
	var payerBatch []PayerRow
	var codeInfoBatch []CodeInfoRow
	var inputCount int64
	lastLog := time.Now()

	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isJSON {
				return nil, fmt.Errorf("read JSON item %d: %w", jsonReader.ItemNum()+1, err)
			}
			return nil, fmt.Errorf("read CSV row %d: %w", csvReader.RowNum(), err)
		}

		inputCount++

		// Filter by code types if specified
		if len(codeTypes) > 0 {
			filtered := rows[:0]
			for _, r := range rows {
				if r.HasAnyCodeType(codeTypes) {
					filtered = append(filtered, r)
				}
			}
			rows = filtered
		}

		for _, row := range rows {
			// Items file: deduplicate to one row per item
			itemHash := itemDedupHash(&row)
			if _, dup := seenItems[itemHash]; !dup {
				seenItems[itemHash] = struct{}{}
				itemBatch = append(itemBatch, row.ToItemRow())
			}

			// Payer file: only rows that have payer data
			if row.PayerName != nil && *row.PayerName != "" {
				payerBatch = append(payerBatch, row.ToPayerRow())
			}

			// Code-info file: collect unique codes
			collectCodesHashed(&row, seenCodes, &codeInfoBatch)
		}

		// Flush batches if large enough
		if len(itemBatch) >= batchSize {
			if _, err := itemWriter.Write(itemBatch); err != nil {
				return nil, fmt.Errorf("write items batch: %w", err)
			}
			itemBatch = itemBatch[:0]
		}
		if len(payerBatch) >= batchSize {
			if _, err := payerWriter.Write(payerBatch); err != nil {
				return nil, fmt.Errorf("write payer batch: %w", err)
			}
			payerBatch = payerBatch[:0]
		}

		if time.Since(lastLog) >= 5*time.Second {
			elapsed := time.Since(start).Seconds()
			logger.Debug("split-progress",
				"input_count", inputCount,
				"items", itemWriter.Count()+len(itemBatch),
				"payer_rows", payerWriter.Count()+len(payerBatch),
				"rows_per_sec", fmt.Sprintf("%.0f", float64(inputCount)/elapsed))
			lastLog = time.Now()
		}
	}

	// Flush remaining batches
	if len(itemBatch) > 0 {
		if _, err := itemWriter.Write(itemBatch); err != nil {
			return nil, fmt.Errorf("write final items batch: %w", err)
		}
	}
	if len(payerBatch) > 0 {
		if _, err := payerWriter.Write(payerBatch); err != nil {
			return nil, fmt.Errorf("write final payer batch: %w", err)
		}
	}
	if len(codeInfoBatch) > 0 {
		if _, err := codeInfoWriter.Write(codeInfoBatch); err != nil {
			return nil, fmt.Errorf("write code-info batch: %w", err)
		}
	}

	// Close all writers
	if err := itemWriter.Close(); err != nil {
		return nil, fmt.Errorf("close items parquet: %w", err)
	}
	if err := payerWriter.Close(); err != nil {
		return nil, fmt.Errorf("close payer parquet: %w", err)
	}
	if err := codeInfoWriter.Close(); err != nil {
		return nil, fmt.Errorf("close code-info parquet: %w", err)
	}

	elapsed := time.Since(start)
	logger.Info("split-done",
		"duration", elapsed.Round(time.Millisecond).String(),
		"items", itemWriter.Count(),
		"payer_rows", payerWriter.Count(),
		"code_info_rows", codeInfoWriter.Count(),
	)

	meta.PriceCount = itemWriter.Count()
	return &SplitResult{
		Meta:          meta,
		ItemsPath:     itemsPath,
		PayerPath:     payerPath,
		CodeInfoPath:  codeInfoPath,
		ItemCount:     itemWriter.Count(),
		PayerCount:    payerWriter.Count(),
		CodeInfoCount: codeInfoWriter.Count(),
	}, nil
}

// itemDedupHash returns a 64-bit FNV-1a hash of the item dedup key.
// Using a hash instead of the full string key reduces memory ~15-20x
// for large files with millions of unique items.
func itemDedupHash(r *HospitalChargeRow) uint64 {
	h := fnv.New64a()
	h.Write([]byte(r.Description))
	h.Write([]byte{'|'})
	h.Write([]byte(r.Setting))
	h.Write([]byte{'|'})
	if r.Modifiers != nil {
		h.Write([]byte(*r.Modifiers))
	}
	h.Write([]byte{'|'})
	if r.RCCode != nil {
		h.Write([]byte(*r.RCCode))
	}
	h.Write([]byte{'|'})
	if r.CPTCode != nil {
		h.Write([]byte(*r.CPTCode))
	}
	h.Write([]byte{'|'})
	if r.HCPCSCode != nil {
		h.Write([]byte(*r.HCPCSCode))
	}
	h.Write([]byte{'|'})
	if r.MSDRGCode != nil {
		h.Write([]byte(*r.MSDRGCode))
	}
	h.Write([]byte{'|'})
	if r.NDCCode != nil {
		h.Write([]byte(*r.NDCCode))
	}
	return h.Sum64()
}

// collectCodesHashed extracts code type/value pairs from a row and adds unique
// ones to the batch, using FNV-1a hashes for the seen set.
func collectCodesHashed(r *HospitalChargeRow, seen map[uint64]struct{}, batch *[]CodeInfoRow) {
	type cv struct {
		codeType, value string
	}
	var codes []cv

	if r.CPTCode != nil && *r.CPTCode != "" {
		codes = append(codes, cv{"CPT", *r.CPTCode})
	}
	if r.HCPCSCode != nil && *r.HCPCSCode != "" {
		codes = append(codes, cv{"HCPCS", *r.HCPCSCode})
	}
	if r.MSDRGCode != nil && *r.MSDRGCode != "" {
		codes = append(codes, cv{"MS-DRG", *r.MSDRGCode})
	}
	if r.NDCCode != nil && *r.NDCCode != "" {
		codes = append(codes, cv{"NDC", *r.NDCCode})
	}
	if r.RCCode != nil && *r.RCCode != "" {
		codes = append(codes, cv{"RC", *r.RCCode})
	}
	if r.ICDCode != nil && *r.ICDCode != "" {
		codes = append(codes, cv{"ICD", *r.ICDCode})
	}
	if r.DRGCode != nil && *r.DRGCode != "" {
		codes = append(codes, cv{"DRG", *r.DRGCode})
	}
	if r.CDMCode != nil && *r.CDMCode != "" {
		codes = append(codes, cv{"CDM", *r.CDMCode})
	}
	if r.LOCALCode != nil && *r.LOCALCode != "" {
		codes = append(codes, cv{"LOCAL", *r.LOCALCode})
	}
	if r.APCCode != nil && *r.APCCode != "" {
		codes = append(codes, cv{"APC", *r.APCCode})
	}
	if r.EAPGCode != nil && *r.EAPGCode != "" {
		codes = append(codes, cv{"EAPG", *r.EAPGCode})
	}
	if r.HIPPSCode != nil && *r.HIPPSCode != "" {
		codes = append(codes, cv{"HIPPS", *r.HIPPSCode})
	}
	if r.CDTCode != nil && *r.CDTCode != "" {
		codes = append(codes, cv{"CDT", *r.CDTCode})
	}
	if r.RDRGCode != nil && *r.RDRGCode != "" {
		codes = append(codes, cv{"R-DRG", *r.RDRGCode})
	}
	if r.SDRGCode != nil && *r.SDRGCode != "" {
		codes = append(codes, cv{"S-DRG", *r.SDRGCode})
	}
	if r.APSDRGCode != nil && *r.APSDRGCode != "" {
		codes = append(codes, cv{"APS-DRG", *r.APSDRGCode})
	}
	if r.APDRGCode != nil && *r.APDRGCode != "" {
		codes = append(codes, cv{"AP-DRG", *r.APDRGCode})
	}
	if r.APRDRGCode != nil && *r.APRDRGCode != "" {
		codes = append(codes, cv{"APR-DRG", *r.APRDRGCode})
	}
	if r.TRISDRGCode != nil && *r.TRISDRGCode != "" {
		codes = append(codes, cv{"TRIS-DRG", *r.TRISDRGCode})
	}
	if r.CMGCode != nil && *r.CMGCode != "" {
		codes = append(codes, cv{"CMG", *r.CMGCode})
	}
	if r.MSLTCDRGCode != nil && *r.MSLTCDRGCode != "" {
		codes = append(codes, cv{"MS-LTC-DRG", *r.MSLTCDRGCode})
	}
	if r.Modifiers != nil && *r.Modifiers != "" {
		// Split by both pipe and space (JSON uses pipe, CSV may use spaces)
		for _, mod := range strings.FieldsFunc(*r.Modifiers, func(c rune) bool {
			return c == '|' || c == ' '
		}) {
			mod = strings.TrimSpace(mod)
			if mod != "" {
				codes = append(codes, cv{"MODIFIER", mod})
			}
		}
	}

	for _, c := range codes {
		h := fnv.New64a()
		h.Write([]byte(c.codeType))
		h.Write([]byte{'|'})
		h.Write([]byte(c.value))
		key := h.Sum64()
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			*batch = append(*batch, CodeInfoRow{
				CodeType:  c.codeType,
				CodeValue: c.value,
			})
		}
	}
}
