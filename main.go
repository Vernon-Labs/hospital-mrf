package main

import (
	"log/slog"
	"mrf/internal"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// parseCodeTypes splits a comma-separated string of code types into a set.
// Returns nil if the input is empty (meaning "all code types").
func parseCodeTypes(s string) map[string]bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	m := make(map[string]bool)
	for _, t := range strings.Split(s, ",") {
		t = strings.ToUpper(strings.TrimSpace(t))
		if t != "" {
			m[t] = true
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

var rootCmd = &cobra.Command{
	Use:   "mrf",
	Short: "Convert a hospital MRF file (CSV/JSON) to Parquet",
	Long: `Convert a single hospital price transparency file (CSV or JSON) to Parquet.

Examples:
  mrf --file input.csv
  mrf --file input.json --out output.parquet
  mrf --file https://example.com/charges.csv`,
	Run: func(cmd *cobra.Command, args []string) {
		file, _ := cmd.Flags().GetString("file")
		out, _ := cmd.Flags().GetString("out")
		batch, _ := cmd.Flags().GetInt("batch")
		skipPayer, _ := cmd.Flags().GetBool("skip-payer-charges")
		logPath, _ := cmd.Flags().GetString("log")
		hospitalName, _ := cmd.Flags().GetString("hospitalName")
		codeTypesStr, _ := cmd.Flags().GetString("code-types")
		split, _ := cmd.Flags().GetBool("split")

		if file == "" {
			slog.Error("--file is required")
			cmd.Usage()
			os.Exit(1)
		}

		codeTypes := parseCodeTypes(codeTypesStr)

		if split {
			// Split mode: produce 3 separate parquet files (items, payer, code-info)
			basePath := out
			if basePath == "" {
				basePath = strings.TrimSuffix(file, ".csv")
				basePath = strings.TrimSuffix(basePath, ".json")
				basePath = strings.TrimSuffix(basePath, ".JSON")
				basePath = strings.TrimSuffix(basePath, ".CSV")
			}
			basePath = strings.TrimSuffix(basePath, ".parquet")

			result, err := internal.ConvertSplit(slog.Default(), file, basePath, batch, codeTypes)
			if err != nil {
				slog.Error("split conversion failed", "error", err)
				os.Exit(1)
			}
			slog.Info("split complete",
				"items", result.ItemCount,
				"payer", result.PayerCount,
				"code_info", result.CodeInfoCount,
				"items_file", result.ItemsPath,
				"payer_file", result.PayerPath,
				"codeinfo_file", result.CodeInfoPath,
			)
			return
		}

		if err := internal.ProcessEntry(slog.Default(), file, out, logPath, batch, skipPayer, hospitalName, codeTypes); err != nil {
			slog.Error("conversion failed", "error", err)
			os.Exit(1)
		}

		if err := internal.GeocodeLogFile(logPath); err != nil {
			slog.Warn("geocoding failed", "error", err)
		}
	},
}

func init() {
	rootCmd.Flags().String("file", "", "Input file path or URL (required)")
	rootCmd.Flags().String("out", "", "Output Parquet file (default: derived from input)")
	rootCmd.Flags().Int("batch", 10000, "Batch size for Parquet writes")
	rootCmd.Flags().Bool("skip-payer-charges", false, "Skip payer-specific negotiated rates")
	rootCmd.Flags().String("log", "mrf-log.jsonl", "JSONL log file path")
	rootCmd.Flags().String("hospitalName", "", "CMS HPT location name for log entry")
	rootCmd.Flags().String("code-types", "CPT,HCPCS,CDM,LOCAL,DRG,MS-DRG,R-DRG,S-DRG,APS-DRG,AP-DRG,APR-DRG,TRIS-DRG,MS-LTC-DRG,NDC", "Comma-separated billing code types to include (empty string = all)")
	rootCmd.Flags().Bool("split", true, "Produce 3 split parquet files: items, payer, code-info")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
