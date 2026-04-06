# mrf

A command-line tool that converts hospital price transparency files (CSV or JSON) into optimized Apache Parquet files.

## Rationale

The CMS Hospital Price Transparency rule requires hospitals to publish machine-readable files of their standard charges. In practice these files are massive (hundreds of MB to multiple GB), deeply nested, and denormalized — making them impractical for analytical queries. MRF solves this by:

- Normalizing both CSV and JSON formats into a unified data model
- Converting to Parquet with query-specific optimizations (dedicated columns per billing code type, bloom filters, Zstd compression)
- Optionally splitting output into separate items, payer charges, and code metadata tables
- Geocoding hospital addresses via the US Census API
- Logging conversion results with rich metadata in JSONL format

## Usage

```
mrf --file <input> [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | *(required)* | Input file path or URL (CSV or JSON) |
| `--out` | *(derived)* | Output Parquet file path |
| `--batch` | `10000` | Batch size for Parquet row group writes |
| `--split` | `true` | Produce 3 separate Parquet files (items, payer, code-info) |
| `--skip-payer-charges` | `false` | Skip payer-specific negotiated rates |
| `--code-types` | all | Comma-separated billing code types to include (CPT, HCPCS, MS-DRG, NDC, etc.) |
| `--log` | `mrf-log.jsonl` | JSONL log file path |
| `--hospitalName` | | CMS HPT location name for log entry |

### Examples

```bash
# Convert a local CSV
mrf --file charges.csv

# Convert a remote JSON file with custom output path
mrf --file https://hospital.example.com/charges.json --out hospital.parquet

# Only include CPT and HCPCS codes
mrf --file charges.csv --code-types CPT,HCPCS
```

### Output

**Split mode** (default) produces three files:

- `*-items.parquet` — one row per unique service/item with high-level pricing
- `*-payer.parquet` — one row per item × payer/plan combination with negotiated rates
- `*-codeinfo.parquet` — deduplicated lookup table of billing code descriptions

**Single mode** (`--split=false`) produces one denormalized Parquet file with all charge data.

Both modes also write a `meta.json` (hospital metadata) and append to the JSONL log.

### Parquet optimizations

- Dedicated columns for each billing code type (CPT, HCPCS, MS-DRG, NDC, etc.) enabling predicate pushdown
- Zstd level-3 compression
- Bloom filters on all code columns plus payer/plan name
- 50K-row row groups with page-level statistics
- Columns ordered by query frequency for page-cache locality
