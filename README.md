# hospital-mrf

A command-line tool that converts hospital price transparency files (CSV or JSON) into optimized Apache Parquet files.

## Rationale

The CMS Hospital Price Transparency rule requires hospitals to publish machine-readable files of their standard charges. In practice these files are massive (hundreds of MB to multiple GB), deeply nested, and denormalized — making them impractical for analytical queries. MRF solves this by:

- Normalizing both CSV and JSON formats into a unified data model
- Converting to Parquet with query-specific optimizations (dedicated columns per billing code type, bloom filters, Zstd compression)
- Optionally splitting output into separate items, payer charges, and code metadata tables
- Geocoding hospital addresses via the US Census API
- Logging conversion results with rich metadata in JSONL format

## Supported versions

MRF supports **V2** and **V3** of the CMS Hospital Price Transparency schema. V1 is not supported.

| Format | Layouts |
|--------|---------|
| JSON | V2, V3 |
| CSV | V2, V3 (both Tall and Wide) |

- **CSV Tall** — explicit `payer_name` / `plan_name` columns, one row per payer/plan/charge combination
- **CSV Wide** — payer/plan groups encoded in pipe-delimited column headers (e.g. `standard_charge|Aetna|PPO|negotiated_dollar`)

V3 adds several fields over V2: allowed-amount statistics (median, 10th/90th percentile, count), `billing_class` at both item and charge levels, and two new code types (CMG, MS-LTC-DRG). Version is auto-detected from the file.

For the full schema specification and implementation guide, see the [CMS Hospital Price Transparency repository](https://github.com/CMSgov/hospital-price-transparency).

## Usage

```
hospital-mrf --file <input> [flags]
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
hospital-mrf --file charges.csv

# Convert a remote JSON file with custom output path
hospital-mrf --file https://hospital.example.com/charges.json --out hospital.parquet

# Only include CPT and HCPCS codes
hospital-mrf --file charges.csv --code-types CPT,HCPCS
```

### Output

**Split mode** (default) produces three files:

- `*-items.parquet` — one row per unique service/item with high-level pricing
- `*-payer.parquet` — one row per item × payer/plan combination with negotiated rates
- `*-codeinfo.parquet` — deduplicated lookup table of billing code descriptions

**Single mode** (`--split=false`) produces one denormalized Parquet file with all charge data.

Both modes also write a `meta.json` (hospital metadata) and append to the JSONL log.

### Parquet optimizations

**Dedicated columns per billing code type.** Each CMS code type (CPT, HCPCS, MS-DRG, NDC, RC, ICD, CDM, etc.) gets its own column instead of a generic `code_type`/`code_value` pair. A query like `WHERE cpt_code = '99213'` scans a single column and benefits from row-group min/max skip. The alternative — a discriminator column — requires scanning all rows and defeats min/max statistics. Hospitals typically populate 2–4 code types per item, so the remaining columns are mostly null and cost only ~1 bit/row via null-bitmap RLE.

**Zstd level-3 compression.** Achieves ~20–30% smaller files compared to Snappy with acceptable write overhead — a good balance for files that are written once and queried many times.

**Bloom filters on code and payer columns.** All code columns plus `payer_name` and `plan_name` carry bloom filters (10 bits/value, ~1% false-positive rate). These columns are high-cardinality but frequently filtered; bloom filters let engines definitively rule out a row group with a small read, even when min/max ranges overlap.

**50K-row row groups.** For a typical 210K-row hospital file this yields ~4 row groups. Smaller row groups give finer-grained predicate pushdown — engines skip entire groups whose statistics don't match the query predicate, which is especially valuable over the network.

**8 KB pages with page-level statistics.** Modern engines (DuckDB 0.9+, Spark 3.3+) can filter at the page level within a row group, tightening the filtering boundary beyond row-group-level statistics alone.

**Column ordering by query frequency.** Primary query columns (description, setting) are placed first, followed by billing codes, payer identification, charge amounts, and hospital metadata last. This improves page-cache locality when engines read column chunks sequentially.

**Row sorting by CPT code.** Rows are sorted before writing so that per-row-group min/max statistics are tight for the most common query pattern (filtering by procedure code).

**Implicit compression wins.** Hospital metadata repeats per row and dictionary+RLE compresses to near-zero. String enums (`setting`, `methodology`, `billing_class`) dictionary-encode automatically, letting engines resolve equality predicates as integer comparisons.
