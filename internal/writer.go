package internal

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const (
	// RowsPerGroup controls how many rows go into each Parquet row group.
	// Smaller row groups = more granular predicate pushdown over the network
	// (engines skip entire row groups whose min/max stats don't match).
	// 50K rows yields ~4 row groups for a typical 210K-row hospital file.
	RowsPerGroup = 50000

	// MaxBufferedRows is the threshold at which writers sort and flush
	// buffered rows to disk to bound memory usage. Most hospitals have
	// well under 30M payer rows and will never hit this — their rows are
	// buffered, globally sorted, and written as a single sorted segment
	// on Close(). Only extreme outliers (e.g. John Muir Health with 34M
	// rows) trigger mid-stream flushes, producing multiple sorted segments
	// with slight min/max overlap between segments.
	MaxBufferedRows = 30_000_000

	// bloomBitsPerValue controls bloom filter sizing. 10 bits/value ≈ 1%
	// false positive rate — a good trade-off between filter size and accuracy.
	bloomBitsPerValue = 10
)

// ChargeWriter writes HospitalChargeRow records to a Parquet file configured
// for fast analytical queries and small file size.
//
// Writer configuration rationale:
//
//   - Zstd(3): ~20-30% smaller than Snappy with acceptable write overhead.
//
//   - Rows sorted by cpt_code ascending: clusters rows by the most common
//     query predicate, producing tight per-row-group min/max statistics so
//     engines can skip row groups that can't match.
//
//   - 50K rows per row group: for a 210K-row file this yields ~4 row groups.
//     More row groups = finer-grained predicate pushdown over the network.
//
//   - Bloom filters on all 19 code columns plus payer_name/plan_name: lets
//     engines definitively rule out a row group with a small read, even when
//     min/max ranges overlap.
//
//   - 8KB page size with statistics: enables page-level filtering within row
//     groups (DuckDB 0.9+, Spark 3.3+).
type ChargeWriter struct {
	file    *os.File
	writer  *parquet.GenericWriter[HospitalChargeRow]
	rows    []HospitalChargeRow
	flushed int
}

// NewChargeWriter creates a Parquet writer optimized for analytical queries.
func NewChargeWriter(filename string) (*ChargeWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[HospitalChargeRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("hospital-mrf", "1.0", ""),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "cpt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hcpcs_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ndc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "rc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "icd_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdm_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "local_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "eapg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hipps_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "r_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "s_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "aps_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ap_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apr_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "tris_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cmg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_ltc_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "payer_name"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "plan_name"),
		),
	)

	return &ChargeWriter{
		file:   file,
		writer: writer,
	}, nil
}

// Write buffers rows. If the buffer exceeds MaxBufferedRows, it is sorted
// and flushed to disk as a sorted segment to bound memory usage.
func (w *ChargeWriter) Write(rows []HospitalChargeRow) (int, error) {
	w.rows = append(w.rows, rows...)
	if len(w.rows) >= MaxBufferedRows {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// flush sorts all buffered rows by cpt_code, writes them in RowsPerGroup-sized
// row groups, and resets the buffer.
func (w *ChargeWriter) flush() error {
	slices.SortFunc(w.rows, func(a, b HospitalChargeRow) int {
		return cmpOptStr(a.CPTCode, b.CPTCode)
	})

	for i := 0; i < len(w.rows); i += RowsPerGroup {
		end := i + RowsPerGroup
		if end > len(w.rows) {
			end = len(w.rows)
		}
		if _, err := w.writer.Write(w.rows[i:end]); err != nil {
			return fmt.Errorf("write parquet rows: %w", err)
		}
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush row group: %w", err)
		}
	}

	w.flushed += len(w.rows)
	w.rows = w.rows[:0]
	return nil
}

// Close flushes remaining buffered rows and closes the Parquet file.
func (w *ChargeWriter) Close() error {
	if len(w.rows) > 0 {
		if err := w.flush(); err != nil {
			w.file.Close()
			return err
		}
	}
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return w.file.Close()
}

// Count returns the total number of rows (flushed + buffered).
func (w *ChargeWriter) Count() int {
	return w.flushed + len(w.rows)
}

// cmpOptStr compares two optional strings, with nil (null) sorting first.
func cmpOptStr(a, b *string) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return strings.Compare(*a, *b)
}

// ── Split writers for items / payer / code-info files ─────────────────

// ItemWriter writes ItemRow records to a Parquet file.
type ItemWriter struct {
	file    *os.File
	writer  *parquet.GenericWriter[ItemRow]
	rows    []ItemRow
	flushed int
}

func NewItemWriter(filename string) (*ItemWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create items parquet: %w", err)
	}
	writer := parquet.NewGenericWriter[ItemRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("hospital-mrf", "1.0", ""),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "cpt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hcpcs_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ndc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "rc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "icd_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdm_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "local_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "eapg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hipps_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cdt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "r_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "s_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "aps_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ap_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "apr_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "tris_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "cmg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_ltc_drg_code"),
		),
	)
	return &ItemWriter{file: file, writer: writer}, nil
}

func (w *ItemWriter) Write(rows []ItemRow) (int, error) {
	w.rows = append(w.rows, rows...)
	if len(w.rows) >= MaxBufferedRows {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

func (w *ItemWriter) flush() error {
	slices.SortFunc(w.rows, func(a, b ItemRow) int {
		return cmpOptStr(a.CPTCode, b.CPTCode)
	})
	for i := 0; i < len(w.rows); i += RowsPerGroup {
		end := i + RowsPerGroup
		if end > len(w.rows) {
			end = len(w.rows)
		}
		if _, err := w.writer.Write(w.rows[i:end]); err != nil {
			return fmt.Errorf("write items rows: %w", err)
		}
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush items row group: %w", err)
		}
	}
	w.flushed += len(w.rows)
	w.rows = w.rows[:0]
	return nil
}

func (w *ItemWriter) Close() error {
	if len(w.rows) > 0 {
		if err := w.flush(); err != nil {
			w.file.Close()
			return err
		}
	}
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close items writer: %w", err)
	}
	return w.file.Close()
}

func (w *ItemWriter) Count() int { return w.flushed + len(w.rows) }

// PayerWriter writes PayerRow records to a Parquet file.
type PayerWriter struct {
	file    *os.File
	writer  *parquet.GenericWriter[PayerRow]
	rows    []PayerRow
	flushed int
}

func NewPayerWriter(filename string) (*PayerWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create payer parquet: %w", err)
	}
	writer := parquet.NewGenericWriter[PayerRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("hospital-mrf", "1.0", ""),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "cpt_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "hcpcs_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ms_drg_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "ndc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "rc_code"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "payer_name"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "plan_name"),
		),
	)
	return &PayerWriter{file: file, writer: writer}, nil
}

func (w *PayerWriter) Write(rows []PayerRow) (int, error) {
	w.rows = append(w.rows, rows...)
	if len(w.rows) >= MaxBufferedRows {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

func (w *PayerWriter) flush() error {
	slices.SortFunc(w.rows, func(a, b PayerRow) int {
		if c := cmpOptStr(a.CPTCode, b.CPTCode); c != 0 {
			return c
		}
		return cmpOptStr(a.PayerName, b.PayerName)
	})
	for i := 0; i < len(w.rows); i += RowsPerGroup {
		end := i + RowsPerGroup
		if end > len(w.rows) {
			end = len(w.rows)
		}
		if _, err := w.writer.Write(w.rows[i:end]); err != nil {
			return fmt.Errorf("write payer rows: %w", err)
		}
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush payer row group: %w", err)
		}
	}
	w.flushed += len(w.rows)
	w.rows = w.rows[:0]
	return nil
}

func (w *PayerWriter) Close() error {
	if len(w.rows) > 0 {
		if err := w.flush(); err != nil {
			w.file.Close()
			return err
		}
	}
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close payer writer: %w", err)
	}
	return w.file.Close()
}

func (w *PayerWriter) Count() int { return w.flushed + len(w.rows) }

// CodeInfoWriter writes CodeInfoRow records to a Parquet file.
type CodeInfoWriter struct {
	file    *os.File
	writer  *parquet.GenericWriter[CodeInfoRow]
	rows    []CodeInfoRow
	flushed int
}

func NewCodeInfoWriter(filename string) (*CodeInfoWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("create code-info parquet: %w", err)
	}
	writer := parquet.NewGenericWriter[CodeInfoRow](file,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
		parquet.PageBufferSize(8*1024),
		parquet.DataPageStatistics(true),
		parquet.CreatedBy("hospital-mrf", "1.0", ""),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "code_type"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "code_value"),
		),
	)
	return &CodeInfoWriter{file: file, writer: writer}, nil
}

func (w *CodeInfoWriter) Write(rows []CodeInfoRow) (int, error) {
	w.rows = append(w.rows, rows...)
	if len(w.rows) >= MaxBufferedRows {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

func (w *CodeInfoWriter) flush() error {
	slices.SortFunc(w.rows, func(a, b CodeInfoRow) int {
		if c := strings.Compare(a.CodeType, b.CodeType); c != 0 {
			return c
		}
		return strings.Compare(a.CodeValue, b.CodeValue)
	})
	for i := 0; i < len(w.rows); i += RowsPerGroup {
		end := i + RowsPerGroup
		if end > len(w.rows) {
			end = len(w.rows)
		}
		if _, err := w.writer.Write(w.rows[i:end]); err != nil {
			return fmt.Errorf("write code-info rows: %w", err)
		}
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush code-info row group: %w", err)
		}
	}
	w.flushed += len(w.rows)
	w.rows = w.rows[:0]
	return nil
}

func (w *CodeInfoWriter) Close() error {
	if len(w.rows) > 0 {
		if err := w.flush(); err != nil {
			w.file.Close()
			return err
		}
	}
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close code-info writer: %w", err)
	}
	return w.file.Close()
}

func (w *CodeInfoWriter) Count() int { return w.flushed + len(w.rows) }
