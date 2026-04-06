package internal

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
)

// chargeReader is the common interface for CSV and JSON readers.
type chargeReader interface {
	Next() ([]HospitalChargeRow, error)
	Format() string
	Close() error
}

type geocodeResult struct {
	Address   string  `json:"address"`
	Matched   bool    `json:"matched"`
	MatchType string  `json:"match_type,omitempty"`
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
	Source    string  `json:"source,omitempty"`
}

type logEntry struct {
	Success            bool            `json:"success"`
	InputFormat        string          `json:"input_format"`
	URL                string          `json:"url"`
	StartTime          string          `json:"start_time"`
	DurationSeconds    float64         `json:"duration_seconds"`
	Error              string          `json:"error,omitempty"`
	OutputFile         string          `json:"output_file,omitempty"`
	HospitalName       string          `json:"hospital_name"`
	LocationNames      []string        `json:"location_names"`
	HospitalAddresses  []string        `json:"hospital_addresses"`
	LicenseNumber      *string         `json:"license_number"`
	LicenseState       *string         `json:"license_state"`
	Type2NPIs          []string        `json:"type_2_npis"`
	LastUpdatedOn      string          `json:"last_updated_on"`
	SchemaVersion      string          `json:"schema_version"`
	PriceCount         int             `json:"price_count"`
	Geocodes           []geocodeResult `json:"geocodes,omitempty"`
	CMSHPTLocationName string          `json:"cms_hpt_location_name,omitempty"`
	Skipped            bool            `json:"skipped,omitempty"`
}

// ProcessEntry handles a single file conversion: URL download, convert, log.
// Both single and batch subcommands call this.
//
// outputFile can be:
//   - A specific file path: "output.parquet"
//   - An S3 URI: "s3://bucket/key.parquet"
//   - A directory (local or S3, ending in "/"): "out/" or "s3://bucket/prefix/"
//   - Empty: output to current directory with metadata-derived name
//
// When outputFile is empty or a directory, the filename is derived from
// hospital metadata: {hospital_name}-{license_number}-{last_updated_on}.parquet
func ProcessEntry(logger *slog.Logger, inputFile, outputFile, logFile string, batchSize int, skipPayerCharges bool, hospitalName string, codeTypes map[string]bool) error {
	startTime := time.Now()
	inputDisplay := inputFile
	var meta RunMeta
	var processErr error

	// Always write a log entry when we're done, regardless of success/failure.
	defer func() {
		inputFormat := "csv"
		localExt := filepath.Ext(inputFile)
		if isURL(inputFile) {
			if u, err := url.Parse(inputFile); err == nil {
				localExt = path.Ext(u.Path)
			}
		}
		if strings.EqualFold(localExt, ".json") {
			inputFormat = "json"
		}

		entry := logEntry{
			Success:            processErr == nil,
			InputFormat:        inputFormat,
			URL:                inputDisplay,
			StartTime:          startTime.Format(time.RFC3339),
			DurationSeconds:    time.Since(startTime).Seconds(),
			HospitalName:       meta.HospitalName,
			LocationNames:      meta.LocationNames,
			HospitalAddresses:  meta.HospitalAddresses,
			LicenseNumber:      meta.LicenseNumber,
			LicenseState:       meta.LicenseState,
			Type2NPIs:          meta.Type2NPIs,
			LastUpdatedOn:      meta.LastUpdatedOn,
			SchemaVersion:      meta.Version,
			PriceCount:         meta.PriceCount,
			CMSHPTLocationName: hospitalName,
		}
		if processErr != nil {
			entry.Error = processErr.Error()
		}
		if processErr == nil && outputFile != "" {
			if strings.HasPrefix(outputFile, "s3://") {
				entry.OutputFile = outputFile
			} else if abs, err := filepath.Abs(outputFile); err == nil {
				entry.OutputFile = abs
			} else {
				entry.OutputFile = outputFile
			}
		}

		if err := appendLogEntry(logFile, &entry); err != nil {
			logger.Warn("failed to write log entry", "error", err)
		}
	}()

	// If input is a URL, download to a temp file first.
	localInput := inputFile
	if isURL(inputFile) {
		localPath, cleanup, err := downloadURL(logger, inputFile)
		if err != nil {
			processErr = fmt.Errorf("download %s: %w", inputFile, err)
			return processErr
		}
		defer cleanup()
		localInput = localPath
	}

	// Determine if output is a directory (filename will be derived from metadata).
	outputIsDir := outputFile == "" || strings.HasSuffix(outputFile, "/")
	isS3 := strings.HasPrefix(outputFile, "s3://")

	// Always write to a temp file when the final name isn't known yet,
	// or when uploading to S3.
	var tempFile string
	s3Dest := ""
	localOut := outputFile

	if isS3 || outputIsDir {
		f, err := os.CreateTemp("", "hospital-loader-*.parquet")
		if err != nil {
			processErr = fmt.Errorf("create temp file: %v", err)
			return processErr
		}
		tempFile = f.Name()
		f.Close()
		localOut = tempFile
		defer func() {
			if tempFile != "" {
				os.Remove(tempFile)
			}
		}()
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			if tempFile != "" {
				os.Remove(tempFile)
			}
			os.Exit(1)
		}()
		if isS3 {
			s3Dest = outputFile
		}
	}

	displayOut := outputFile
	meta, processErr = convert(logger, localInput, inputDisplay, localOut, displayOut, batchSize, skipPayerCharges, codeTypes)
	if processErr != nil {
		return processErr
	}

	// Resolve the final output filename from metadata.
	meta.InputSource = inputDisplay
	if outputIsDir {
		filename := buildOutputFilename(meta)
		if isS3 {
			outputFile = strings.TrimSuffix(outputFile, "/") + "/" + filename
			s3Dest = outputFile
		} else {
			dir := strings.TrimSuffix(outputFile, "/")
			if dir == "" {
				dir = "."
			}
			finalPath := filepath.Join(dir, filename)
			if err := os.Rename(localOut, finalPath); err != nil {
				processErr = fmt.Errorf("rename output: %w", err)
				return processErr
			}
			tempFile = "" // renamed successfully, don't clean up
			outputFile = finalPath
		}
		logger.Info("output file resolved", "path", outputFile)
	}

	if s3Dest != "" {
		if err := uploadToS3(logger, context.Background(), localOut, s3Dest); err != nil {
			processErr = err
			return processErr
		}
	}

	return nil
}

func convert(logger *slog.Logger, inputPath, inputDisplay, outputPath, displayPath string, batchSize int, skipPayerCharges bool, codeTypes map[string]bool) (RunMeta, error) {
	start := time.Now()
	var meta RunMeta

	isJSON := strings.EqualFold(filepath.Ext(inputPath), ".json")

	var reader chargeReader
	var csvReader *CSVReader
	var jsonReader *JSONReader
	var err error

	if isJSON {
		jsonReader, err = NewJSONReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open JSON: %w", err)
		}
		jsonReader.SkipPayerCharges = skipPayerCharges
		reader = jsonReader
		meta = jsonReader.Meta()
	} else {
		csvReader, err = NewCSVReader(inputPath)
		if err != nil {
			return meta, fmt.Errorf("open CSV: %w", err)
		}
		csvReader.SkipPayerCharges = skipPayerCharges
		reader = csvReader
		meta = csvReader.Meta()
	}
	defer reader.Close()

	writer, err := NewChargeWriter(outputPath)
	if err != nil {
		return meta, fmt.Errorf("create Parquet: %w", err)
	}

	fi, _ := os.Stat(inputPath)
	inputSize := int64(0)
	if fi != nil {
		inputSize = fi.Size()
	}

	// Log conversion start as a single line with all metadata.
	attrs := []any{
		"input", inputDisplay,
		"output", displayPath,
		"format", reader.Format(),
	}
	if csvReader != nil && csvReader.Format() == "wide" {
		attrs = append(attrs, "payers", csvReader.PayerPlanCount())
	}
	if inputSize > 0 {
		attrs = append(attrs, "size_mb", fmt.Sprintf("%.1f", float64(inputSize)/1024/1024))
	}
	logger.Info("converting", attrs...)

	inputLabel := "CSV rows"
	if isJSON {
		inputLabel = "JSON items"
	}

	batch := make([]HospitalChargeRow, 0, batchSize)
	var totalRows int
	var inputCount int64
	lastLog := time.Now()

	for {
		rows, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isJSON {
				return meta, fmt.Errorf("read JSON item %d: %w", jsonReader.ItemNum()+1, err)
			}
			return meta, fmt.Errorf("read CSV row %d: %w", csvReader.RowNum(), err)
		}

		inputCount++

		if len(codeTypes) > 0 {
			filtered := rows[:0]
			for _, r := range rows {
				if r.HasAnyCodeType(codeTypes) {
					filtered = append(filtered, r)
				}
			}
			rows = filtered
		}

		batch = append(batch, rows...)

		if len(batch) >= batchSize {
			if _, err := writer.Write(batch); err != nil {
				return meta, fmt.Errorf("write Parquet batch: %w", err)
			}
			totalRows += len(batch)
			batch = batch[:0]
		}

		if time.Since(lastLog) >= 5*time.Second {
			elapsed := time.Since(start).Seconds()
			cur := totalRows + len(batch)
			logger.Debug("progress",
				"input_type", inputLabel, "input_count", inputCount,
				"parquet_rows", cur,
				"rows_per_sec", fmt.Sprintf("%.0f", float64(cur)/elapsed))
			lastLog = time.Now()
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		if _, err := writer.Write(batch); err != nil {
			return meta, fmt.Errorf("write final Parquet batch: %w", err)
		}
		totalRows += len(batch)
	}

	if err := writer.Close(); err != nil {
		return meta, fmt.Errorf("close Parquet: %w", err)
	}

	elapsed := time.Since(start)
	outFi, _ := os.Stat(outputPath)
	outSize := int64(0)
	if outFi != nil {
		outSize = outFi.Size()
	}

	// Log completion as a single line with all stats.
	doneAttrs := []any{
		"duration", elapsed.Round(time.Millisecond).String(),
		"input_type", inputLabel,
		"input_count", inputCount,
		"parquet_rows", totalRows,
		"rows_per_sec", fmt.Sprintf("%.0f", float64(totalRows)/elapsed.Seconds()),
	}
	if inputSize > 0 && outSize > 0 {
		doneAttrs = append(doneAttrs,
			"input_size_mb", fmt.Sprintf("%.1f", float64(inputSize)/1024/1024),
			"output_size_mb", fmt.Sprintf("%.1f", float64(outSize)/1024/1024),
			"compression", fmt.Sprintf("%.1fx", float64(inputSize)/float64(outSize)),
		)
	}
	logger.Info("done", doneAttrs...)

	meta.PriceCount = totalRows
	return meta, nil
}

// buildOutputDirName builds a directory name from hospital metadata:
// {hospital_name}-{license_number}-{last_updated_on}-{hash}
// Missing parts are omitted. A short hash of InputSource is appended
// to guarantee uniqueness when multiple MRF files produce the same
// base name (e.g. same hospital, different source files).
func buildOutputDirName(meta RunMeta) string {
	var parts []string

	name := sanitizeFilename(meta.HospitalName)
	if name == "" {
		name = "unknown"
	}
	parts = append(parts, name)

	if meta.LicenseNumber != nil && *meta.LicenseNumber != "" {
		parts = append(parts, sanitizeFilename(*meta.LicenseNumber))
	}

	if meta.LastUpdatedOn != "" {
		parts = append(parts, sanitizeFilename(meta.LastUpdatedOn))
	}

	if meta.InputSource != "" {
		h := sha256.Sum256([]byte(meta.InputSource))
		parts = append(parts, hex.EncodeToString(h[:4])) // 8-char hex
	}

	return strings.Join(parts, "-")
}

// buildOutputFilename builds a parquet filename from hospital metadata:
// {hospital_name}-{license_number}-{last_updated_on}-{hash}.parquet
func buildOutputFilename(meta RunMeta) string {
	return buildOutputDirName(meta) + ".parquet"
}

// sanitizeFilename replaces characters that are unsafe in filenames with
// underscores and collapses whitespace.
func sanitizeFilename(name string) string {
	var b strings.Builder
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == ' ' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.ToLower(strings.TrimSpace(strings.ReplaceAll(b.String(), " ", "_")))
}

// logWriteMu serialises appendLogEntry calls so parallel workers don't
// interleave large writes and corrupt the JSONL log.
var logWriteMu sync.Mutex

func appendLogEntry(path string, entry *logEntry) error {
	logWriteMu.Lock()
	defer logWriteMu.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}
	data = append(data, '\n')

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}
	return nil
}

// parseS3URI splits "s3://bucket/key/path" into bucket and key.
func parseS3URI(uri string) (bucket, key string, err error) {
	uri = strings.TrimPrefix(uri, "s3://")
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid S3 URI: must be s3://bucket/key")
	}
	return parts[0], parts[1], nil
}

func uploadToS3(logger *slog.Logger, ctx context.Context, localPath, s3URI string) error {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return err
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local file for upload: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}

	logger.Debug("uploading",
		"size_mb", fmt.Sprintf("%.1f", float64(fi.Size())/1024/1024),
		"dest", s3URI)
	start := time.Now()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("S3 PutObject: %w", err)
	}

	logger.Debug("uploaded", "duration", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// DownloadFromS3 downloads an S3 object to a local temp file.
// Returns the temp file path and a cleanup function.
func DownloadFromS3(ctx context.Context, s3URI string) (string, func(), error) {
	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return "", nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", nil, fmt.Errorf("S3 GetObject: %w", err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "s3-download-*.jsonl")
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", nil, fmt.Errorf("download S3 object: %w", err)
	}
	tmp.Close()

	cleanup := func() { os.Remove(tmp.Name()) }
	return tmp.Name(), cleanup, nil
}

// UploadToS3 uploads a local file to S3.
func UploadToS3(ctx context.Context, localPath, s3URI string) error {
	return uploadToS3(slog.Default(), ctx, localPath, s3URI)
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// downloadURL downloads a URL to a temp file, preserving the original file
// extension so format detection works. Returns the temp file path and a
// cleanup function that removes the temp file.
func downloadURL(logger *slog.Logger, rawURL string) (localPath string, cleanup func(), err error) {
	// Upgrade http:// to https:// to avoid WAF/CDN challenges (e.g. Sucuri).
	if strings.HasPrefix(rawURL, "http://") {
		rawURL = "https://" + strings.TrimPrefix(rawURL, "http://")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, fmt.Errorf("parse URL: %w", err)
	}

	ext := path.Ext(u.Path)
	if ext == "" {
		ext = ".csv" // default assumption
	}

	f, err := os.CreateTemp("", "hospital-loader-*"+ext)
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := f.Name()

	cleanupFn := func() { os.Remove(tmpPath) }

	// Also clean up on signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Remove(tmpPath)
		os.Exit(1)
	}()

	logger.Info("downloading", "url", rawURL)
	start := time.Now()

	const maxAttempts = 3
	var result downloadResult
	var lastErr error

	for attempt := range maxAttempts {
		if attempt > 0 {
			// 404 = file not found, unlikely to recover; one quick retry then give up.
			if lastErr != nil && strings.Contains(lastErr.Error(), "404") {
				if attempt > 1 {
					break
				}
				logger.Info("retrying download", "attempt", attempt+1, "backoff", "5s", "error", lastErr)
				time.Sleep(5 * time.Second)
			} else if lastErr != nil && strings.Contains(lastErr.Error(), "download too slow") {
				// Slow download: one retry after 15s then give up.
				if attempt > 1 {
					break
				}
				logger.Info("retrying download", "attempt", attempt+1, "backoff", "15s", "error", lastErr)
				time.Sleep(15 * time.Second)
			} else {
				backoff := time.Duration(1<<(attempt-1)) * 15 * time.Second // 15s, 30s
				// WAF 403 blocks need longer cooldown.
				if lastErr != nil && strings.Contains(lastErr.Error(), "403") {
					backoff = time.Duration(attempt) * 30 * time.Second // 30s, 60s
				}
				logger.Info("retrying download", "attempt", attempt+1, "backoff", backoff.String(), "error", lastErr)
				time.Sleep(backoff)
			}
			// Truncate the file for a fresh write.
			if err := f.Truncate(0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("truncate temp file: %w", err)
			}
			if _, err := f.Seek(0, 0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("seek temp file: %w", err)
			}
		}

		throttleHost(rawURL)
		result, lastErr = doDownload(f, rawURL)
		if lastErr == nil {
			break
		}
		// If a WAF JS challenge is detected, try headless browser immediately.
		if errors.Is(lastErr, errJSChallenge) {
			logger.Info("WAF JS challenge detected, trying headless browser", "url", rawURL)
			if err := f.Truncate(0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("truncate temp file: %w", err)
			}
			if _, err := f.Seek(0, 0); err != nil {
				f.Close()
				cleanupFn()
				return "", nil, fmt.Errorf("seek temp file: %w", err)
			}
			result, lastErr = doDownloadHeadless(f, rawURL)
			if lastErr == nil {
				logger.Info("headless browser download succeeded", "bytes", result.N)
			} else {
				logger.Warn("headless browser download failed", "error", lastErr)
			}
			break
		}
	}

	if lastErr != nil {
		f.Close()
		cleanupFn()
		return "", nil, lastErr
	}

	if err := f.Close(); err != nil {
		cleanupFn()
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}

	n := result.N
	dlDuration := time.Since(start)
	dlSpeedMBs := float64(n) / 1024 / 1024 / dlDuration.Seconds()
	logger.Info("downloaded",
		"size_mb", fmt.Sprintf("%.1f", float64(n)/1024/1024),
		"duration", dlDuration.Round(time.Millisecond).String(),
		"speed_mb_s", fmt.Sprintf("%.1f", dlSpeedMBs))

	// If Content-Disposition provided a filename with a different extension,
	// rename the temp file so format detection works correctly.
	if result.Filename != "" {
		cdExt := strings.ToLower(filepath.Ext(result.Filename))
		if cdExt != "" && cdExt != strings.ToLower(filepath.Ext(tmpPath)) {
			newPath := strings.TrimSuffix(tmpPath, filepath.Ext(tmpPath)) + cdExt
			if err := os.Rename(tmpPath, newPath); err == nil {
				logger.Info("renamed from Content-Disposition", "filename", result.Filename)
				tmpPath = newPath
				cleanupFn = func() { os.Remove(newPath) }
			}
		}
	}

	// If the file starts with gzip magic bytes (0x1f 0x8b), decompress it.
	// Some servers serve .json files that are actually gzip-compressed.
	if isGzipFile(tmpPath) {
		decompressed, err := decompressGzipFile(tmpPath)
		if err != nil {
			cleanupFn()
			return "", nil, fmt.Errorf("decompress gzip: %w", err)
		}
		os.Remove(tmpPath)
		logger.Info("decompressed gzip",
			"size_mb", fmt.Sprintf("%.1f", float64(fileSize(decompressed))/1024/1024))
		tmpPath = decompressed
		cleanupFn = func() { os.Remove(decompressed) }
	}

	// If the extension is ambiguous (e.g. .aspx, .csv default), sniff the
	// file content to detect JSON vs CSV.
	curExt := strings.ToLower(filepath.Ext(tmpPath))
	if curExt != ".json" && curExt != ".zip" {
		if sniffed := sniffFileType(tmpPath); sniffed != "" && sniffed != curExt {
			newPath := strings.TrimSuffix(tmpPath, filepath.Ext(tmpPath)) + sniffed
			if err := os.Rename(tmpPath, newPath); err == nil {
				logger.Info("detected format from content", "format", sniffed)
				tmpPath = newPath
				cleanupFn = func() { os.Remove(newPath) }
			}
		}
	}

	// If the downloaded file is a zip, extract the first CSV/JSON from it.
	if strings.HasSuffix(strings.ToLower(tmpPath), ".zip") {
		extracted, err := extractZip(tmpPath)
		if err != nil {
			// File has .zip extension but isn't a valid zip — sniff actual format.
			if sniffed := sniffFileType(tmpPath); sniffed != "" && sniffed != ".zip" {
				newPath := strings.TrimSuffix(tmpPath, filepath.Ext(tmpPath)) + sniffed
				if renameErr := os.Rename(tmpPath, newPath); renameErr == nil {
					logger.Warn("file is not a valid zip, detected as "+sniffed, "error", err)
					cleanupFn = func() { os.Remove(newPath) }
					return newPath, cleanupFn, nil
				}
			}
			cleanupFn()
			return "", nil, fmt.Errorf("extract zip: %w", err)
		}
		// Clean up the zip file, return the extracted file instead.
		os.Remove(tmpPath)
		extractedDir := filepath.Dir(extracted)
		extractedCleanup := func() {
			os.Remove(extracted)
			// Remove temp dir if extractZipExternal created one.
			// Use filepath.Clean to normalize trailing slashes (macOS's
			// $TMPDIR includes a trailing slash, filepath.Dir does not).
			if filepath.Clean(extractedDir) != filepath.Clean(os.TempDir()) {
				os.RemoveAll(extractedDir)
			}
		}
		logger.Info("extracted",
			"file", filepath.Base(extracted),
			"size_mb", fmt.Sprintf("%.1f", float64(fileSize(extracted))/1024/1024))
		return extracted, extractedCleanup, nil
	}

	return tmpPath, cleanupFn, nil
}

// extractZip opens a zip file and extracts the first CSV or JSON file to a
// temp file. Returns the path to the extracted file.
func extractZip(zipPath string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	// Find first CSV or JSON file in the archive.
	var target *zip.File
	for _, f := range r.File {
		lower := strings.ToLower(f.Name)
		if strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".json") {
			target = f
			break
		}
	}
	if target == nil {
		// Fall back to first file.
		if len(r.File) == 0 {
			return "", fmt.Errorf("empty zip archive")
		}
		target = r.File[0]
	}

	ext := filepath.Ext(target.Name)
	if ext == "" {
		ext = ".csv"
	}

	rc, err := target.Open()
	if err != nil {
		// Fallback to system unzip for unsupported compression (e.g. Deflate64).
		r.Close()
		return extractZipExternal(zipPath, target.Name)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp("", "hospital-loader-*"+ext)
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("extract %s: %w", target.Name, err)
	}
	tmp.Close()

	return tmp.Name(), nil
}

// extractZipExternal uses the system unzip command to extract a file from a
// zip archive. This handles compression methods that Go's archive/zip doesn't
// support (e.g. Deflate64/method 9).
func extractZipExternal(zipPath, targetName string) (string, error) {
	tmpDir, err := os.MkdirTemp("", "hospital-loader-unzip-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	cmd := exec.Command("unzip", "-o", "-d", tmpDir, zipPath, targetName)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("unzip %s: %w: %s", targetName, err, out)
	}

	extracted := filepath.Join(tmpDir, targetName)
	if _, err := os.Stat(extracted); err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("extracted file not found: %s", targetName)
	}

	return extracted, nil
}

// utlsTransport is an http.RoundTripper that uses a Chrome TLS fingerprint.
// It checks the negotiated ALPN protocol and delegates to either the HTTP/2
// or HTTP/1.1 transport accordingly.
type utlsTransport struct {
	h1 *http.Transport
	h2 *http2.Transport
}

func (t *utlsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// For non-HTTPS, use default transport.
	if req.URL.Scheme != "https" {
		return t.h1.RoundTrip(req)
	}

	addr := req.URL.Host
	if !strings.Contains(addr, ":") {
		addr += ":443"
	}

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	host, _, _ := net.SplitHostPort(addr)
	tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
	if err := tlsConn.HandshakeContext(req.Context()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("TLS handshake: %w", err)
	}

	alpn := tlsConn.ConnectionState().NegotiatedProtocol
	if alpn == "h2" {
		return t.h2.RoundTrip(req)
	}

	// HTTP/1.1: write request and read response directly.
	if err := req.Write(tlsConn); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("write request: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tlsConn), req)
	if err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}

var chromeClient = &http.Client{
	Transport: &utlsTransport{
		h1: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
		},
		h2: &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				conn, err := net.DialTimeout(network, addr, 30*time.Second)
				if err != nil {
					return nil, err
				}
				host, _, _ := net.SplitHostPort(addr)
				tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, err
				}
				return tlsConn, nil
			},
		},
	},
}

// hostRateLimiter throttles concurrent requests per hostname to avoid
// triggering CDN bot detection (e.g., Akamai WAF). When multiple parallel
// workers try to download from the same host, this ensures a minimum gap
// between requests.
var hostRateLimiter = struct {
	sync.Mutex
	lastReq map[string]time.Time
}{lastReq: make(map[string]time.Time)}

const perHostMinGap = 5 * time.Second

// throttleHost blocks until at least perHostMinGap has elapsed since the
// last request to the given host.
func throttleHost(rawURL string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return
	}
	host := u.Hostname()

	hostRateLimiter.Lock()
	for {
		last := hostRateLimiter.lastReq[host]
		if gap := time.Since(last); gap >= perHostMinGap {
			break
		} else {
			wait := perHostMinGap - gap
			hostRateLimiter.Unlock()
			time.Sleep(wait)
			hostRateLimiter.Lock()
		}
	}
	hostRateLimiter.lastReq[host] = time.Now()
	hostRateLimiter.Unlock()
}

// downloadResult holds the result of a single HTTP download.
type downloadResult struct {
	N        int64
	Filename string // from Content-Disposition, if present
}

// errJSChallenge is returned when a WAF (e.g. Sucuri) returns a JS challenge
// instead of the actual file. The caller should fall back to a headless browser.
var errJSChallenge = fmt.Errorf("WAF JS challenge detected")

// doDownload performs a single HTTP GET and writes the response body to w.
func doDownload(w io.Writer, rawURL string) (downloadResult, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return downloadResult{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Sec-Ch-Ua", `"Chromium";v="131", "Not_A Brand";v="24"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"macOS"`)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	resp, err := chromeClient.Do(req)
	if err != nil {
		return downloadResult{}, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Detect Sucuri WAF JS challenge: 307 with X-Sucuri-ID header and no Location.
		if resp.StatusCode == 307 && resp.Header.Get("X-Sucuri-ID") != "" && resp.Header.Get("Location") == "" {
			return downloadResult{}, errJSChallenge
		}
		return downloadResult{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	// Decompress gzip response body if Content-Encoding indicates gzip.
	// (When Accept-Encoding is set explicitly, Go's http.Client does NOT
	// auto-decompress, so we must do it ourselves.)
	body := resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return downloadResult{}, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		body = gz
	}

	// Extract filename from Content-Disposition header if present.
	var filename string
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			filename = params["filename"]
		}
	}

	// Copy with speed check: after 1 minutes of downloading, if the
	// estimated remaining time exceeds 10 minutes, abort.
	const (
		steadyStateAfter = 1 * time.Minute
		maxETA           = 10 * time.Minute
		checkInterval    = 10 * time.Second
	)

	contentLength := resp.ContentLength // -1 if unknown
	dlStart := time.Now()
	lastCheck := dlStart
	var totalBytes int64

	buf := make([]byte, 256*1024)
	for {
		nr, readErr := body.Read(buf)
		if nr > 0 {
			nw, writeErr := w.Write(buf[:nr])
			totalBytes += int64(nw)
			if writeErr != nil {
				return downloadResult{N: totalBytes}, fmt.Errorf("download: %w", writeErr)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return downloadResult{N: totalBytes}, fmt.Errorf("download: %w", readErr)
		}

		now := time.Now()
		if contentLength > 0 && now.Sub(lastCheck) >= checkInterval {
			lastCheck = now
			elapsed := now.Sub(dlStart)
			if elapsed >= steadyStateAfter {
				speed := float64(totalBytes) / elapsed.Seconds() // bytes/sec
				if speed > 0 {
					remaining := float64(contentLength-totalBytes) / speed
					eta := time.Duration(remaining) * time.Second
					if eta > maxETA {
						return downloadResult{N: totalBytes}, fmt.Errorf(
							"download too slow: %.1f MB downloaded in %s, ETA %s exceeds %s limit",
							float64(totalBytes)/1024/1024,
							elapsed.Round(time.Second),
							eta.Round(time.Second),
							maxETA,
						)
					}
				}
			}
		}
	}

	return downloadResult{N: totalBytes, Filename: filename}, nil
}

// doDownloadHeadless uses a headless Chrome browser to download a file,
// solving WAF JS challenges (e.g. Sucuri) that require JavaScript execution.
// It navigates to the URL, waits for the JS challenge to set cookies and
// reload, then uses the browser's own network stack to fetch the actual file.
func doDownloadHeadless(w io.Writer, rawURL string) (downloadResult, error) {
	// Use a temp file as the browser download target.
	tmpDir, err := os.MkdirTemp("", "headless-dl-*")
	if err != nil {
		return downloadResult{}, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	ctx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()

	// Set a timeout for the entire headless operation.
	ctx, timeoutCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer timeoutCancel()

	// Navigate to the URL. The JS challenge will set a cookie and reload.
	// After reload, the browser will receive the actual file content.
	// We use a JS snippet to fetch the content as text/bytes after the
	// challenge is solved.
	if err := chromedp.Run(ctx,
		chromedp.Navigate(rawURL),
		// Wait for the JS challenge to execute and reload.
		chromedp.Sleep(8*time.Second),
	); err != nil {
		return downloadResult{}, fmt.Errorf("headless navigate: %w", err)
	}

	// Use the browser's fetch API (same TLS session, same cookies) to
	// download the actual file content after the JS challenge is solved.
	var body string
	fetchJS := fmt.Sprintf(`
		(async () => {
			const r = await fetch(%q);
			if (!r.ok) throw new Error('HTTP ' + r.status);
			return await r.text();
		})()
	`, rawURL)

	awaitPromise := func(p *runtime.EvaluateParams) *runtime.EvaluateParams {
		return p.WithAwaitPromise(true)
	}
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(fetchJS, &body, awaitPromise),
	); err != nil {
		return downloadResult{}, fmt.Errorf("headless fetch: %w", err)
	}

	n, err := io.Copy(w, strings.NewReader(body))
	if err != nil {
		return downloadResult{N: n}, fmt.Errorf("headless write: %w", err)
	}

	return downloadResult{N: n}, nil
}

// sniffFileType reads the first few bytes of a file to detect if it's JSON
// or CSV. Returns ".json", ".csv", or "" if unknown.
func sniffFileType(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	buf := make([]byte, 512)
	n, err := f.Read(buf)
	if err != nil || n == 0 {
		return ""
	}

	content := buf[:n]

	// Detect zip magic (PK\x03\x04).
	if len(content) >= 4 && content[0] == 'P' && content[1] == 'K' && content[2] == 0x03 && content[3] == 0x04 {
		return ".zip"
	}

	// Skip BOM if present.
	if len(content) >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
		content = content[3:]
	}

	// Trim leading whitespace.
	s := strings.TrimLeftFunc(string(content), func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})

	if len(s) > 0 && (s[0] == '{' || s[0] == '[') {
		return ".json"
	}
	return ".csv"
}

// isGzipFile checks if a file starts with the gzip magic bytes (0x1f 0x8b).
func isGzipFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var magic [2]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return false
	}
	return magic[0] == 0x1f && magic[1] == 0x8b
}

// decompressGzipFile decompresses a gzip file to a new temp file.
// Returns the path to the decompressed file.
func decompressGzipFile(gzPath string) (string, error) {
	in, err := os.Open(gzPath)
	if err != nil {
		return "", err
	}
	defer in.Close()

	gz, err := gzip.NewReader(in)
	if err != nil {
		return "", fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()

	// Determine output extension from the original path (strip .gz if present).
	outExt := filepath.Ext(gzPath)
	if strings.EqualFold(outExt, ".gz") {
		outExt = filepath.Ext(strings.TrimSuffix(gzPath, outExt))
		if outExt == "" {
			outExt = ".json"
		}
	}

	out, err := os.CreateTemp("", "hospital-gunzip-*"+outExt)
	if err != nil {
		return "", err
	}
	defer out.Close()

	if _, err := io.Copy(out, gz); err != nil {
		os.Remove(out.Name())
		return "", fmt.Errorf("decompress: %w", err)
	}
	return out.Name(), nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
