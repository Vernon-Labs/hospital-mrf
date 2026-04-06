package internal

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const censusBatchURL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"

// GeocodeLogFile reads the JSONL log file, geocodes all unique hospital
// addresses via the US Census batch API, and rewrites the log file with
// geocode results populated.
func GeocodeLogFile(logFile string) error {
	entries, err := readLogEntries(logFile)
	if err != nil {
		return fmt.Errorf("read log entries: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Collect unique addresses, assigning each a numeric ID.
	// rawToClean maps the original address to its cleaned form.
	// addrToID maps cleaned address → ID for the Census API.
	// Skip entries that already have geocode results.
	rawToClean := make(map[string]string)
	addrToID := make(map[string]int)
	var orderedAddrs []string
	var skipped int
	var alreadyGeocoded int
	for _, e := range entries {
		if hasGeocodedLocation(e.Geocodes) {
			alreadyGeocoded++
			continue
		}
		for _, raw := range e.HospitalAddresses {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			if _, ok := rawToClean[raw]; ok {
				continue // already seen this raw address
			}
			cleaned := cleanAddress(raw)
			if cleaned == "" {
				skipped++
				rawToClean[raw] = ""
				continue
			}
			rawToClean[raw] = cleaned
			if _, ok := addrToID[cleaned]; !ok {
				addrToID[cleaned] = len(orderedAddrs) + 1
				orderedAddrs = append(orderedAddrs, cleaned)
			}
		}
	}

	if len(orderedAddrs) == 0 {
		if alreadyGeocoded > 0 {
			slog.Info("geocoding skipped, all entries already geocoded", "already_geocoded", alreadyGeocoded)
		}
		return nil
	}

	slog.Info("geocoding", "unique_addresses", len(orderedAddrs), "skipped_invalid", skipped, "already_geocoded", alreadyGeocoded)

	// Build reverse map: id → address string.
	idToAddr := make(map[int]string, len(orderedAddrs))
	for addr, id := range addrToID {
		idToAddr[id] = addr
	}

	// Initialize all addresses as unmatched.
	geoMap := make(map[string]geocodeResult, len(orderedAddrs))
	for _, addr := range orderedAddrs {
		geoMap[addr] = geocodeResult{Address: addr, Matched: false}
	}

	// Process in batches of 1000 to stay well within Census API limits.
	const batchSize = 1000
	for batchStart := 0; batchStart < len(orderedAddrs); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(orderedAddrs) {
			batchEnd = len(orderedAddrs)
		}
		batch := orderedAddrs[batchStart:batchEnd]

		slog.Info("geocoding batch", "start", batchStart+1, "end", batchEnd, "total", len(orderedAddrs))

		// Build CSV using encoding/csv for proper quoting.
		var csvBuf bytes.Buffer
		cw := csv.NewWriter(&csvBuf)
		for _, addr := range batch {
			id := addrToID[addr]
			cw.Write([]string{strconv.Itoa(id), addr, "", "", ""})
		}
		cw.Flush()

		results, err := callCensusBatchAPI(csvBuf.Bytes())
		if err != nil {
			slog.Warn("census batch failed, skipping", "start", batchStart+1, "end", batchEnd, "error", err)
			continue
		}

		for _, r := range results {
			addr, ok := idToAddr[r.id]
			if !ok {
				continue
			}
			geoMap[addr] = geocodeResult{
				Address:   addr,
				Matched:   r.matched,
				MatchType: r.matchType,
				Latitude:  r.lat,
				Longitude: r.lon,
				Source:    "census",
			}
		}
	}

	// Count Census matches.
	censusMatched := 0
	for _, g := range geoMap {
		if g.Matched {
			censusMatched++
		}
	}
	slog.Info("census geocoded", "matched", censusMatched, "total", len(orderedAddrs))

	// Fallback: use Nominatim for unmatched addresses.
	var unmatched []string
	for _, addr := range orderedAddrs {
		if !geoMap[addr].Matched {
			unmatched = append(unmatched, addr)
		}
	}
	if len(unmatched) > 0 {
		slog.Info("nominatim fallback", "unmatched", len(unmatched))
		nominatimMatched := 0
		for i, addr := range unmatched {
			if i > 0 {
				time.Sleep(1 * time.Second) // Nominatim rate limit: 1 req/sec
			}
			result, err := callNominatim(addr)
			if err != nil {
				slog.Warn("nominatim failed", "address", addr, "error", err)
				continue
			}
			if result.Matched {
				nominatimMatched++
				geoMap[addr] = result
			}
			if (i+1)%50 == 0 {
				slog.Info("nominatim progress", "done", i+1, "total", len(unmatched), "matched", nominatimMatched)
			}
		}
		slog.Info("nominatim geocoded", "matched", nominatimMatched, "total", len(unmatched))
	}

	matched := 0
	for _, g := range geoMap {
		if g.Matched {
			matched++
		}
	}
	slog.Info("geocoded total", "matched", matched, "total", len(orderedAddrs))

	// Log all failures with diagnosed reasons.
	// First: addresses skipped by cleanAddress.
	for raw, cleaned := range rawToClean {
		if cleaned == "" {
			slog.Warn("geocode skip", "address", raw, "reason", diagnoseAddressFailure(raw))
		}
	}
	// Then: addresses that went through both geocoders but still failed.
	for _, addr := range orderedAddrs {
		if !geoMap[addr].Matched {
			slog.Warn("geocode fail", "address", addr, "reason", diagnoseAddressFailure(addr))
		}
	}

	// Populate geocodes on each entry, mapping raw → cleaned → result.
	// Skip entries that already had valid geocodes (they were not re-geocoded).
	for i := range entries {
		if hasGeocodedLocation(entries[i].Geocodes) {
			continue
		}
		var geocodes []geocodeResult
		for _, raw := range entries[i].HospitalAddresses {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			cleaned := rawToClean[raw]
			if cleaned == "" {
				continue
			}
			if g, ok := geoMap[cleaned]; ok {
				g.Address = raw // preserve the original address in output
				geocodes = append(geocodes, g)
			}
		}
		entries[i].Geocodes = geocodes
	}

	// Rewrite the log file.
	return writeLogEntries(logFile, entries)
}

// hasGeocodedLocation returns true if any geocode entry has a valid lat/lon.
func hasGeocodedLocation(geocodes []geocodeResult) bool {
	for _, g := range geocodes {
		if g.Latitude != 0 || g.Longitude != 0 {
			return true
		}
	}
	return false
}

// cleanAddress normalizes an address for the Census geocoder.
// Returns empty string if the address is junk/ungeocodable.
func cleanAddress(addr string) string {
	// Strip surrounding literal quotes.
	addr = strings.Trim(addr, `"`)
	// Replace underscores with spaces (e.g. "10030_Gilead_Road_Huntersville_NC_28078").
	addr = strings.ReplaceAll(addr, "_", " ")
	// Collapse multiple spaces.
	for strings.Contains(addr, "  ") {
		addr = strings.ReplaceAll(addr, "  ", " ")
	}
	addr = strings.TrimSpace(addr)
	// Skip junk: too short or no digits (a real address has a street number or zip).
	if len(addr) < 10 {
		return ""
	}
	hasDigit := false
	for _, c := range addr {
		if c >= '0' && c <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return ""
	}
	return addr
}

var (
	reZIP5     = regexp.MustCompile(`\b\d{5}\b`)
	reZIPPlus4 = regexp.MustCompile(`\b\d{5}-\d{4}\b`)
	reLongZIP  = regexp.MustCompile(`\b\d{8,}\b`)
	reStateAbb = regexp.MustCompile(`\b[A-Z]{2}\b`)
)

// diagnoseAddressFailure returns a human-readable reason why an address
// likely failed to geocode.
func diagnoseAddressFailure(addr string) string {
	// Junk / placeholder.
	trimmed := strings.TrimSpace(addr)
	if len(trimmed) < 10 {
		return "too short / placeholder"
	}
	hasDigit := false
	for _, c := range trimmed {
		if unicode.IsDigit(c) {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return "no street number or ZIP code"
	}

	// Multiple addresses pipe-delimited.
	if strings.Contains(addr, "|") {
		return "multiple addresses in single field (pipe-delimited)"
	}

	// Puerto Rico — Census TIGER coverage is limited.
	upper := strings.ToUpper(addr)
	if strings.Contains(upper, ", PR ") || strings.HasSuffix(upper, ", PR") || strings.Contains(upper, "PUERTO RICO") {
		return "Puerto Rico address (limited geocoder coverage)"
	}

	// Malformed ZIP — digits run together with no dash.
	if reLongZIP.MatchString(addr) && !reZIPPlus4.MatchString(addr) {
		return "malformed ZIP code (digits concatenated)"
	}

	// Underscore-separated fields.
	if strings.Contains(addr, "_") {
		return "underscore-separated fields"
	}

	// Incomplete — missing ZIP.
	hasZIP := reZIP5.MatchString(addr) || reZIPPlus4.MatchString(addr)
	if !hasZIP {
		return "missing ZIP code"
	}

	// Incomplete — missing state abbreviation.
	if !reStateAbb.MatchString(addr) {
		return "missing state abbreviation"
	}

	// Duplicate suite/unit (e.g. "SUITE 300, SUITE 300").
	lowerAddr := strings.ToLower(addr)
	for _, prefix := range []string{"suite ", "ste ", "unit ", "apt "} {
		if strings.Count(lowerAddr, prefix) >= 2 {
			return "duplicate suite/unit in address"
		}
	}

	// Catch-all: private/campus road not in geocoder databases.
	return "address not found in Census TIGER or OpenStreetMap"
}

type censusResult struct {
	id        int
	matched   bool
	matchType string
	lat       float64
	lon       float64
}

func callCensusBatchAPI(csvData []byte) ([]censusResult, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	if err := w.WriteField("benchmark", "Public_AR_Current"); err != nil {
		return nil, err
	}

	fw, err := w.CreateFormFile("addressFile", "addresses.csv")
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(csvData); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	var resp *http.Response
	contentType := w.FormDataContentType()
	bodyBytes := body.Bytes()

	for attempt := 1; attempt <= 3; attempt++ {
		resp, err = http.Post(censusBatchURL, contentType, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("HTTP POST: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 && attempt < 3 {
			backoff := time.Duration(attempt) * 30 * time.Second
			slog.Warn("census API rate limited, retrying", "attempt", attempt, "backoff", backoff)
			time.Sleep(backoff)
			continue
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	defer resp.Body.Close()

	return parseCensusResponse(resp.Body)
}

// parseCensusResponse parses the Census batch geocoder CSV response.
// Columns: ID, InputAddress, Match/NoMatch, MatchType, OutputAddress, Lon/Lat, TigerlineID, Side
// The coordinates column is formatted as "lon,lat" (longitude first).
func parseCensusResponse(r io.Reader) ([]censusResult, error) {
	var results []censusResult
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // variable number of fields

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return results, fmt.Errorf("parse CSV row: %w", err)
		}

		if len(record) < 3 {
			continue
		}

		id, err := strconv.Atoi(strings.TrimSpace(record[0]))
		if err != nil {
			continue
		}

		matchIndicator := strings.TrimSpace(record[2])
		matched := strings.EqualFold(matchIndicator, "Match")

		cr := censusResult{id: id, matched: matched}

		if matched && len(record) >= 6 {
			cr.matchType = strings.TrimSpace(record[3])
			// record[5] contains coordinates as "lon,lat"
			coords := strings.TrimSpace(record[5])
			parts := strings.SplitN(coords, ",", 2)
			if len(parts) == 2 {
				cr.lon, _ = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
				cr.lat, _ = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			}
		}

		results = append(results, cr)
	}

	return results, nil
}

var nominatimClient = &http.Client{Timeout: 10 * time.Second}

// callNominatim geocodes a single address via the Nominatim API.
func callNominatim(address string) (geocodeResult, error) {
	req, err := http.NewRequest("GET", "https://nominatim.openstreetmap.org/search", nil)
	if err != nil {
		return geocodeResult{}, err
	}
	q := req.URL.Query()
	q.Set("q", address)
	q.Set("format", "json")
	q.Set("limit", "1")
	q.Set("countrycodes", "us")
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "hospital-loader/1.0 (hospital MRF geocoding)")

	var resp *http.Response
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err = nominatimClient.Do(req)
		if err != nil {
			return geocodeResult{}, fmt.Errorf("HTTP GET: %w", err)
		}
		if resp.StatusCode == 200 {
			break
		}
		resp.Body.Close()
		if resp.StatusCode == 429 && attempt < 3 {
			backoff := time.Duration(attempt) * 10 * time.Second
			slog.Warn("nominatim rate limited, retrying", "address", address, "attempt", attempt, "backoff", backoff)
			time.Sleep(backoff)
			continue
		}
		return geocodeResult{}, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	var results []struct {
		Lat         string `json:"lat"`
		Lon         string `json:"lon"`
		DisplayName string `json:"display_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return geocodeResult{}, fmt.Errorf("parse response: %w", err)
	}

	if len(results) == 0 {
		return geocodeResult{Address: address, Matched: false, Source: "nominatim"}, nil
	}

	lat, _ := strconv.ParseFloat(results[0].Lat, 64)
	lon, _ := strconv.ParseFloat(results[0].Lon, 64)

	return geocodeResult{
		Address:   address,
		Matched:   true,
		MatchType: "nominatim",
		Latitude:  lat,
		Longitude: lon,
		Source:    "nominatim",
	}, nil
}

func readLogEntries(path string) ([]logEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []logEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var e logEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		entries = append(entries, e)
	}
	return entries, scanner.Err()
}

func writeLogEntries(path string, entries []logEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal log entry: %w", err)
		}
		w.Write(data)
		w.WriteByte('\n')
	}
	return w.Flush()
}
