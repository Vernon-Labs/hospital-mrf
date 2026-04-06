package internal

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestParseCensusResponse(t *testing.T) {
	// Sample Census batch geocoder response.
	resp := `"1","5995 Spring St, Warm Springs, GA 31830","Match","Exact","5995 Spring St, Warm Springs, GA, 31830","-84.67,32.89","123456","L"
"2","123 Fake St, Nowhere, ZZ 00000","No_Match"
"3","1100 Mercer Ave, Decatur, IN 46733","Match","Non_Exact","1100 Mercer Ave, Decatur, IN, 46733","-84.93,40.83","789012","R"
`

	results, err := parseCensusResponse(strings.NewReader(resp))
	if err != nil {
		t.Fatalf("parseCensusResponse: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First: matched exact
	if !results[0].matched {
		t.Error("result 0: expected matched=true")
	}
	if results[0].matchType != "Exact" {
		t.Errorf("result 0: matchType=%q, want Exact", results[0].matchType)
	}
	if results[0].lon != -84.67 || results[0].lat != 32.89 {
		t.Errorf("result 0: coords=(%v,%v), want (-84.67,32.89)", results[0].lon, results[0].lat)
	}

	// Second: no match
	if results[1].matched {
		t.Error("result 1: expected matched=false")
	}

	// Third: matched non-exact
	if !results[2].matched {
		t.Error("result 2: expected matched=true")
	}
	if results[2].matchType != "Non_Exact" {
		t.Errorf("result 2: matchType=%q, want Non_Exact", results[2].matchType)
	}
}

func TestGeocodeLogFilePreservesExistingGeocodes(t *testing.T) {
	// Entry 1: already has valid geocode (lat/lon) — should be preserved.
	alreadyGeocoded := logEntry{
		Success:           true,
		URL:               "https://example.com/hospital_a.csv",
		HospitalName:      "Hospital A",
		HospitalAddresses: []string{"550 1st Ave., New York, NY 10016"},
		Geocodes: []geocodeResult{
			{
				Address:   "550 1st Ave., New York, NY 10016",
				Matched:   true,
				MatchType: "Exact",
				Latitude:  40.742346,
				Longitude: -73.974474,
				Source:    "census",
			},
		},
	}

	// Entry 2: has geocode but no lat/lon (unmatched) — should be re-geocoded.
	unmatchedGeocode := logEntry{
		Success:           true,
		URL:               "https://example.com/hospital_b.csv",
		HospitalName:      "Hospital B",
		HospitalAddresses: []string{"123 Main St, Springfield, IL 62704"},
		Geocodes: []geocodeResult{
			{
				Address: "123 Main St, Springfield, IL 62704",
				Matched: false,
			},
		},
	}

	// Entry 3: no geocodes at all — should be geocoded.
	noGeocode := logEntry{
		Success:           true,
		URL:               "https://example.com/hospital_c.csv",
		HospitalName:      "Hospital C",
		HospitalAddresses: []string{"300 Pasteur Drive, Stanford, CA 94305"},
	}

	// Write test JSONL file.
	tmpFile, err := os.CreateTemp("", "geocode-test-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	for _, e := range []logEntry{alreadyGeocoded, unmatchedGeocode, noGeocode} {
		data, _ := json.Marshal(e)
		tmpFile.Write(data)
		tmpFile.Write([]byte("\n"))
	}
	tmpFile.Close()

	// Run GeocodeLogFile — this calls external APIs for entries 2 and 3,
	// but entry 1 should be untouched.
	err = GeocodeLogFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("GeocodeLogFile: %v", err)
	}

	// Read back the file.
	entries, err := readLogEntries(tmpFile.Name())
	if err != nil {
		t.Fatalf("readLogEntries: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Entry 1: geocode must be preserved exactly.
	e1 := entries[0]
	if len(e1.Geocodes) != 1 {
		t.Fatalf("entry 1: expected 1 geocode, got %d", len(e1.Geocodes))
	}
	if e1.Geocodes[0].Latitude != 40.742346 {
		t.Errorf("entry 1: latitude=%v, want 40.742346", e1.Geocodes[0].Latitude)
	}
	if e1.Geocodes[0].Longitude != -73.974474 {
		t.Errorf("entry 1: longitude=%v, want -73.974474", e1.Geocodes[0].Longitude)
	}
	if e1.Geocodes[0].Source != "census" {
		t.Errorf("entry 1: source=%q, want census", e1.Geocodes[0].Source)
	}

	// Entry 2: had unmatched geocode (lat=0,lon=0), should have been re-geocoded.
	// We can't assert the exact result (depends on external API), but it should
	// not still have the original unmatched entry with lat=0/lon=0 unchanged
	// (unless the APIs also fail, which is possible).
	e2 := entries[1]
	t.Logf("entry 2 geocodes: %d", len(e2.Geocodes))
	for _, g := range e2.Geocodes {
		t.Logf("  addr=%s matched=%v lat=%v lon=%v source=%s", g.Address, g.Matched, g.Latitude, g.Longitude, g.Source)
	}

	// Entry 3: had no geocodes, should now have some (from external API).
	e3 := entries[2]
	t.Logf("entry 3 geocodes: %d", len(e3.Geocodes))
	for _, g := range e3.Geocodes {
		t.Logf("  addr=%s matched=%v lat=%v lon=%v source=%s", g.Address, g.Matched, g.Latitude, g.Longitude, g.Source)
	}
}

func TestHasGeocodedLocation(t *testing.T) {
	tests := []struct {
		name     string
		geocodes []geocodeResult
		want     bool
	}{
		{"nil", nil, false},
		{"empty", []geocodeResult{}, false},
		{"unmatched zero coords", []geocodeResult{{Matched: false}}, false},
		{"matched with coords", []geocodeResult{{Latitude: 40.7, Longitude: -73.9}}, true},
		{"only lat", []geocodeResult{{Latitude: 40.7}}, true},
		{"only lon", []geocodeResult{{Longitude: -73.9}}, true},
		{"multiple mixed", []geocodeResult{{Matched: false}, {Latitude: 40.7, Longitude: -73.9}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasGeocodedLocation(tt.geocodes)
			if got != tt.want {
				t.Errorf("hasGeocodedLocation() = %v, want %v", got, tt.want)
			}
		})
	}
}
