package maxmind

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"bifract/pkg/storage"
)

// Manager handles downloading, parsing, and loading MaxMind GeoLite2 data
// into ClickHouse IP_TRIE dictionaries.
type Manager struct {
	ch     *storage.ClickHouseClient
	cfg    *Config
	loaded bool
	mu     sync.RWMutex
	stopCh chan struct{}
}

// NewManager creates a new MaxMind manager.
func NewManager(ch *storage.ClickHouseClient, cfg *Config) *Manager {
	return &Manager{
		ch:     ch,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// IsLoaded returns whether GeoIP dictionaries are available.
func (m *Manager) IsLoaded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.loaded
}

// Start launches a daily refresh goroutine.
func (m *Manager) Start() {
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("[MaxMind] Starting daily GeoIP refresh...")
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
				if err := m.LoadAll(ctx); err != nil {
					log.Printf("[MaxMind] Daily refresh failed: %v", err)
				} else {
					log.Println("[MaxMind] Daily refresh completed")
				}
				cancel()
			case <-m.stopCh:
				return
			}
		}
	}()
}

// Stop signals the refresh goroutine to exit.
func (m *Manager) Stop() {
	close(m.stopCh)
}

// LoadAll downloads all configured editions and loads them into ClickHouse.
func (m *Manager) LoadAll(ctx context.Context) error {
	for _, edition := range m.cfg.EditionIDs {
		log.Printf("[MaxMind] Downloading %s...", edition)
		csvDir, err := Download(m.cfg, edition)
		if err != nil {
			return fmt.Errorf("download %s: %w", edition, err)
		}

		switch {
		case strings.Contains(edition, "City"):
			if err := m.loadCity(ctx, csvDir); err != nil {
				return fmt.Errorf("load city data: %w", err)
			}
		case strings.Contains(edition, "ASN"):
			if err := m.loadASN(ctx, csvDir); err != nil {
				return fmt.Errorf("load ASN data: %w", err)
			}
		default:
			log.Printf("[MaxMind] Skipping unknown edition: %s", edition)
		}
	}

	m.mu.Lock()
	m.loaded = true
	m.mu.Unlock()
	return nil
}

// cityLocation holds denormalized location data keyed by geoname_id.
type cityLocation struct {
	Country     string
	City        string
	Subdivision string
	Continent   string
	Timezone    string
}

// loadCity parses GeoLite2 City CSVs, denormalizes blocks+locations, and loads
// into ClickHouse.
func (m *Manager) loadCity(ctx context.Context, csvDir string) error {
	// Parse locations first
	locPath := filepath.Join(csvDir, "GeoLite2-City-Locations-en.csv")
	locations, err := parseCityLocations(locPath)
	if err != nil {
		return fmt.Errorf("parse locations: %w", err)
	}
	log.Printf("[MaxMind] Parsed %d city locations", len(locations))

	// Create backing table
	if err := m.ch.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS geoip_city (
			network String,
			country String DEFAULT '',
			city String DEFAULT '',
			subdivision String DEFAULT '',
			continent String DEFAULT '',
			timezone String DEFAULT '',
			latitude Float64 DEFAULT 0,
			longitude Float64 DEFAULT 0,
			postal_code String DEFAULT ''
		) ENGINE = MergeTree() ORDER BY network
	`); err != nil {
		return fmt.Errorf("create geoip_city table: %w", err)
	}

	// Truncate before reload
	if err := m.ch.Exec(ctx, "TRUNCATE TABLE geoip_city"); err != nil {
		return fmt.Errorf("truncate geoip_city: %w", err)
	}

	// Parse blocks and batch insert
	blocksPath := filepath.Join(csvDir, "GeoLite2-City-Blocks-IPv4.csv")
	count, err := m.loadCityBlocks(ctx, blocksPath, locations)
	if err != nil {
		return fmt.Errorf("load city blocks: %w", err)
	}
	log.Printf("[MaxMind] Loaded %d city block rows", count)

	// Create IP_TRIE dictionary
	if err := m.createCityDictionary(ctx); err != nil {
		return fmt.Errorf("create city dictionary: %w", err)
	}

	return nil
}

// parseCityLocations reads the locations CSV into a geoname_id -> cityLocation map.
func parseCityLocations(path string) (map[string]*cityLocation, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := csvIndex(header)
	locations := make(map[string]*cityLocation)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		geonameID := csvField(record, idx, "geoname_id")
		if geonameID == "" {
			continue
		}

		locations[geonameID] = &cityLocation{
			Country:     csvField(record, idx, "country_name"),
			City:        csvField(record, idx, "city_name"),
			Subdivision: csvField(record, idx, "subdivision_1_name"),
			Continent:   csvField(record, idx, "continent_name"),
			Timezone:    csvField(record, idx, "time_zone"),
		}
	}

	return locations, nil
}

// loadCityBlocks reads the blocks CSV, joins with locations, and batch-inserts
// into ClickHouse.
func (m *Manager) loadCityBlocks(ctx context.Context, path string, locations map[string]*cityLocation) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}

	idx := csvIndex(header)
	total := 0
	const batchSize = 10000

	batch, err := m.ch.Conn().PrepareBatch(ctx, "INSERT INTO geoip_city (network, country, city, subdivision, continent, timezone, latitude, longitude, postal_code)")
	if err != nil {
		return 0, fmt.Errorf("prepare batch: %w", err)
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		network := csvField(record, idx, "network")
		if network == "" {
			continue
		}

		geonameID := csvField(record, idx, "geoname_id")
		loc := locations[geonameID]

		country, city, subdivision, continent, timezone := "", "", "", "", ""
		if loc != nil {
			country = loc.Country
			city = loc.City
			subdivision = loc.Subdivision
			continent = loc.Continent
			timezone = loc.Timezone
		}

		lat := parseFloat(csvField(record, idx, "latitude"))
		lon := parseFloat(csvField(record, idx, "longitude"))
		postalCode := csvField(record, idx, "postal_code")

		if err := batch.Append(network, country, city, subdivision, continent, timezone, lat, lon, postalCode); err != nil {
			return 0, fmt.Errorf("append row: %w", err)
		}

		total++
		if total%batchSize == 0 {
			if err := batch.Send(); err != nil {
				return 0, fmt.Errorf("send batch at row %d: %w", total, err)
			}
			batch, err = m.ch.Conn().PrepareBatch(ctx, "INSERT INTO geoip_city (network, country, city, subdivision, continent, timezone, latitude, longitude, postal_code)")
			if err != nil {
				return 0, fmt.Errorf("prepare next batch: %w", err)
			}
		}
	}

	// Send remaining rows
	if total%batchSize != 0 {
		if err := batch.Send(); err != nil {
			return 0, fmt.Errorf("send final batch: %w", err)
		}
	}

	return total, nil
}

func (m *Manager) createCityDictionary(ctx context.Context) error {
	sql := fmt.Sprintf(`
		CREATE OR REPLACE DICTIONARY geoip_city_lookup (
			network String,
			country String DEFAULT '',
			city String DEFAULT '',
			subdivision String DEFAULT '',
			continent String DEFAULT '',
			timezone String DEFAULT '',
			latitude Float64 DEFAULT 0,
			longitude Float64 DEFAULT 0,
			postal_code String DEFAULT ''
		)
		PRIMARY KEY network
		SOURCE(CLICKHOUSE(TABLE 'geoip_city' DB '%s' USER '%s' PASSWORD '%s'))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(IP_TRIE())
	`, m.ch.Database, m.ch.User, m.ch.Password)

	if err := m.ch.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create geoip_city_lookup dictionary: %w", err)
	}

	// Force reload
	if err := m.ch.Exec(ctx, "SYSTEM RELOAD DICTIONARY geoip_city_lookup"); err != nil {
		return fmt.Errorf("reload geoip_city_lookup: %w", err)
	}

	log.Println("[MaxMind] Created geoip_city_lookup dictionary")
	return nil
}

// loadASN parses GeoLite2 ASN CSVs and loads into ClickHouse.
func (m *Manager) loadASN(ctx context.Context, csvDir string) error {
	// Create backing table
	if err := m.ch.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS geoip_asn (
			network String,
			asn UInt32 DEFAULT 0,
			as_org String DEFAULT ''
		) ENGINE = MergeTree() ORDER BY network
	`); err != nil {
		return fmt.Errorf("create geoip_asn table: %w", err)
	}

	// Truncate before reload
	if err := m.ch.Exec(ctx, "TRUNCATE TABLE geoip_asn"); err != nil {
		return fmt.Errorf("truncate geoip_asn: %w", err)
	}

	blocksPath := filepath.Join(csvDir, "GeoLite2-ASN-Blocks-IPv4.csv")
	count, err := m.loadASNBlocks(ctx, blocksPath)
	if err != nil {
		return fmt.Errorf("load ASN blocks: %w", err)
	}
	log.Printf("[MaxMind] Loaded %d ASN block rows", count)

	if err := m.createASNDictionary(ctx); err != nil {
		return fmt.Errorf("create ASN dictionary: %w", err)
	}

	return nil
}

func (m *Manager) loadASNBlocks(ctx context.Context, path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}

	idx := csvIndex(header)
	total := 0
	const batchSize = 10000

	batch, err := m.ch.Conn().PrepareBatch(ctx, "INSERT INTO geoip_asn (network, asn, as_org)")
	if err != nil {
		return 0, fmt.Errorf("prepare batch: %w", err)
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		network := csvField(record, idx, "network")
		if network == "" {
			continue
		}

		asn := parseUint32(csvField(record, idx, "autonomous_system_number"))
		asOrg := csvField(record, idx, "autonomous_system_organization")

		if err := batch.Append(network, asn, asOrg); err != nil {
			return 0, fmt.Errorf("append row: %w", err)
		}

		total++
		if total%batchSize == 0 {
			if err := batch.Send(); err != nil {
				return 0, fmt.Errorf("send batch at row %d: %w", total, err)
			}
			batch, err = m.ch.Conn().PrepareBatch(ctx, "INSERT INTO geoip_asn (network, asn, as_org)")
			if err != nil {
				return 0, fmt.Errorf("prepare next batch: %w", err)
			}
		}
	}

	if total%batchSize != 0 {
		if err := batch.Send(); err != nil {
			return 0, fmt.Errorf("send final batch: %w", err)
		}
	}

	return total, nil
}

func (m *Manager) createASNDictionary(ctx context.Context) error {
	sql := fmt.Sprintf(`
		CREATE OR REPLACE DICTIONARY geoip_asn_lookup (
			network String,
			asn UInt32 DEFAULT 0,
			as_org String DEFAULT ''
		)
		PRIMARY KEY network
		SOURCE(CLICKHOUSE(TABLE 'geoip_asn' DB '%s' USER '%s' PASSWORD '%s'))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(IP_TRIE())
	`, m.ch.Database, m.ch.User, m.ch.Password)

	if err := m.ch.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create geoip_asn_lookup dictionary: %w", err)
	}

	if err := m.ch.Exec(ctx, "SYSTEM RELOAD DICTIONARY geoip_asn_lookup"); err != nil {
		return fmt.Errorf("reload geoip_asn_lookup: %w", err)
	}

	log.Println("[MaxMind] Created geoip_asn_lookup dictionary")
	return nil
}

// csvIndex builds a column name -> index map from a CSV header row.
func csvIndex(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, col := range header {
		m[strings.TrimSpace(col)] = i
	}
	return m
}

// csvField safely retrieves a field from a CSV record by column name.
func csvField(record []string, idx map[string]int, col string) string {
	i, ok := idx[col]
	if !ok || i >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[i])
}

func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseUint32(s string) uint32 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 32)
	return uint32(v)
}
