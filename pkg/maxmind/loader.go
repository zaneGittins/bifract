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

	"github.com/ClickHouse/clickhouse-go/v2"
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
// On startup (initial=true), it skips the download if ClickHouse already has
// GeoIP data from a previous run, and just ensures dictionaries exist.
func (m *Manager) LoadAll(ctx context.Context) error {
	return m.loadAll(ctx, false)
}

// LoadAllInitial is like LoadAll but skips downloading if ClickHouse already
// has GeoIP data, avoiding unnecessary MaxMind API calls on restart.
func (m *Manager) LoadAllInitial(ctx context.Context) error {
	return m.loadAll(ctx, true)
}

func (m *Manager) loadAll(ctx context.Context, initial bool) error {
	// On startup, check if ClickHouse already has data from a previous run.
	// If so, just ensure the dictionaries exist and skip the download entirely.
	if initial && m.hasExistingData(ctx) {
		if m.hasOffShardRows(ctx) {
			// Rows written before the Distributed companion existed sit on whichever
			// shard answered, so the same network prefix can appear on several shards
			// and IP_TRIE resolves it arbitrarily. A full reload rewrites the table
			// through the companion, onto one shard.
			log.Println("[MaxMind] GeoIP rows are spread across shards, reloading from source...")
		} else {
			log.Println("[MaxMind] ClickHouse already has GeoIP data, ensuring dictionaries exist...")
			if err := m.ensureDictionaries(ctx); err != nil {
				log.Printf("[MaxMind] Failed to ensure dictionaries, will do full reload: %v", err)
			} else {
				m.mu.Lock()
				m.loaded = true
				m.mu.Unlock()
				return nil
			}
		}
	}

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

// hasExistingData checks if both geoip_city and geoip_asn tables exist and
// have rows from a previous load.
func (m *Manager) hasExistingData(ctx context.Context) bool {
	for _, table := range geoipTables {
		// An install that predates the companion has none; creating it here is what
		// makes the count below (and the reload decision after it) answerable.
		if err := m.ensureDistTable(ctx, table); err != nil {
			return false
		}
		var count uint64
		row := m.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", m.geoipReadTable(table)))
		if err := row.Scan(&count); err != nil || count == 0 {
			return false
		}
	}
	return true
}

// hasOffShardRows reports whether any GeoIP row sits on a shard other than the one the
// companion writes to. Always false on a single node.
func (m *Manager) hasOffShardRows(ctx context.Context) bool {
	if !m.ch.Topology().DistributedTables {
		return false
	}
	for _, table := range geoipTables {
		var count uint64
		row := m.ch.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE _shard_num != %d",
			m.geoipReadTable(table), writeShardNum))
		if err := row.Scan(&count); err != nil {
			return false
		}
		if count > 0 {
			return true
		}
	}
	return false
}

// ensureDictionaries creates the IP_TRIE dictionaries if they don't already
// exist. This is used on startup when the backing tables already have data.
func (m *Manager) ensureDictionaries(ctx context.Context) error {
	if err := m.createCityDictionary(ctx); err != nil {
		return err
	}
	return m.createASNDictionary(ctx)
}

// geoipTables are the local backing tables the GeoIP dictionaries read from.
var geoipTables = []string{"geoip_city", "geoip_asn"}

// writeShardNum is the shard every GeoIP row lands on: the Distributed companion's
// sharding key is the constant 0, which resolves to the first shard.
const writeShardNum = 1

// geoipReadTable names the local backing table and, on a cluster, the Distributed
// companion every read and write goes through. The backing tables are Replicated per
// shard, so rows written through the load-balanced pool reach one shard only and every
// other shard's copy of the dictionary loads empty. The companion's sharding key is
// the constant 0, pinning the whole table to the first shard, which each shard's
// dictionary then reads in full.
func (m *Manager) geoipReadTable(local string) string {
	if m.ch.Topology().DistributedTables {
		return local + "_distributed"
	}
	return local
}

// ensureDistTable creates or replaces a backing table's Distributed companion. It
// holds no data, so replacing it rather than skipping an existing one keeps its
// columns in step with the local table for free.
func (m *Manager) ensureDistTable(ctx context.Context, local string) error {
	if !m.ch.Topology().DistributedTables {
		return nil
	}
	sql := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s ENGINE = Distributed('%s', currentDatabase(), '%s', 0)",
		m.geoipReadTable(local), local, storage.EscCHStr(m.ch.Topology().DDLCluster), local)
	if err := m.ch.Exec(ctx, m.ch.InjectOnCluster(sql)); err != nil && !storage.IsDDLTimeout(err) {
		return err
	}
	return nil
}

// reloadSQL renders a cluster-aware SYSTEM RELOAD DICTIONARY. Every node keeps its own
// copy, so reloading only the one this connection landed on leaves the rest stale, and
// an unqualified name resolves against whatever database that node is using.
func (m *Manager) reloadSQL(name string) string {
	db := strings.ReplaceAll(m.ch.LogsDatabase(), "`", "")
	return fmt.Sprintf("SYSTEM RELOAD DICTIONARY%s `%s`.`%s`", m.ch.OnClusterSQL(), db, name)
}

// insertCtx makes a Distributed insert synchronous, so the dictionary created right
// after the load reads a table that already holds every row.
func (m *Manager) insertCtx(ctx context.Context) context.Context {
	if !m.ch.Topology().DistributedTables {
		return ctx
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"distributed_foreground_insert": 1,
	}))
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
	createCitySQL := m.ch.RewriteEngine(m.ch.InjectOnCluster(`
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
	`))
	if err := m.ch.Exec(ctx, createCitySQL); err != nil {
		return fmt.Errorf("create geoip_city table: %w", err)
	}
	if err := m.ensureDistTable(ctx, "geoip_city"); err != nil {
		return fmt.Errorf("create geoip_city distributed table: %w", err)
	}

	// Truncate before reload
	if err := m.ch.Exec(ctx, m.ch.InjectOnCluster("TRUNCATE TABLE geoip_city")); err != nil {
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

	batch, err := m.ch.Conn().PrepareBatch(m.insertCtx(ctx), "INSERT INTO "+m.geoipReadTable("geoip_city")+" (network, country, city, subdivision, continent, timezone, latitude, longitude, postal_code)")
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
			batch, err = m.ch.Conn().PrepareBatch(m.insertCtx(ctx), "INSERT INTO "+m.geoipReadTable("geoip_city")+" (network, country, city, subdivision, continent, timezone, latitude, longitude, postal_code)")
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
	dictSQL := m.ch.InjectOnCluster(fmt.Sprintf(`
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
		SOURCE(CLICKHOUSE(TABLE '%s' DB '%s' USER '%s' PASSWORD '%s'))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(IP_TRIE())
	`, m.geoipReadTable("geoip_city"), m.ch.LogsDatabase(), m.ch.User, m.ch.Password))

	if err := m.ch.Exec(ctx, dictSQL); err != nil {
		return fmt.Errorf("create geoip_city_lookup dictionary: %w", err)
	}

	if err := m.ch.Exec(ctx, m.reloadSQL("geoip_city_lookup")); err != nil {
		return fmt.Errorf("reload geoip_city_lookup: %w", err)
	}

	log.Println("[MaxMind] Created geoip_city_lookup dictionary")
	return nil
}

// loadASN parses GeoLite2 ASN CSVs and loads into ClickHouse.
func (m *Manager) loadASN(ctx context.Context, csvDir string) error {
	// Create backing table
	createASNSQL := m.ch.RewriteEngine(m.ch.InjectOnCluster(`
		CREATE TABLE IF NOT EXISTS geoip_asn (
			network String,
			asn UInt32 DEFAULT 0,
			as_org String DEFAULT ''
		) ENGINE = MergeTree() ORDER BY network
	`))
	if err := m.ch.Exec(ctx, createASNSQL); err != nil {
		return fmt.Errorf("create geoip_asn table: %w", err)
	}
	if err := m.ensureDistTable(ctx, "geoip_asn"); err != nil {
		return fmt.Errorf("create geoip_asn distributed table: %w", err)
	}

	// Truncate before reload
	if err := m.ch.Exec(ctx, m.ch.InjectOnCluster("TRUNCATE TABLE geoip_asn")); err != nil {
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

	batch, err := m.ch.Conn().PrepareBatch(m.insertCtx(ctx), "INSERT INTO "+m.geoipReadTable("geoip_asn")+" (network, asn, as_org)")
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
			batch, err = m.ch.Conn().PrepareBatch(m.insertCtx(ctx), "INSERT INTO "+m.geoipReadTable("geoip_asn")+" (network, asn, as_org)")
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
	dictSQL := m.ch.InjectOnCluster(fmt.Sprintf(`
		CREATE OR REPLACE DICTIONARY geoip_asn_lookup (
			network String,
			asn UInt32 DEFAULT 0,
			as_org String DEFAULT ''
		)
		PRIMARY KEY network
		SOURCE(CLICKHOUSE(TABLE '%s' DB '%s' USER '%s' PASSWORD '%s'))
		LIFETIME(MIN 0 MAX 3600)
		LAYOUT(IP_TRIE())
	`, m.geoipReadTable("geoip_asn"), m.ch.LogsDatabase(), m.ch.User, m.ch.Password))

	if err := m.ch.Exec(ctx, dictSQL); err != nil {
		return fmt.Errorf("create geoip_asn_lookup dictionary: %w", err)
	}

	if err := m.ch.Exec(ctx, m.reloadSQL("geoip_asn_lookup")); err != nil {
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
