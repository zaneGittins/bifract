package dictionaries

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"bifract/pkg/storage"
)

// Manager handles dictionary CRUD and ClickHouse dictionary lifecycle.
type Manager struct {
	pg   *storage.PostgresClient
	ch   *storage.ClickHouseClient
	chDB string // ClickHouse database name
}

// NewManager creates a new dictionary manager.
func NewManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Manager {
	return &Manager{pg: pg, ch: ch, chDB: "logs"}
}

// chTableName returns the ClickHouse backing table name for a dictionary ID.
func chTableName(id string) string {
	return "dict_" + strings.ReplaceAll(id, "-", "_")
}

// chDictName returns the primary ClickHouse dictionary object name for a dictionary ID.
func chDictName(id string) string {
	return "lookup_" + strings.ReplaceAll(id, "-", "_")
}

// chColDictName returns the ClickHouse dictionary name for a secondary key column.
func chColDictName(id, colName string) string {
	return "lookup_" + strings.ReplaceAll(id, "-", "_") + "_by_" + sanitizeColForCH(colName)
}

// sanitizeColForCH converts a column name to a safe ClickHouse identifier component.
func sanitizeColForCH(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// ---- List / Get ----

// ListDictionaries returns all dictionaries for a fractal or prism (pass one, leave other empty).
// Global dictionaries (is_global=true) are always included regardless of scope.
func (m *Manager) ListDictionaries(ctx context.Context, fractalID, prismID string) ([]*Dictionary, error) {
	var q string
	var arg string
	if prismID != "" {
		q = `SELECT id, name, description, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), is_global, key_column, columns, row_count, COALESCE(created_by, ''), created_at, updated_at
		     FROM dictionaries WHERE prism_id = $1 OR is_global = true ORDER BY name ASC`
		arg = prismID
	} else {
		q = `SELECT id, name, description, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), is_global, key_column, columns, row_count, COALESCE(created_by, ''), created_at, updated_at
		     FROM dictionaries WHERE fractal_id = $1 OR is_global = true ORDER BY name ASC`
		arg = fractalID
	}
	rows, err := m.pg.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dicts []*Dictionary
	for rows.Next() {
		d := &Dictionary{}
		var colsJSON []byte
		if err := rows.Scan(&d.ID, &d.Name, &d.Description, &d.FractalID, &d.PrismID, &d.IsGlobal, &d.KeyColumn,
			&colsJSON, &d.RowCount, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(colsJSON, &d.Columns); err != nil {
			d.Columns = []DictionaryColumn{}
		}
		d.CHTableName = chTableName(d.ID)
		d.CHDictName = chDictName(d.ID)
		dicts = append(dicts, d)
	}
	return dicts, rows.Err()
}

// GetDictionary returns a single dictionary by ID.
func (m *Manager) GetDictionary(ctx context.Context, id string) (*Dictionary, error) {
	d := &Dictionary{}
	var colsJSON []byte
	err := m.pg.QueryRow(ctx,
		`SELECT id, name, description, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), is_global, key_column, columns, row_count, COALESCE(created_by, ''), created_at, updated_at
		 FROM dictionaries WHERE id = $1`, id).
		Scan(&d.ID, &d.Name, &d.Description, &d.FractalID, &d.PrismID, &d.IsGlobal, &d.KeyColumn,
			&colsJSON, &d.RowCount, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(colsJSON, &d.Columns); err != nil {
		d.Columns = []DictionaryColumn{}
	}
	d.CHTableName = chTableName(d.ID)
	d.CHDictName = chDictName(d.ID)
	return d, nil
}

// GetDictionaryByName returns a dictionary by name within a fractal or prism scope.
// Pass fractalID or prismID (not both). The matching scope column is used for the lookup.
func (m *Manager) GetDictionaryByName(ctx context.Context, fractalID, prismID, name string) (*Dictionary, error) {
	d := &Dictionary{}
	var colsJSON []byte
	var q string
	var arg string
	if prismID != "" {
		q = `SELECT id, name, description, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), is_global, key_column, columns, row_count, COALESCE(created_by, ''), created_at, updated_at
		     FROM dictionaries WHERE prism_id = $1 AND name = $2`
		arg = prismID
	} else {
		q = `SELECT id, name, description, COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''), is_global, key_column, columns, row_count, COALESCE(created_by, ''), created_at, updated_at
		     FROM dictionaries WHERE fractal_id = $1 AND name = $2`
		arg = fractalID
	}
	err := m.pg.QueryRow(ctx, q, arg, name).
		Scan(&d.ID, &d.Name, &d.Description, &d.FractalID, &d.PrismID, &d.IsGlobal, &d.KeyColumn,
			&colsJSON, &d.RowCount, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(colsJSON, &d.Columns); err != nil {
		d.Columns = []DictionaryColumn{}
	}
	d.CHTableName = chTableName(d.ID)
	d.CHDictName = chDictName(d.ID)
	return d, nil
}

// ---- Create / Update / Delete ----

// CreateDictionary creates a new dictionary scoped to a fractal or prism (pass one, leave other empty).
// If columns is empty, the dictionary starts with no columns and no CH objects; the first column
// added via AddColumn becomes the key and triggers CH table/dictionary creation.
func (m *Manager) CreateDictionary(ctx context.Context, fractalID, prismID, name, description, keyColumn string, columns []DictionaryColumn, createdBy string, isGlobal bool) (*Dictionary, error) {
	// When columns are provided (e.g. from ExecuteDictionaryAction), ensure the key column is set.
	if len(columns) > 0 {
		if keyColumn == "" {
			keyColumn = columns[0].Name
		}
		hasKey := false
		for _, c := range columns {
			if c.Name == keyColumn {
				hasKey = true
				break
			}
		}
		if !hasKey {
			columns = append([]DictionaryColumn{{Name: keyColumn, Type: "string"}}, columns...)
		}
	}

	colsJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal columns: %w", err)
	}

	var fractalIDPtr, prismIDPtr interface{}
	if prismID != "" {
		prismIDPtr = prismID
	} else {
		fractalIDPtr = fractalID
	}

	var id string
	err = m.pg.QueryRow(ctx,
		`INSERT INTO dictionaries (name, description, fractal_id, prism_id, key_column, columns, is_global, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		name, description, fractalIDPtr, prismIDPtr, keyColumn, colsJSON, isGlobal, createdBy).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("failed to insert dictionary: %w", err)
	}

	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}

	// Only create CH objects if we have columns. Empty dictionaries get CH objects
	// created when the first column is added.
	if len(columns) > 0 {
		if err := m.createCHObjects(ctx, dict); err != nil {
			_, _ = m.pg.Exec(ctx, `DELETE FROM dictionaries WHERE id = $1`, id)
			return nil, fmt.Errorf("failed to create ClickHouse objects: %w", err)
		}
	}

	return dict, nil
}

// UpdateDictionary updates dictionary metadata (name, description, is_global).
func (m *Manager) UpdateDictionary(ctx context.Context, id, name, description string, isGlobal bool) (*Dictionary, error) {
	_, err := m.pg.Exec(ctx,
		`UPDATE dictionaries SET name = $1, description = $2, is_global = $3 WHERE id = $4`,
		name, description, isGlobal, id)
	if err != nil {
		return nil, err
	}
	return m.GetDictionary(ctx, id)
}

// DeleteDictionary deletes a dictionary and drops its ClickHouse objects.
func (m *Manager) DeleteDictionary(ctx context.Context, id string) error {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return err
	}
	_ = m.dropCHObjects(ctx, dict)
	_, err = m.pg.Exec(ctx, `DELETE FROM dictionaries WHERE id = $1`, id)
	return err
}

// ---- Columns ----

// AddColumn adds a new column to a dictionary.
// If this is the first column, it becomes the key column and CH objects are created.
func (m *Manager) AddColumn(ctx context.Context, id, colName string) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, c := range dict.Columns {
		if c.Name == colName {
			return nil, fmt.Errorf("column %q already exists", colName)
		}
	}

	if !isValidIdentifier(colName) {
		return nil, fmt.Errorf("invalid column name %q: must start with a letter or underscore and contain only alphanumeric, underscore, or hyphen characters", colName)
	}

	isFirstColumn := len(dict.Columns) == 0

	dict.Columns = append(dict.Columns, DictionaryColumn{Name: colName, Type: "string"})
	colsJSON, _ := json.Marshal(dict.Columns)

	if isFirstColumn {
		// First column becomes the key. Set key_column and create CH objects from scratch.
		dict.KeyColumn = colName
		if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET key_column = $1, columns = $2 WHERE id = $3`,
			colName, colsJSON, id); err != nil {
			return nil, err
		}
		dict.CHTableName = chTableName(dict.ID)
		dict.CHDictName = chDictName(dict.ID)
		if err := m.createCHObjects(ctx, dict); err != nil {
			return nil, fmt.Errorf("failed to create ClickHouse objects: %w", err)
		}
	} else {
		if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET columns = $1 WHERE id = $2`, colsJSON, id); err != nil {
			return nil, err
		}

		alterSQL := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `%s` String DEFAULT ''",
			escCH(dict.CHTableName), escCH(colName))
		if err := m.ch.Exec(ctx, alterSQL); err != nil {
			return nil, fmt.Errorf("failed to alter ClickHouse table: %w", err)
		}

		if err := m.recreateAllCHDictionaries(ctx, dict, dict.Columns); err != nil {
			return nil, fmt.Errorf("failed to recreate ClickHouse dictionaries: %w", err)
		}
	}
	return m.GetDictionary(ctx, id)
}

// RemoveColumn removes a column from a dictionary.
func (m *Manager) RemoveColumn(ctx context.Context, id, colName string) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	if colName == dict.KeyColumn {
		return nil, fmt.Errorf("cannot remove the key column")
	}
	if !isValidIdentifier(colName) {
		return nil, fmt.Errorf("invalid column name %q", colName)
	}

	newCols := make([]DictionaryColumn, 0, len(dict.Columns))
	var wasKey bool
	found := false
	for _, c := range dict.Columns {
		if c.Name == colName {
			found = true
			wasKey = c.IsKey
			continue
		}
		newCols = append(newCols, c)
	}
	if !found {
		return nil, fmt.Errorf("column %q not found", colName)
	}

	// Drop secondary key dict before altering the table
	if wasKey {
		_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(chColDictName(id, colName)))))
	}

	colsJSON, _ := json.Marshal(newCols)
	if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET columns = $1 WHERE id = $2`, colsJSON, id); err != nil {
		return nil, err
	}

	dropSQL := m.ch.InjectOnCluster(fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN IF EXISTS `%s`",
		escCH(dict.CHTableName), escCH(colName)))
	if err := m.ch.Exec(ctx, dropSQL); err != nil {
		return nil, fmt.Errorf("failed to drop column: %w", err)
	}

	dict.Columns = newCols
	if err := m.recreateAllCHDictionaries(ctx, dict, newCols); err != nil {
		return nil, fmt.Errorf("failed to recreate ClickHouse dictionaries: %w", err)
	}
	return m.GetDictionary(ctx, id)
}

// SetColumnKey marks a column as a secondary lookup key and creates a dedicated
// ClickHouse DICTIONARY keyed by that column.
func (m *Manager) SetColumnKey(ctx context.Context, id, colName string) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	if colName == dict.KeyColumn {
		return dict, nil // primary key needs no extra dictionary
	}

	found := false
	for i, c := range dict.Columns {
		if c.Name == colName {
			if c.IsKey {
				return dict, nil // already set
			}
			dict.Columns[i].IsKey = true
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("column %q not found", colName)
	}

	colsJSON, _ := json.Marshal(dict.Columns)
	if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET columns = $1 WHERE id = $2`, colsJSON, id); err != nil {
		return nil, err
	}

	if err := m.createCHDictionaryForKey(ctx, dict, colName, dict.Columns); err != nil {
		return nil, fmt.Errorf("failed to create secondary key dictionary: %w", err)
	}
	return m.GetDictionary(ctx, id)
}

// UnsetColumnKey removes the secondary lookup key status from a column and drops
// the associated ClickHouse DICTIONARY.
func (m *Manager) UnsetColumnKey(ctx context.Context, id, colName string) (*Dictionary, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, err
	}
	if colName == dict.KeyColumn {
		return nil, fmt.Errorf("cannot unset the primary key column")
	}

	found := false
	for i, c := range dict.Columns {
		if c.Name == colName {
			if !c.IsKey {
				return dict, nil // already not a key
			}
			dict.Columns[i].IsKey = false
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("column %q not found", colName)
	}

	colsJSON, _ := json.Marshal(dict.Columns)
	if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET columns = $1 WHERE id = $2`, colsJSON, id); err != nil {
		return nil, err
	}

	_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(chColDictName(dict.ID, colName)))))
	return m.GetDictionary(ctx, id)
}

// ---- Data (rows) ----

// GetRows returns rows from the ClickHouse backing table with optional search.
func (m *Manager) GetRows(ctx context.Context, id, search string, limit, offset int) ([]DictionaryRow, int64, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	if len(dict.Columns) == 0 {
		return nil, 0, nil
	}

	var colRefs []string
	for _, c := range dict.Columns {
		colRefs = append(colRefs, fmt.Sprintf("`%s`", escCH(c.Name)))
	}

	where := ""
	if search != "" {
		var searchClauses []string
		for _, c := range dict.Columns {
			searchClauses = append(searchClauses, fmt.Sprintf("positionCaseInsensitive(`%s`, '%s') > 0",
				escCH(c.Name), escCHStr(search)))
		}
		where = "WHERE (" + strings.Join(searchClauses, " OR ") + ")"
	}

	countSQL := fmt.Sprintf("SELECT count() FROM `%s` %s", escCH(dict.CHTableName), where)
	countRows, err := m.ch.Query(ctx, countSQL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count rows: %w", err)
	}
	var total int64
	if len(countRows) > 0 {
		for _, v := range countRows[0] {
			switch n := v.(type) {
			case uint64:
				total = int64(n)
			case int64:
				total = n
			}
			break
		}
	}

	querySQL := fmt.Sprintf("SELECT %s FROM `%s` %s ORDER BY `%s` ASC LIMIT %d OFFSET %d",
		strings.Join(colRefs, ", "), escCH(dict.CHTableName), where, escCH(dict.KeyColumn), limit, offset)

	dataRows, err := m.ch.Query(ctx, querySQL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query rows: %w", err)
	}

	var result []DictionaryRow
	for _, row := range dataRows {
		dr := DictionaryRow{Fields: make(map[string]string)}
		for k, v := range row {
			s := fmt.Sprintf("%v", v)
			dr.Fields[k] = s
			if k == dict.KeyColumn {
				dr.Key = s
			}
		}
		result = append(result, dr)
	}
	return result, total, nil
}

// UpsertRows upserts rows into the ClickHouse backing table.
func (m *Manager) UpsertRows(ctx context.Context, id string, rows []DictionaryRow) error {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return err
	}
	if len(rows) == 0 || len(dict.Columns) == 0 {
		return nil
	}

	var colNames []string
	for _, c := range dict.Columns {
		colNames = append(colNames, fmt.Sprintf("`%s`", escCH(c.Name)))
	}

	// Build parameterized INSERT with placeholders.
	placeholders := make([]string, len(dict.Columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		escCH(dict.CHTableName), strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	for _, row := range rows {
		args := make([]interface{}, len(dict.Columns))
		for i, c := range dict.Columns {
			args[i] = row.Fields[c.Name]
		}
		if err := m.ch.ExecArgs(ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("failed to upsert row: %w", err)
		}
	}

	m.updateRowCount(ctx, dict)
	return nil
}

// DeleteRow deletes a row by key from the ClickHouse backing table.
func (m *Manager) DeleteRow(ctx context.Context, id, key string) error {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return err
	}
	deleteSQL := fmt.Sprintf("ALTER TABLE `%s` DELETE WHERE `%s` = '%s'",
		escCH(dict.CHTableName), escCH(dict.KeyColumn), escCHStr(key))
	if err := m.ch.Exec(ctx, deleteSQL); err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}
	m.updateRowCount(ctx, dict)
	return nil
}

// ImportCSV parses a CSV reader and upserts all rows. First row must be headers.
// Returns the number of rows imported.
func (m *Manager) ImportCSV(ctx context.Context, id string, r io.Reader) (int, error) {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return 0, err
	}

	reader := csv.NewReader(r)
	headers, err := reader.Read()
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Auto-add any new columns found in CSV
	existingCols := make(map[string]bool)
	for _, c := range dict.Columns {
		existingCols[c.Name] = true
	}
	for _, h := range headers {
		if !existingCols[h] {
			if _, err := m.AddColumn(ctx, id, h); err != nil {
				return 0, fmt.Errorf("failed to add column %q: %w", h, err)
			}
			existingCols[h] = true
			dict, err = m.GetDictionary(ctx, id)
			if err != nil {
				return 0, err
			}
		}
	}

	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("CSV parse error at row %d: %w", count+1, err)
		}

		fields := make(map[string]string)
		for i, h := range headers {
			if i < len(record) {
				fields[h] = record[i]
			}
		}

		keyVal := fields[dict.KeyColumn]
		if keyVal == "" {
			continue
		}

		if err := m.UpsertRows(ctx, id, []DictionaryRow{{Key: keyVal, Fields: fields}}); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// ReloadDictionary forces ClickHouse to reload the dictionary from its source table.
func (m *Manager) ReloadDictionary(ctx context.Context, id string) error {
	dict, err := m.GetDictionary(ctx, id)
	if err != nil {
		return err
	}
	return m.ch.Exec(ctx, fmt.Sprintf("SYSTEM RELOAD DICTIONARY `%s`", escCH(dict.CHDictName)))
}

// ---- ClickHouse object lifecycle ----

func (m *Manager) createCHObjects(ctx context.Context, dict *Dictionary) error {
	if err := m.createCHTable(ctx, dict); err != nil {
		return err
	}
	return m.createCHDictionary(ctx, dict, dict.Columns)
}

func (m *Manager) dropCHObjects(ctx context.Context, dict *Dictionary) error {
	_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(dict.CHDictName))))
	for _, c := range dict.Columns {
		if c.IsKey {
			_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(chColDictName(dict.ID, c.Name)))))
		}
	}
	_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", escCH(dict.CHTableName))))
	return nil
}

func (m *Manager) createCHTable(ctx context.Context, dict *Dictionary) error {
	var colDefs []string
	for _, c := range dict.Columns {
		colDefs = append(colDefs, fmt.Sprintf("`%s` String DEFAULT ''", escCH(c.Name)))
	}
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (%s) ENGINE = ReplacingMergeTree() ORDER BY (`%s`)",
		escCH(dict.CHTableName), strings.Join(colDefs, ", "), escCH(dict.KeyColumn))
	createSQL = m.ch.RewriteEngine(m.ch.InjectOnCluster(createSQL))
	return m.ch.Exec(ctx, createSQL)
}

func (m *Manager) createCHDictionary(ctx context.Context, dict *Dictionary, cols []DictionaryColumn) error {
	var attrDefs []string
	for _, c := range cols {
		if c.Name == dict.KeyColumn {
			continue
		}
		attrDefs = append(attrDefs, fmt.Sprintf("    `%s` String DEFAULT ''", escCH(c.Name)))
	}

	attrsStr := ""
	if len(attrDefs) > 0 {
		attrsStr = ",\n" + strings.Join(attrDefs, ",\n")
	}

	createSQL := fmt.Sprintf(
		"CREATE OR REPLACE DICTIONARY `%s` (\n    `%s` String%s\n)\nPRIMARY KEY `%s`\nSOURCE(CLICKHOUSE(TABLE '%s' DB '%s' USER '%s' PASSWORD '%s'))\nLIFETIME(MIN 0 MAX 300)\nLAYOUT(HASHED())",
		escCH(dict.CHDictName),
		escCH(dict.KeyColumn),
		attrsStr,
		escCH(dict.KeyColumn),
		escCH(dict.CHTableName),
		m.chDB,
		escCHStr(m.ch.User),
		escCHStr(m.ch.Password),
	)
	return m.ch.Exec(ctx, m.ch.InjectOnCluster(createSQL))
}

// recreateAllCHDictionaries drops and recreates the primary dictionary and any secondary
// key column dictionaries after a schema change.
func (m *Manager) recreateAllCHDictionaries(ctx context.Context, dict *Dictionary, cols []DictionaryColumn) error {
	_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(dict.CHDictName))))
	if err := m.createCHDictionary(ctx, dict, cols); err != nil {
		return err
	}
	for _, c := range cols {
		if c.IsKey {
			_ = m.ch.Exec(ctx, m.ch.InjectOnCluster(fmt.Sprintf("DROP DICTIONARY IF EXISTS `%s`", escCH(chColDictName(dict.ID, c.Name)))))
			if err := m.createCHDictionaryForKey(ctx, dict, c.Name, cols); err != nil {
				return fmt.Errorf("failed to recreate secondary key dictionary for %q: %w", c.Name, err)
			}
		}
	}
	return nil
}

// createCHDictionaryForKey creates a ClickHouse DICTIONARY using colName as the PRIMARY KEY,
// enabling match(column=colName, ...) lookups against this dictionary.
func (m *Manager) createCHDictionaryForKey(ctx context.Context, dict *Dictionary, colName string, cols []DictionaryColumn) error {
	var attrDefs []string
	for _, c := range cols {
		if c.Name == colName {
			continue
		}
		attrDefs = append(attrDefs, fmt.Sprintf("    `%s` String DEFAULT ''", escCH(c.Name)))
	}

	attrsStr := ""
	if len(attrDefs) > 0 {
		attrsStr = ",\n" + strings.Join(attrDefs, ",\n")
	}

	dictName := chColDictName(dict.ID, colName)
	createSQL := fmt.Sprintf(
		"CREATE OR REPLACE DICTIONARY `%s` (\n    `%s` String%s\n)\nPRIMARY KEY `%s`\nSOURCE(CLICKHOUSE(TABLE '%s' DB '%s' USER '%s' PASSWORD '%s'))\nLIFETIME(MIN 0 MAX 300)\nLAYOUT(HASHED())",
		escCH(dictName),
		escCH(colName),
		attrsStr,
		escCH(colName),
		escCH(dict.CHTableName),
		m.chDB,
		escCHStr(m.ch.User),
		escCHStr(m.ch.Password),
	)
	return m.ch.Exec(ctx, m.ch.InjectOnCluster(createSQL))
}

func (m *Manager) updateRowCount(ctx context.Context, dict *Dictionary) {
	countSQL := fmt.Sprintf("SELECT count() FROM `%s`", escCH(dict.CHTableName))
	rows, err := m.ch.Query(ctx, countSQL)
	if err != nil || len(rows) == 0 {
		return
	}
	for _, v := range rows[0] {
		var count int64
		switch n := v.(type) {
		case uint64:
			count = int64(n)
		case int64:
			count = n
		}
		_, _ = m.pg.Exec(ctx, `UPDATE dictionaries SET row_count = $1 WHERE id = $2`, count, dict.ID)
		return
	}
}

// ListDictionaryMappings returns a map of dict name -> (key col -> CH dict name) for a fractal or prism.
// The primary key column always maps to the main dictionary (lookup_<id>).
// Columns with IsKey=true map to their secondary dictionaries (lookup_<id>_by_<col>).
// Used by the query translator for match() resolution.
func (m *Manager) ListDictionaryMappings(ctx context.Context, fractalID, prismID string) (map[string]map[string]string, error) {
	var q string
	var arg string
	if prismID != "" {
		q = `SELECT id, name, key_column, columns FROM dictionaries WHERE prism_id = $1 OR is_global = true`
		arg = prismID
	} else {
		q = `SELECT id, name, key_column, columns FROM dictionaries WHERE fractal_id = $1 OR is_global = true`
		arg = fractalID
	}
	rows, err := m.pg.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mappings := make(map[string]map[string]string)
	for rows.Next() {
		var id, name, keyCol string
		var colsJSON []byte
		if err := rows.Scan(&id, &name, &keyCol, &colsJSON); err != nil {
			return nil, err
		}

		inner := make(map[string]string)
		inner[keyCol] = chDictName(id)

		var cols []DictionaryColumn
		if err := json.Unmarshal(colsJSON, &cols); err == nil {
			for _, c := range cols {
				if c.IsKey && c.Name != keyCol {
					inner[c.Name] = chColDictName(id, c.Name)
				}
			}
		}
		mappings[name] = inner
	}
	return mappings, rows.Err()
}

// ---- Dictionary Actions ----

// ListDictionaryActions returns dictionary actions scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) ListDictionaryActions(ctx context.Context, fractalID, prismID string) ([]*DictionaryAction, error) {
	base := `SELECT da.id, da.name, da.description, da.dictionary_name,
	               da.max_logs_per_trigger, da.enabled, COALESCE(da.created_by, ''), da.created_at, da.updated_at,
	               COALESCE(da.fractal_id::text, ''), COALESCE(da.prism_id::text, '')
	         FROM dictionary_actions da`
	var args []interface{}
	var where string
	if prismID != "" {
		where = " WHERE da.prism_id = $1"
		args = append(args, prismID)
	} else if fractalID != "" {
		where = " WHERE da.fractal_id = $1"
		args = append(args, fractalID)
	}
	rows, err := m.pg.Query(ctx, base+where+" ORDER BY da.name ASC", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDictionaryActions(rows)
}

// GetDictionaryAction returns a single dictionary action by ID.
func (m *Manager) GetDictionaryAction(ctx context.Context, id string) (*DictionaryAction, error) {
	a := &DictionaryAction{}
	err := m.pg.QueryRow(ctx,
		`SELECT da.id, da.name, da.description, da.dictionary_name,
		        da.max_logs_per_trigger, da.enabled, COALESCE(da.created_by, ''), da.created_at, da.updated_at,
		        COALESCE(da.fractal_id::text, ''), COALESCE(da.prism_id::text, '')
		 FROM dictionary_actions da
		 WHERE da.id = $1`, id).
		Scan(&a.ID, &a.Name, &a.Description, &a.DictionaryName,
			&a.MaxLogsPerTrigger, &a.Enabled,
			&a.CreatedBy, &a.CreatedAt, &a.UpdatedAt,
			&a.FractalID, &a.PrismID)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// CreateDictionaryAction creates a new dictionary action scoped to the given fractal or prism.
// Pass exactly one of fractalID or prismID.
func (m *Manager) CreateDictionaryAction(ctx context.Context, name, description, dictName string, maxLogs int, enabled bool, createdBy, fractalID, prismID string) (*DictionaryAction, error) {
	if (fractalID == "") == (prismID == "") {
		return nil, fmt.Errorf("exactly one of fractal_id or prism_id must be set")
	}
	if maxLogs <= 0 {
		maxLogs = 1000
	}
	var fPtr, pPtr interface{}
	if fractalID != "" {
		fPtr = fractalID
	}
	if prismID != "" {
		pPtr = prismID
	}
	var id string
	err := m.pg.QueryRow(ctx,
		`INSERT INTO dictionary_actions (name, description, dictionary_name, max_logs_per_trigger, enabled, created_by, fractal_id, prism_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		name, description, dictName, maxLogs, enabled, createdBy, fPtr, pPtr).Scan(&id)
	if err != nil {
		return nil, err
	}
	return m.GetDictionaryAction(ctx, id)
}

// UpdateDictionaryAction updates an existing dictionary action.
func (m *Manager) UpdateDictionaryAction(ctx context.Context, id, name, description, dictName string, maxLogs int, enabled bool) (*DictionaryAction, error) {
	if maxLogs <= 0 {
		maxLogs = 1000
	}
	_, err := m.pg.Exec(ctx,
		`UPDATE dictionary_actions SET name=$1, description=$2, dictionary_name=$3,
		 max_logs_per_trigger=$4, enabled=$5 WHERE id=$6`,
		name, description, dictName, maxLogs, enabled, id)
	if err != nil {
		return nil, err
	}
	return m.GetDictionaryAction(ctx, id)
}

// DeleteDictionaryAction deletes a dictionary action.
func (m *Manager) DeleteDictionaryAction(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM dictionary_actions WHERE id = $1`, id)
	return err
}

// GetDictionaryActionsByAlertID returns all dictionary actions linked to an alert.
func (m *Manager) GetDictionaryActionsByAlertID(ctx context.Context, alertID string) ([]*DictionaryAction, error) {
	rows, err := m.pg.Query(ctx,
		`SELECT da.id, da.name, da.description, da.dictionary_name,
		        da.max_logs_per_trigger, da.enabled, COALESCE(da.created_by, ''), da.created_at, da.updated_at,
		        COALESCE(da.fractal_id::text, ''), COALESCE(da.prism_id::text, '')
		 FROM dictionary_actions da
		 JOIN alert_dictionary_actions ada ON ada.dictionary_action_id = da.id
		 WHERE ada.alert_id = $1`, alertID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDictionaryActions(rows)
}

// ExecuteDictionaryAction replaces the target dictionary contents with data from log results.
// The dictionary is created automatically if it doesn't exist. All log fields become columns;
// the first field is used as the key. Existing data is completely overwritten.
// Pass fractalID or prismID (not both) to scope the dictionary.
func (m *Manager) ExecuteDictionaryAction(ctx context.Context, action *DictionaryAction, fractalID, prismID string, logs []map[string]interface{}) (int, error) {
	if action.DictionaryName == "" {
		return 0, fmt.Errorf("dictionary action %q has no target dictionary name", action.Name)
	}
	if fractalID == "" && prismID == "" {
		return 0, fmt.Errorf("fractal_id or prism_id is required to execute dictionary action")
	}

	limit := action.MaxLogsPerTrigger
	if limit <= 0 {
		limit = 1000
	}
	if len(logs) > limit {
		logs = logs[:limit]
	}
	if len(logs) == 0 {
		return 0, nil
	}

	// Collect all field names from logs and determine stable column order.
	// The first field of the first log becomes the key column.
	colOrder := collectLogColumns(logs)
	if len(colOrder) == 0 {
		return 0, nil
	}
	keyCol := colOrder[0]

	// Look up or create the target dictionary.
	dict, err := m.GetDictionaryByName(ctx, fractalID, prismID, action.DictionaryName)
	if err != nil {
		// Dictionary doesn't exist - create it.
		var cols []DictionaryColumn
		for _, c := range colOrder {
			cols = append(cols, DictionaryColumn{Name: c, Type: "string"})
		}
		creator := action.CreatedBy
		if creator == "" {
			creator = "admin"
		}
		dict, err = m.CreateDictionary(ctx, fractalID, prismID, action.DictionaryName, "", keyCol, cols, creator, false)
		if err != nil {
			return 0, fmt.Errorf("failed to create dictionary %q: %w", action.DictionaryName, err)
		}
	} else {
		// Dictionary exists - rebuild schema to match current log fields.
		if err := m.rebuildDictionarySchema(ctx, dict, keyCol, colOrder); err != nil {
			return 0, fmt.Errorf("failed to rebuild dictionary schema: %w", err)
		}
		// Refresh after schema change.
		dict, err = m.GetDictionary(ctx, dict.ID)
		if err != nil {
			return 0, err
		}
	}

	// Truncate existing data.
	truncSQL := m.ch.InjectOnCluster(fmt.Sprintf("TRUNCATE TABLE `%s`", escCH(dict.CHTableName)))
	if err := m.ch.Exec(ctx, truncSQL); err != nil {
		return 0, fmt.Errorf("failed to truncate dictionary table: %w", err)
	}

	// Build rows from logs.
	var upsertRows []DictionaryRow
	for _, logEntry := range logs {
		keyVal := getLogField(logEntry, keyCol)
		if keyVal == "" {
			continue
		}
		fields := make(map[string]string)
		for _, col := range colOrder {
			fields[col] = getLogField(logEntry, col)
		}
		upsertRows = append(upsertRows, DictionaryRow{Key: keyVal, Fields: fields})
	}

	if len(upsertRows) == 0 {
		return 0, nil
	}

	if err := m.UpsertRows(ctx, dict.ID, upsertRows); err != nil {
		return 0, err
	}
	return len(upsertRows), nil
}

// rebuildDictionarySchema drops and recreates the CH table and dictionaries to match new columns.
func (m *Manager) rebuildDictionarySchema(ctx context.Context, dict *Dictionary, keyCol string, colOrder []string) error {
	// Drop all existing CH objects.
	_ = m.dropCHObjects(ctx, dict)

	// Update PG schema.
	var newCols []DictionaryColumn
	for _, c := range colOrder {
		newCols = append(newCols, DictionaryColumn{Name: c, Type: "string"})
	}
	colsJSON, _ := json.Marshal(newCols)
	_, err := m.pg.Exec(ctx,
		`UPDATE dictionaries SET key_column = $1, columns = $2 WHERE id = $3`,
		keyCol, colsJSON, dict.ID)
	if err != nil {
		return err
	}

	// Update dict in memory for CH object creation.
	dict.KeyColumn = keyCol
	dict.Columns = newCols
	dict.CHTableName = chTableName(dict.ID)
	dict.CHDictName = chDictName(dict.ID)

	return m.createCHObjects(ctx, dict)
}

// collectLogColumns returns a stable ordered list of all field names across logs.
// It preserves insertion order from the first log, then appends any extra fields from later logs.
func collectLogColumns(logs []map[string]interface{}) []string {
	seen := make(map[string]bool)
	var order []string

	for _, logEntry := range logs {
		// Check top-level keys (skip "fields" sub-map key itself).
		for k := range logEntry {
			if k == "fields" {
				continue
			}
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
		}
		// Check nested "fields" sub-map.
		if fields, ok := logEntry["fields"].(map[string]interface{}); ok {
			for k := range fields {
				if !seen[k] {
					seen[k] = true
					order = append(order, k)
				}
			}
		}
	}
	return order
}

// scanDictionaryActions scans sql.Rows into a slice of DictionaryAction.
// Expects SELECT columns in this order: id, name, description, dictionary_name,
// max_logs_per_trigger, enabled, created_by, created_at, updated_at, fractal_id, prism_id.
func scanDictionaryActions(rows *sql.Rows) ([]*DictionaryAction, error) {
	var actions []*DictionaryAction
	for rows.Next() {
		a := &DictionaryAction{}
		if err := rows.Scan(&a.ID, &a.Name, &a.Description, &a.DictionaryName,
			&a.MaxLogsPerTrigger, &a.Enabled,
			&a.CreatedBy, &a.CreatedAt, &a.UpdatedAt,
			&a.FractalID, &a.PrismID); err != nil {
			return nil, err
		}
		actions = append(actions, a)
	}
	return actions, rows.Err()
}

// getLogField extracts a string value from a log map, checking top-level and fields sub-map.
func getLogField(log map[string]interface{}, field string) string {
	if v, ok := log[field]; ok {
		return fmt.Sprintf("%v", v)
	}
	if fields, ok := log["fields"].(map[string]interface{}); ok {
		if v, ok := fields[field]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// escCH sanitizes an identifier for use inside ClickHouse backtick-quoted names.
func escCH(s string) string {
	return strings.ReplaceAll(s, "`", "")
}

// escCHStr escapes a value for use inside single-quoted ClickHouse strings.
var validIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_\-]*$`)

func isValidIdentifier(s string) bool {
	return len(s) > 0 && len(s) <= 255 && validIdentifierRe.MatchString(s)
}

func escCHStr(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}
