-- Bifract PostgreSQL Initialization Script
-- Creates tables for user authentication and comments

-- Users table for authentication
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(50) PRIMARY KEY,
    password_hash VARCHAR(255),
    display_name VARCHAR(100),
    gravatar_color VARCHAR(7) NOT NULL,  -- Hex color: #3498db
    gravatar_initial VARCHAR(1) NOT NULL,  -- First letter of username
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login TIMESTAMP,
    is_admin BOOLEAN DEFAULT FALSE,
    auth_provider VARCHAR(20) NOT NULL DEFAULT 'local',
    oidc_subject VARCHAR(255),
    force_password_change BOOLEAN NOT NULL DEFAULT FALSE
);

-- Ensure OIDC columns exist (handles case where table was created by container init without them)
ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider VARCHAR(20) NOT NULL DEFAULT 'local';
ALTER TABLE users ADD COLUMN IF NOT EXISTS oidc_subject VARCHAR(255);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_oidc_subject ON users(oidc_subject) WHERE oidc_subject IS NOT NULL;

-- Comments table
CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    log_id VARCHAR(64) NOT NULL,        -- Hash(timestamp + raw_log)
    log_timestamp TIMESTAMP NOT NULL,   -- Denormalized for faster queries
    text TEXT NOT NULL,                 -- Comment content
    author VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    tags TEXT[] DEFAULT '{}',           -- Array of tags: ['important', 'todo', 'resolved']
    query TEXT DEFAULT '',             -- BQL query active when comment was created
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_comments_log_id ON comments(log_id);
CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author);
CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_comments_log_timestamp ON comments(log_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_comments_tags ON comments USING GIN(tags);  -- GIN index for array searches

-- Function to generate random pastel color for gravatar
CREATE OR REPLACE FUNCTION random_pastel_color()
RETURNS VARCHAR(7) AS $$
DECLARE
    colors TEXT[] := ARRAY[
        '#9c6ade',  -- Bifract Purple
        '#6bcf7f',  -- Bifract Green
        '#5bbce4',  -- Sky Blue
        '#e07a8b',  -- Dusty Rose
        '#d4a054',  -- Amber
        '#ca6be0',  -- Orchid
        '#5bc4b5',  -- Teal
        '#e07a4f',  -- Burnt Sienna
        '#7a9de0',  -- Periwinkle
        '#b5c44f'   -- Chartreuse
    ];
BEGIN
    RETURN colors[floor(random() * array_length(colors, 1) + 1)];
END;
$$ LANGUAGE plpgsql;

-- Trigger function to set gravatar on user creation
CREATE OR REPLACE FUNCTION set_user_gravatar()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.gravatar_color IS NULL OR NEW.gravatar_color = '' THEN
        NEW.gravatar_color := random_pastel_color();
    END IF;
    IF NEW.gravatar_initial IS NULL OR NEW.gravatar_initial = '' THEN
        NEW.gravatar_initial := UPPER(SUBSTRING(NEW.username FROM 1 FOR 1));
    END IF;
    IF NEW.display_name IS NULL OR NEW.display_name = '' THEN
        NEW.display_name := NEW.username;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for user gravatar
DROP TRIGGER IF EXISTS user_gravatar_trigger ON users;
CREATE TRIGGER user_gravatar_trigger
    BEFORE INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION set_user_gravatar();

-- Defensive ADD COLUMN for upgrades where table was created by older init scripts
ALTER TABLE users ADD COLUMN IF NOT EXISTS invite_token_hash VARCHAR(255);
ALTER TABLE users ADD COLUMN IF NOT EXISTS invite_expires_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS force_password_change BOOLEAN NOT NULL DEFAULT FALSE;

-- Insert default admin user
-- Generated with: bcrypt.GenerateFromPassword([]byte("bifract"), 10)
INSERT INTO users (username, password_hash, display_name, gravatar_color, gravatar_initial, is_admin, force_password_change)
VALUES (
    'admin',
    '$2a$10$6qlugatnTUiTnVhThGK.l.g241wHWktjOAPykPJpHOh8RbxkApQvG',
    'Administrator',
    '#9c6ade',
    'A',
    TRUE,
    TRUE
)
ON CONFLICT (username) DO NOTHING;

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-update updated_at on comments
DROP TRIGGER IF EXISTS update_comments_updated_at ON comments;
CREATE TRIGGER update_comments_updated_at
    BEFORE UPDATE ON comments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Settings table
CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================
-- Alert System Tables
-- ============================

-- Alert definitions table
CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    query_string TEXT NOT NULL,
    alert_type VARCHAR(50) NOT NULL DEFAULT 'event',
    enabled BOOLEAN DEFAULT true,
    throttle_time_seconds INTEGER DEFAULT 0,
    throttle_field VARCHAR(255),
    labels TEXT[] DEFAULT '{}',
    "references" TEXT[] DEFAULT '{}',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    updated_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_triggered TIMESTAMP
);
-- Ensure columns exist (handles case where table was created by container init without them)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS alert_type VARCHAR(50) NOT NULL DEFAULT 'event';
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS updated_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS disabled_reason TEXT;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS labels TEXT[] DEFAULT '{}';
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS "references" TEXT[] DEFAULT '{}';

-- Webhook actions table
CREATE TABLE IF NOT EXISTS webhook_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    url TEXT NOT NULL,
    method VARCHAR(10) DEFAULT 'POST',
    headers JSONB DEFAULT '{}',
    auth_type VARCHAR(20) DEFAULT 'none', -- 'none', 'bearer', 'basic'
    auth_config JSONB DEFAULT '{}',
    timeout_seconds INTEGER DEFAULT 30,
    retry_count INTEGER DEFAULT 3,
    include_alert_link BOOLEAN DEFAULT true,
    enabled BOOLEAN DEFAULT true,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE webhook_actions ADD COLUMN IF NOT EXISTS include_alert_link BOOLEAN DEFAULT true;

-- Alert-to-webhook mapping
CREATE TABLE IF NOT EXISTS alert_webhook_actions (
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    webhook_id UUID NOT NULL REFERENCES webhook_actions(id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, webhook_id)
);

-- Alert execution log
CREATE TABLE IF NOT EXISTS alert_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    triggered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    log_count INTEGER NOT NULL,
    throttled BOOLEAN DEFAULT false,
    throttle_key VARCHAR(255),
    execution_time_ms INTEGER,
    webhook_results JSONB DEFAULT '[]',
    fractal_results JSONB DEFAULT '[]'
);

-- Indexes for alert system
CREATE INDEX IF NOT EXISTS idx_alerts_enabled ON alerts(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_alerts_created_by ON alerts(created_by);
CREATE INDEX IF NOT EXISTS idx_alerts_name ON alerts(name);
CREATE INDEX IF NOT EXISTS idx_alerts_labels ON alerts USING GIN(labels);
CREATE INDEX IF NOT EXISTS idx_webhook_actions_enabled ON webhook_actions(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_webhook_actions_created_by ON webhook_actions(created_by);
CREATE INDEX IF NOT EXISTS idx_webhook_actions_name ON webhook_actions(name);
CREATE INDEX IF NOT EXISTS idx_alert_executions_alert_id ON alert_executions(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_executions_triggered_at ON alert_executions(triggered_at DESC);

-- Triggers for updated_at on alert tables
DROP TRIGGER IF EXISTS update_alerts_updated_at ON alerts;
CREATE TRIGGER update_alerts_updated_at BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_webhook_actions_updated_at ON webhook_actions;
CREATE TRIGGER update_webhook_actions_updated_at BEFORE UPDATE ON webhook_actions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Fractal System Tables (v1.0.0)
-- ============================

-- Fractals table for multi-tenant isolation (must be created before fractal_actions)
CREATE TABLE IF NOT EXISTS fractals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    is_default BOOLEAN DEFAULT FALSE,
    is_system BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Cached statistics (updated by background jobs)
    log_count BIGINT DEFAULT 0,
    size_bytes BIGINT DEFAULT 0,
    earliest_log TIMESTAMP,
    latest_log TIMESTAMP
);

-- Ensure only one default fractal exists
CREATE UNIQUE INDEX IF NOT EXISTS idx_fractals_default_unique ON fractals(is_default) WHERE is_default = true;

-- Performance fractals
CREATE INDEX IF NOT EXISTS idx_fractals_name ON fractals(name);
CREATE INDEX IF NOT EXISTS idx_fractals_created_by ON fractals(created_by);
CREATE INDEX IF NOT EXISTS idx_fractals_created_at ON fractals(created_at DESC);

-- Ensure is_system column exists before seeding (handles existing installs)
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS is_system BOOLEAN NOT NULL DEFAULT FALSE;

-- Retention period in days (NULL = unlimited)
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS retention_days INTEGER DEFAULT NULL;

-- Archive scheduling
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS archive_schedule VARCHAR(20) DEFAULT 'never';
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS max_archives INTEGER DEFAULT NULL;

-- Disk quota (NULL = no limit)
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS disk_quota_bytes BIGINT DEFAULT NULL;
ALTER TABLE fractals ADD COLUMN IF NOT EXISTS disk_quota_action VARCHAR(10) DEFAULT 'reject';

-- Insert default fractal (will be used for existing data migration)
INSERT INTO fractals (name, description, is_default, created_by)
VALUES ('default', 'Default fractal for all logs', true, 'admin')
ON CONFLICT (name) DO NOTHING;

-- Insert system fractals (non-deletable)
INSERT INTO fractals (name, description, is_system, created_by) VALUES
  ('audit',  'Audit log of all queries and user activity', true, 'admin'),
  ('system', 'System-level logs', true, 'admin'),
  ('alerts', 'Alert trigger logs from all fractals', true, 'admin')
ON CONFLICT (name) DO NOTHING;

-- Add trigger for fractals updated_at
DROP TRIGGER IF EXISTS update_fractals_updated_at ON fractals;
CREATE TRIGGER update_fractals_updated_at BEFORE UPDATE ON fractals
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- RBAC: Groups & Fractal Permissions
-- ============================

-- Groups for organizing users (tenant-level)
CREATE TABLE IF NOT EXISTS groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_groups_name ON groups(name);
CREATE INDEX IF NOT EXISTS idx_groups_created_by ON groups(created_by);

DROP TRIGGER IF EXISTS update_groups_updated_at ON groups;
CREATE TRIGGER update_groups_updated_at BEFORE UPDATE ON groups
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Group membership (many-to-many)
CREATE TABLE IF NOT EXISTS group_members (
    group_id UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    added_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    added_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, username)
);

CREATE INDEX IF NOT EXISTS idx_group_members_username ON group_members(username);
CREATE INDEX IF NOT EXISTS idx_group_members_group_id ON group_members(group_id);

-- Per-fractal permissions for users and groups
-- Exactly one of username/group_id must be set (enforced by CHECK constraint)
CREATE TABLE IF NOT EXISTS fractal_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    username VARCHAR(50) REFERENCES users(username) ON DELETE CASCADE,
    group_id UUID REFERENCES groups(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('viewer', 'analyst', 'admin')),
    granted_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT exactly_one_grantee CHECK (
        (username IS NOT NULL AND group_id IS NULL) OR
        (username IS NULL AND group_id IS NOT NULL)
    ),
    UNIQUE(fractal_id, username),
    UNIQUE(fractal_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_fractal_permissions_fractal_id ON fractal_permissions(fractal_id);
CREATE INDEX IF NOT EXISTS idx_fractal_permissions_username ON fractal_permissions(username) WHERE username IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fractal_permissions_group_id ON fractal_permissions(group_id) WHERE group_id IS NOT NULL;

DROP TRIGGER IF EXISTS update_fractal_permissions_updated_at ON fractal_permissions;
CREATE TRIGGER update_fractal_permissions_updated_at BEFORE UPDATE ON fractal_permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Alert Actions Extension: Fractal Actions
-- ============================

-- Fractal actions table for "send to fractal" action type
CREATE TABLE IF NOT EXISTS fractal_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,

    -- Target fractal configuration
    target_fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,

    -- Log transformation options
    preserve_timestamp BOOLEAN DEFAULT true,
    add_alert_context BOOLEAN DEFAULT true,  -- Add alert name/context to logs
    field_mappings JSONB DEFAULT '{}',      -- Optional field transformations

    -- Filtering options
    max_logs_per_trigger INTEGER DEFAULT 1000,  -- Limit logs sent per trigger

    -- Status and metadata
    enabled BOOLEAN DEFAULT true,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Alert-to-fractal-action mapping (junction table)
CREATE TABLE IF NOT EXISTS alert_fractal_actions (
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    fractal_action_id UUID NOT NULL REFERENCES fractal_actions(id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, fractal_action_id)
);

-- Performance indexes for fractal actions
CREATE INDEX IF NOT EXISTS idx_fractal_actions_enabled ON fractal_actions(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_fractal_actions_target_fractal ON fractal_actions(target_fractal_id);
CREATE INDEX IF NOT EXISTS idx_fractal_actions_created_by ON fractal_actions(created_by);
CREATE INDEX IF NOT EXISTS idx_fractal_actions_name ON fractal_actions(name);
CREATE INDEX IF NOT EXISTS idx_alert_fractal_actions_alert ON alert_fractal_actions(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_fractal_actions_fractal ON alert_fractal_actions(fractal_action_id);

-- Updated_at trigger for fractal actions
DROP TRIGGER IF EXISTS update_fractal_actions_updated_at ON fractal_actions;
CREATE TRIGGER update_fractal_actions_updated_at BEFORE UPDATE ON fractal_actions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default settings
INSERT INTO settings (key, value) VALUES ('alert_timeout_seconds', '5')
ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('query_timeout_seconds', '60')
ON CONFLICT (key) DO NOTHING;

-- ============================
-- Dictionary System Tables
-- ============================

-- Dictionaries table (per-fractal lookup files backed by ClickHouse dictionaries)
CREATE TABLE IF NOT EXISTS dictionaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    key_column VARCHAR(255) NOT NULL DEFAULT 'key',
    columns JSONB NOT NULL DEFAULT '[]',  -- Array of {name, type} objects
    row_count BIGINT DEFAULT 0,
    is_global BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(fractal_id, name)
);

CREATE INDEX IF NOT EXISTS idx_dictionaries_fractal_id ON dictionaries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_dictionaries_name ON dictionaries(name);
CREATE INDEX IF NOT EXISTS idx_dictionaries_created_by ON dictionaries(created_by);

DROP TRIGGER IF EXISTS update_dictionaries_updated_at ON dictionaries;
CREATE TRIGGER update_dictionaries_updated_at BEFORE UPDATE ON dictionaries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Dictionary actions for alerts (populate a dictionary from alert results)
CREATE TABLE IF NOT EXISTS dictionary_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    dictionary_name VARCHAR(255) NOT NULL DEFAULT '',  -- Target dictionary name (auto-created if missing)
    max_logs_per_trigger INTEGER DEFAULT 1000,
    enabled BOOLEAN DEFAULT true,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Migrate legacy columns: copy dictionary name from dictionaries table, then drop old columns
ALTER TABLE dictionary_actions ADD COLUMN IF NOT EXISTS dictionary_name VARCHAR(255) NOT NULL DEFAULT '';
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dictionary_actions' AND column_name='dictionary_id') THEN
    UPDATE dictionary_actions SET dictionary_name = d.name FROM dictionaries d WHERE dictionary_actions.dictionary_id = d.id AND dictionary_actions.dictionary_name = '';
  END IF;
END $$;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS dictionary_id;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS key_field;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS field_mappings;

CREATE TABLE IF NOT EXISTS alert_dictionary_actions (
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    dictionary_action_id UUID NOT NULL REFERENCES dictionary_actions(id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, dictionary_action_id)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_actions_enabled ON dictionary_actions(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_alert_dictionary_actions_alert ON alert_dictionary_actions(alert_id);

DROP TRIGGER IF EXISTS update_dictionary_actions_updated_at ON dictionary_actions;
CREATE TRIGGER update_dictionary_actions_updated_at BEFORE UPDATE ON dictionary_actions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add fractal_id foreign keys to existing tables (safely handle existing data)
-- First add nullable columns
ALTER TABLE comments ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;
ALTER TABLE alert_executions ADD COLUMN IF NOT EXISTS fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE;

-- Update existing records to use the default fractal
UPDATE comments SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL;
UPDATE alerts SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL;
UPDATE alert_executions SET fractal_id = (SELECT id FROM fractals WHERE is_default = true LIMIT 1) WHERE fractal_id IS NULL;

-- Now make the columns NOT NULL (only after all existing records have values)
ALTER TABLE comments ALTER COLUMN fractal_id SET NOT NULL;
ALTER TABLE alerts ALTER COLUMN fractal_id SET NOT NULL;
ALTER TABLE alert_executions ALTER COLUMN fractal_id SET NOT NULL;

-- Add performance indexes for fractal_id foreign keys
CREATE INDEX IF NOT EXISTS idx_comments_fractal_id ON comments(fractal_id);
CREATE INDEX IF NOT EXISTS idx_alerts_fractal_id ON alerts(fractal_id);
CREATE INDEX IF NOT EXISTS idx_alert_executions_fractal_id ON alert_executions(fractal_id);

-- ============================
-- API Keys System Tables
-- ============================

-- API keys table for programmatic access
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,

    -- Key authentication (store hash, never plaintext)
    key_id VARCHAR(32) NOT NULL UNIQUE,         -- Public identifier (first 8 chars)
    key_hash VARCHAR(255) NOT NULL UNIQUE,      -- SHA-256 hash of full key

    -- Index and user relationships
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,

    -- Expiration and status
    expires_at TIMESTAMP,                       -- NULL = never expires
    is_active BOOLEAN DEFAULT true,

    -- Permissions (extensible)
    permissions JSONB DEFAULT '{"query": true, "comment": true, "alert_manage": false}',

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMP,
    usage_count INTEGER DEFAULT 0
);

-- Performance fractals for API keys
CREATE INDEX IF NOT EXISTS idx_api_keys_key_id ON api_keys(key_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_fractal_id ON api_keys(fractal_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_created_by ON api_keys(created_by);
CREATE INDEX IF NOT EXISTS idx_api_keys_expires_at ON api_keys(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active) WHERE is_active = true;

-- Auto-update trigger for API keys
DROP TRIGGER IF EXISTS update_api_keys_updated_at ON api_keys;
CREATE TRIGGER update_api_keys_updated_at BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Notebooks System Tables
-- ============================

CREATE TABLE IF NOT EXISTS notebooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    time_range_type VARCHAR(20) NOT NULL,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    max_results_per_section INTEGER DEFAULT 1000,
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    variables JSONB DEFAULT '[]',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE notebooks ADD COLUMN IF NOT EXISTS variables JSONB DEFAULT '[]';

CREATE TABLE IF NOT EXISTS notebook_sections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notebook_id UUID NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    section_type VARCHAR(20) NOT NULL CHECK (section_type IN ('markdown', 'query', 'ai_summary', 'comment_context', 'ai_attack_chain')),
    title VARCHAR(255),
    content TEXT NOT NULL,
    rendered_content TEXT,
    order_index INTEGER NOT NULL,
    last_executed_at TIMESTAMP,
    last_results JSONB,
    chart_type VARCHAR(50),
    chart_config JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Allow ai_summary and comment_context section types (for upgrades where table already exists with old constraint)
ALTER TABLE notebook_sections DROP CONSTRAINT IF EXISTS notebook_sections_section_type_check;
ALTER TABLE notebook_sections ADD CONSTRAINT notebook_sections_section_type_check
    CHECK (section_type IN ('markdown', 'query', 'ai_summary', 'comment_context', 'ai_attack_chain'));

-- Enforce at most one ai_summary section per notebook
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_ai_summary_per_notebook
    ON notebook_sections (notebook_id) WHERE section_type = 'ai_summary';

CREATE TABLE IF NOT EXISTS notebook_presence (
    notebook_id UUID NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (notebook_id, username)
);

CREATE INDEX IF NOT EXISTS idx_notebooks_fractal_id ON notebooks(fractal_id);
CREATE INDEX IF NOT EXISTS idx_notebooks_created_by ON notebooks(created_by);
CREATE INDEX IF NOT EXISTS idx_notebooks_created_at ON notebooks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notebooks_name ON notebooks(name);

CREATE INDEX IF NOT EXISTS idx_notebook_sections_notebook_id ON notebook_sections(notebook_id);
CREATE INDEX IF NOT EXISTS idx_notebook_sections_order ON notebook_sections(notebook_id, order_index);
CREATE INDEX IF NOT EXISTS idx_notebook_sections_type ON notebook_sections(section_type);
CREATE INDEX IF NOT EXISTS idx_notebook_sections_created_at ON notebook_sections(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notebook_presence_notebook_id ON notebook_presence(notebook_id);
CREATE INDEX IF NOT EXISTS idx_notebook_presence_last_seen ON notebook_presence(last_seen_at DESC);

DROP TRIGGER IF EXISTS update_notebooks_updated_at ON notebooks;
CREATE TRIGGER update_notebooks_updated_at BEFORE UPDATE ON notebooks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_notebook_sections_updated_at ON notebook_sections;
CREATE TRIGGER update_notebook_sections_updated_at BEFORE UPDATE ON notebook_sections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Dashboards System Tables
-- ============================

CREATE TABLE IF NOT EXISTS dashboards (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    time_range_type VARCHAR(20) NOT NULL DEFAULT 'last1h',
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    variables JSONB DEFAULT '[]',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS variables JSONB DEFAULT '[]';

CREATE TABLE IF NOT EXISTS dashboard_widgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    title VARCHAR(255),
    query_content TEXT NOT NULL DEFAULT '',
    chart_type VARCHAR(50) DEFAULT 'table',
    chart_config JSONB,
    pos_x INTEGER NOT NULL DEFAULT 0,
    pos_y INTEGER NOT NULL DEFAULT 0,
    width INTEGER NOT NULL DEFAULT 6,
    height INTEGER NOT NULL DEFAULT 4,
    last_executed_at TIMESTAMP,
    last_results JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dashboards_fractal_id ON dashboards(fractal_id);
CREATE INDEX IF NOT EXISTS idx_dashboards_created_by ON dashboards(created_by);
CREATE INDEX IF NOT EXISTS idx_dashboards_created_at ON dashboards(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dashboard_widgets_dashboard_id ON dashboard_widgets(dashboard_id);
CREATE INDEX IF NOT EXISTS idx_dashboard_widgets_created_at ON dashboard_widgets(created_at DESC);

DROP TRIGGER IF EXISTS update_dashboards_updated_at ON dashboards;
CREATE TRIGGER update_dashboards_updated_at BEFORE UPDATE ON dashboards
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dashboard_widgets_updated_at ON dashboard_widgets;
CREATE TRIGGER update_dashboard_widgets_updated_at BEFORE UPDATE ON dashboard_widgets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================
-- Dictionaries System Tables
-- ============================

-- Dictionaries table: per-fractal lookup tables backed by ClickHouse dictionaries
CREATE TABLE IF NOT EXISTS dictionaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    key_column VARCHAR(255) NOT NULL DEFAULT 'key',
    columns JSONB NOT NULL DEFAULT '[]',
    row_count BIGINT DEFAULT 0,
    is_global BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(fractal_id, name)
);

CREATE INDEX IF NOT EXISTS idx_dictionaries_fractal_id ON dictionaries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_dictionaries_name ON dictionaries(name);
CREATE INDEX IF NOT EXISTS idx_dictionaries_created_by ON dictionaries(created_by);
CREATE INDEX IF NOT EXISTS idx_dictionaries_created_at ON dictionaries(created_at DESC);

DROP TRIGGER IF EXISTS update_dictionaries_updated_at ON dictionaries;
CREATE TRIGGER update_dictionaries_updated_at BEFORE UPDATE ON dictionaries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Dictionary actions table for alert save to dictionary action type
CREATE TABLE IF NOT EXISTS dictionary_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    dictionary_name VARCHAR(255) NOT NULL DEFAULT '',
    max_logs_per_trigger INTEGER DEFAULT 1000,
    enabled BOOLEAN DEFAULT true,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE dictionary_actions ADD COLUMN IF NOT EXISTS dictionary_name VARCHAR(255) NOT NULL DEFAULT '';
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dictionary_actions' AND column_name='dictionary_id') THEN
    UPDATE dictionary_actions SET dictionary_name = d.name FROM dictionaries d WHERE dictionary_actions.dictionary_id = d.id AND dictionary_actions.dictionary_name = '';
  END IF;
END $$;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS dictionary_id;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS key_field;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS field_mappings;

CREATE TABLE IF NOT EXISTS alert_dictionary_actions (
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    dictionary_action_id UUID NOT NULL REFERENCES dictionary_actions(id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, dictionary_action_id)
);

CREATE INDEX IF NOT EXISTS idx_dictionary_actions_enabled ON dictionary_actions(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_dictionary_actions_created_by ON dictionary_actions(created_by);
CREATE INDEX IF NOT EXISTS idx_alert_dictionary_actions_alert ON alert_dictionary_actions(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_dictionary_actions_action ON alert_dictionary_actions(dictionary_action_id);

DROP TRIGGER IF EXISTS update_dictionary_actions_updated_at ON dictionary_actions;
CREATE TRIGGER update_dictionary_actions_updated_at BEFORE UPDATE ON dictionary_actions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Prisms System Tables
-- ============================

-- Prisms: virtual views that query across multiple fractals
CREATE TABLE IF NOT EXISTS prisms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Many-to-many: a prism queries across multiple fractals
CREATE TABLE IF NOT EXISTS prism_members (
    prism_id   UUID NOT NULL REFERENCES prisms(id) ON DELETE CASCADE,
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    added_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (prism_id, fractal_id)
);

CREATE INDEX IF NOT EXISTS idx_prism_members_prism   ON prism_members(prism_id);
CREATE INDEX IF NOT EXISTS idx_prism_members_fractal ON prism_members(fractal_id);

DROP TRIGGER IF EXISTS update_prisms_updated_at ON prisms;
CREATE TRIGGER update_prisms_updated_at BEFORE UPDATE ON prisms
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Per-prism permissions for users and groups (mirrors fractal_permissions)
CREATE TABLE IF NOT EXISTS prism_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prism_id UUID NOT NULL REFERENCES prisms(id) ON DELETE CASCADE,
    username VARCHAR(50) REFERENCES users(username) ON DELETE CASCADE,
    group_id UUID REFERENCES groups(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('viewer', 'analyst', 'admin')),
    granted_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT prism_exactly_one_grantee CHECK (
        (username IS NOT NULL AND group_id IS NULL) OR
        (username IS NULL AND group_id IS NOT NULL)
    ),
    UNIQUE(prism_id, username),
    UNIQUE(prism_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_prism_permissions_prism_id ON prism_permissions(prism_id);
CREATE INDEX IF NOT EXISTS idx_prism_permissions_username ON prism_permissions(username) WHERE username IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_prism_permissions_group_id ON prism_permissions(group_id) WHERE group_id IS NOT NULL;

DROP TRIGGER IF EXISTS update_prism_permissions_updated_at ON prism_permissions;
CREATE TRIGGER update_prism_permissions_updated_at BEFORE UPDATE ON prism_permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Chat System Tables
-- ============================

CREATE TABLE IF NOT EXISTS chat_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL DEFAULT 'New conversation',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    tool_calls JSONB DEFAULT '[]',
    tool_results JSONB DEFAULT '[]',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_conversations_fractal_id ON chat_conversations(fractal_id);
CREATE INDEX IF NOT EXISTS idx_chat_conversations_created_by ON chat_conversations(created_by);
CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated_at ON chat_conversations(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_id ON chat_messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at ASC);

DROP TRIGGER IF EXISTS update_chat_conversations_updated_at ON chat_conversations;
CREATE TRIGGER update_chat_conversations_updated_at BEFORE UPDATE ON chat_conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Alert cursor for guaranteed evaluation (cursor-based alert engine)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMP NOT NULL DEFAULT NOW();

-- Window duration for compound alerts (tumbling window evaluation, in seconds)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS window_duration INTEGER DEFAULT NULL;

-- Scheduled alert fields (cron-based evaluation with configurable query window)
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS schedule_cron VARCHAR(100) DEFAULT NULL;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS query_window_seconds INTEGER DEFAULT NULL;

-- Extend scoped feature tables to support prism ownership
ALTER TABLE alerts        ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE notebooks     ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE dashboards    ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE dictionaries  ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;

-- Allow fractal_id to be null for prism-owned rows (existing rows keep their fractal_id)
ALTER TABLE alerts        ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE notebooks     ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE dashboards    ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE dictionaries  ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE saved_queries ALTER COLUMN fractal_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alerts_prism_id        ON alerts(prism_id)        WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_notebooks_prism_id     ON notebooks(prism_id)     WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dashboards_prism_id    ON dashboards(prism_id)    WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dictionaries_prism_id  ON dictionaries(prism_id)  WHERE prism_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_saved_queries_prism_id ON saved_queries(prism_id) WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_queries_prism_name ON saved_queries(prism_id, name) WHERE prism_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dictionaries_prism_name ON dictionaries(prism_id, name) WHERE prism_id IS NOT NULL;

-- Global dictionaries: available to all fractals/prisms
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS is_global BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS idx_dictionaries_global ON dictionaries(is_global) WHERE is_global = true;

-- Presence tracking for chat conversations
CREATE TABLE IF NOT EXISTS chat_presence (
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (conversation_id, username)
);
CREATE INDEX IF NOT EXISTS idx_chat_presence_conversation_id ON chat_presence(conversation_id);
CREATE INDEX IF NOT EXISTS idx_chat_presence_last_seen ON chat_presence(last_seen_at);

-- ============================
-- Chat Instructions (legacy, kept for migration compatibility)
-- ============================

CREATE TABLE IF NOT EXISTS chat_instructions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_instructions_fractal_id ON chat_instructions(fractal_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_instructions_default_unique
    ON chat_instructions(fractal_id, is_default) WHERE is_default = true;

DROP TRIGGER IF EXISTS update_chat_instructions_updated_at ON chat_instructions;
CREATE TRIGGER update_chat_instructions_updated_at BEFORE UPDATE ON chat_instructions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE chat_conversations ADD COLUMN IF NOT EXISTS instruction_id UUID REFERENCES chat_instructions(id) ON DELETE SET NULL;

-- ============================
-- Instruction Libraries
-- ============================

CREATE TABLE IF NOT EXISTS instruction_libraries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    is_default BOOLEAN NOT NULL DEFAULT false,
    fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE,
    source VARCHAR(20) NOT NULL DEFAULT 'manual',
    repo_url TEXT NOT NULL DEFAULT '',
    branch VARCHAR(255) NOT NULL DEFAULT 'main',
    path TEXT NOT NULL DEFAULT '',
    auth_token TEXT NOT NULL DEFAULT '',
    sync_schedule VARCHAR(50) NOT NULL DEFAULT 'never',
    last_synced_at TIMESTAMP,
    last_sync_status TEXT NOT NULL DEFAULT '',
    last_sync_page_count INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT il_scope CHECK (
        (fractal_id IS NOT NULL AND prism_id IS NULL) OR
        (fractal_id IS NULL AND prism_id IS NOT NULL)
    )
);

ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'manual';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS repo_url TEXT NOT NULL DEFAULT '';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS branch VARCHAR(255) NOT NULL DEFAULT 'main';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS path TEXT NOT NULL DEFAULT '';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS auth_token TEXT NOT NULL DEFAULT '';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS sync_schedule VARCHAR(50) NOT NULL DEFAULT 'never';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMP;
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS last_sync_status TEXT NOT NULL DEFAULT '';
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS last_sync_page_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE instruction_libraries ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_il_default_fractal ON instruction_libraries(fractal_id) WHERE is_default = true;
CREATE UNIQUE INDEX IF NOT EXISTS idx_il_default_prism ON instruction_libraries(prism_id) WHERE is_default = true;
CREATE INDEX IF NOT EXISTS idx_il_fractal ON instruction_libraries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_il_prism ON instruction_libraries(prism_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_il_name_scope ON instruction_libraries(name, COALESCE(fractal_id, prism_id));

DROP TRIGGER IF EXISTS update_instruction_libraries_updated_at ON instruction_libraries;
CREATE TRIGGER update_instruction_libraries_updated_at BEFORE UPDATE ON instruction_libraries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS instruction_pages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    library_id UUID NOT NULL REFERENCES instruction_libraries(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    always_include BOOLEAN NOT NULL DEFAULT false,
    sort_order INTEGER NOT NULL DEFAULT 0,
    source_path TEXT NOT NULL DEFAULT '',
    source_hash TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT ip_unique_name UNIQUE (library_id, name)
);

ALTER TABLE instruction_pages ADD COLUMN IF NOT EXISTS source_path TEXT NOT NULL DEFAULT '';
ALTER TABLE instruction_pages ADD COLUMN IF NOT EXISTS source_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_ip_library ON instruction_pages(library_id);

DROP TRIGGER IF EXISTS update_instruction_pages_updated_at ON instruction_pages;
CREATE TRIGGER update_instruction_pages_updated_at BEFORE UPDATE ON instruction_pages
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS conversation_libraries (
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    library_id UUID NOT NULL REFERENCES instruction_libraries(id) ON DELETE CASCADE,
    PRIMARY KEY (conversation_id, library_id)
);

CREATE INDEX IF NOT EXISTS idx_cl_conversation ON conversation_libraries(conversation_id);
CREATE INDEX IF NOT EXISTS idx_cl_library ON conversation_libraries(library_id);

-- Presence tracking for dashboards
CREATE TABLE IF NOT EXISTS dashboard_presence (
    dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dashboard_id, username)
);
CREATE INDEX IF NOT EXISTS idx_dashboard_presence_dashboard_id ON dashboard_presence(dashboard_id);
CREATE INDEX IF NOT EXISTS idx_dashboard_presence_last_seen ON dashboard_presence(last_seen_at);

-- ============================
-- Normalizers
-- ============================

CREATE TABLE IF NOT EXISTS normalizers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    transforms JSONB NOT NULL DEFAULT '[]',
    field_mappings JSONB NOT NULL DEFAULT '[]',
    timestamp_fields JSONB NOT NULL DEFAULT '[]',
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE normalizers ADD COLUMN IF NOT EXISTS timestamp_fields JSONB NOT NULL DEFAULT '[]';

CREATE UNIQUE INDEX IF NOT EXISTS idx_normalizers_default_unique
    ON normalizers(is_default) WHERE is_default = true;
CREATE INDEX IF NOT EXISTS idx_normalizers_name ON normalizers(name);

DROP TRIGGER IF EXISTS update_normalizers_updated_at ON normalizers;
CREATE TRIGGER update_normalizers_updated_at BEFORE UPDATE ON normalizers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Seed the Bifract Default normalizer
INSERT INTO normalizers (name, description, transforms, field_mappings, timestamp_fields, is_default, created_by)
VALUES (
    'Bifract Default',
    'Flattens nested JSON, converts to snake_case, and lowercases all field names. Includes standard field mappings for common log sources.',
    '["flatten_leaf", "snake_case", "lowercase"]',
    '[
        {"sources": ["computer","host","@host","host_name"], "target": "computer_name"},
        {"sources": ["username","userprincipalname","target_user_name"], "target": "user"},
        {"sources": ["source_ip","orig_h","client_ip"], "target": "src_ip"},
        {"sources": ["destination_ip","resp_h","server_ip"], "target": "dst_ip"},
        {"sources": ["source_port","orig_p","client_port"], "target": "src_port"},
        {"sources": ["destination_port","resp_p","server_port"], "target": "dst_port"},
        {"sources": ["command_line"], "target": "commandline"},
        {"sources": ["hashes"], "target": "hash"}
    ]',
    '[
        {"field": "system_time", "format": "2006-01-02T15:04:05.999999999Z07:00"},
        {"field": "timestamp", "format": "2006-01-02T15:04:05.999999999Z07:00"},
        {"field": "@timestamp", "format": "2006-01-02T15:04:05.999999999Z07:00"},
        {"field": "time", "format": "2006-01-02T15:04:05.999999999Z07:00"}
    ]',
    true,
    'admin'
) ON CONFLICT (name) DO NOTHING;

-- ============================
-- Ingest Token System Tables
-- ============================

CREATE TABLE IF NOT EXISTS ingest_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    token_prefix VARCHAR(16) NOT NULL UNIQUE,
    token_hash VARCHAR(255) NOT NULL UNIQUE,
    token_value TEXT NOT NULL DEFAULT '',
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    parser_type VARCHAR(20) NOT NULL DEFAULT 'json',
    normalize BOOLEAN NOT NULL DEFAULT true,
    normalizer_id UUID REFERENCES normalizers(id) ON DELETE SET NULL,
    timestamp_fields JSONB DEFAULT '[]',
    is_active BOOLEAN DEFAULT true,
    is_default BOOLEAN DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMP,
    usage_count BIGINT DEFAULT 0,
    log_count BIGINT DEFAULT 0,
    UNIQUE(fractal_id, name)
);

ALTER TABLE ingest_tokens ADD COLUMN IF NOT EXISTS token_value TEXT NOT NULL DEFAULT '';
ALTER TABLE ingest_tokens ADD COLUMN IF NOT EXISTS normalizer_id UUID REFERENCES normalizers(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_ingest_tokens_token_hash ON ingest_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_ingest_tokens_token_prefix ON ingest_tokens(token_prefix);
CREATE INDEX IF NOT EXISTS idx_ingest_tokens_fractal_id ON ingest_tokens(fractal_id);

DROP TRIGGER IF EXISTS update_ingest_tokens_updated_at ON ingest_tokens;
CREATE TRIGGER update_ingest_tokens_updated_at BEFORE UPDATE ON ingest_tokens
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add query column to comments (stores the active BQL query when comment was created)
ALTER TABLE comments ADD COLUMN IF NOT EXISTS query TEXT DEFAULT '';

-- ============================
-- Saved Queries
-- ============================
CREATE TABLE IF NOT EXISTS saved_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    query_text TEXT NOT NULL,
    tags TEXT[] DEFAULT '{}',
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(fractal_id, name)
);

CREATE INDEX IF NOT EXISTS idx_saved_queries_fractal_id ON saved_queries(fractal_id);
CREATE INDEX IF NOT EXISTS idx_saved_queries_tags ON saved_queries USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_saved_queries_created_at ON saved_queries(created_at DESC);

DROP TRIGGER IF EXISTS update_saved_queries_updated_at ON saved_queries;
CREATE TRIGGER update_saved_queries_updated_at BEFORE UPDATE ON saved_queries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Context Links System Tables
-- ============================

CREATE TABLE IF NOT EXISTS context_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    short_name VARCHAR(255) NOT NULL UNIQUE,
    match_fields TEXT[] NOT NULL DEFAULT '{}',
    validation_regex VARCHAR(500) DEFAULT '',
    context_link TEXT NOT NULL,
    redirect_warning BOOLEAN DEFAULT true,
    enabled BOOLEAN DEFAULT true,
    is_default BOOLEAN DEFAULT false,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_context_links_enabled ON context_links(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_context_links_short_name ON context_links(short_name);

DROP TRIGGER IF EXISTS update_context_links_updated_at ON context_links;
CREATE TRIGGER update_context_links_updated_at BEFORE UPDATE ON context_links
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================
-- Alert Feeds System Tables
-- ============================

CREATE TABLE IF NOT EXISTS alert_feeds (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    repo_url TEXT NOT NULL,
    branch VARCHAR(255) DEFAULT 'main',
    path VARCHAR(1024) DEFAULT '',
    auth_token TEXT DEFAULT '',
    normalizer_id UUID REFERENCES normalizers(id) ON DELETE SET NULL,
    sync_schedule VARCHAR(50) NOT NULL DEFAULT 'daily',
    min_level VARCHAR(50) DEFAULT '',
    min_status VARCHAR(50) DEFAULT '',
    enabled BOOLEAN DEFAULT true,
    fractal_id UUID REFERENCES fractals(id) ON DELETE CASCADE,
    prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE,
    last_synced_at TIMESTAMP,
    last_sync_status TEXT DEFAULT '',
    last_sync_rule_count INTEGER DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT alert_feeds_scope_check CHECK (
        (fractal_id IS NOT NULL AND prism_id IS NULL) OR
        (fractal_id IS NULL AND prism_id IS NOT NULL)
    )
);
ALTER TABLE alert_feeds ADD COLUMN IF NOT EXISTS min_level VARCHAR(50) DEFAULT '';
ALTER TABLE alert_feeds ADD COLUMN IF NOT EXISTS min_status VARCHAR(50) DEFAULT '';
ALTER TABLE alert_feeds ADD COLUMN IF NOT EXISTS prism_id UUID REFERENCES prisms(id) ON DELETE CASCADE;
ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_name_fractal_id_key;
ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_scope_check;
ALTER TABLE alert_feeds ALTER COLUMN fractal_id DROP NOT NULL;
ALTER TABLE alert_feeds ADD CONSTRAINT alert_feeds_scope_check CHECK (
    (fractal_id IS NOT NULL AND prism_id IS NULL) OR
    (fractal_id IS NULL AND prism_id IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_feeds_name_scope ON alert_feeds (name, COALESCE(fractal_id, prism_id));

CREATE INDEX IF NOT EXISTS idx_alert_feeds_fractal_id ON alert_feeds(fractal_id);
CREATE INDEX IF NOT EXISTS idx_alert_feeds_prism_id ON alert_feeds(prism_id);
CREATE INDEX IF NOT EXISTS idx_alert_feeds_enabled ON alert_feeds(enabled) WHERE enabled = true;

DROP TRIGGER IF EXISTS update_alert_feeds_updated_at ON alert_feeds;
CREATE TRIGGER update_alert_feeds_updated_at BEFORE UPDATE ON alert_feeds
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Feed columns on alerts
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_id UUID REFERENCES alert_feeds(id) ON DELETE CASCADE;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_rule_path TEXT DEFAULT '';
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS feed_rule_hash TEXT DEFAULT '';

ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_name_manual ON alerts(name) WHERE feed_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_feed_rule ON alerts(feed_id, feed_rule_path) WHERE feed_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_feed_id ON alerts(feed_id) WHERE feed_id IS NOT NULL;

-- Default context links (Timesketch-inspired)
INSERT INTO context_links (short_name, match_fields, validation_regex, context_link, redirect_warning, enabled, is_default, created_by) VALUES
    ('VirusTotal Hash Lookup', '{md5,sha1,sha256,hash,file_hash,process_hash,imphash}', '^[a-fA-F0-9]{32,64}$', 'https://www.virustotal.com/gui/search/<ATTR_VALUE>', true, true, true, 'admin'),
    ('VirusTotal URL Lookup', '{url,dest_url,source_url,target_url,uri}', '^https?://', 'https://www.virustotal.com/gui/search/<ATTR_VALUE>', true, true, true, 'admin'),
    ('urlscan.io', '{url,dest_url,source_url,target_url,uri}', '^https?://', 'https://urlscan.io/search/#<ATTR_VALUE>', true, true, true, 'admin'),
    ('URLhaus', '{url,dest_url,source_url,target_url,uri}', '^https?://', 'https://urlhaus.abuse.ch/browse.php?search=<ATTR_VALUE>', true, true, true, 'admin'),
    ('AlienVault OTX - Hash', '{md5,sha1,sha256,hash,file_hash}', '^[a-fA-F0-9]{32,64}$', 'https://otx.alienvault.com/indicator/file/<ATTR_VALUE>', true, true, true, 'admin'),
    ('AlienVault OTX - Domain', '{domain,hostname,dest_host,source_host,dns_query}', '^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 'https://otx.alienvault.com/indicator/domain/<ATTR_VALUE>', true, true, true, 'admin'),
    ('AlienVault OTX - IP', '{ip,src_ip,dst_ip,dest_ip,source_ip,remote_ip,client_ip}', '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', 'https://otx.alienvault.com/indicator/ip/<ATTR_VALUE>', true, true, true, 'admin')
ON CONFLICT (short_name) DO NOTHING;

-- ============================
-- Archives
-- ============================

CREATE TABLE IF NOT EXISTS archives (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fractal_id UUID NOT NULL REFERENCES fractals(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    storage_type VARCHAR(20) NOT NULL DEFAULT 'disk',
    storage_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    log_count BIGINT NOT NULL DEFAULT 0,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
    error_message TEXT,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    archive_type VARCHAR(20) NOT NULL DEFAULT 'adhoc',
    format_version INTEGER NOT NULL DEFAULT 0,
    archive_end_ts TIMESTAMP,
    cursor_ts TIMESTAMP,
    cursor_id TEXT,
    restore_lines_sent BIGINT NOT NULL DEFAULT 0,
    restore_error TEXT
);

ALTER TABLE archives ADD COLUMN IF NOT EXISTS storage_type VARCHAR(20) NOT NULL DEFAULT 'disk';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS storage_path TEXT NOT NULL DEFAULT '';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS log_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS time_range_start TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS time_range_end TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'in_progress';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS archive_type VARCHAR(20) NOT NULL DEFAULT 'adhoc';
ALTER TABLE archives ADD COLUMN IF NOT EXISTS format_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS archive_end_ts TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS cursor_ts TIMESTAMP;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS cursor_id TEXT;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS checksum TEXT;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS restore_lines_sent BIGINT NOT NULL DEFAULT 0;
ALTER TABLE archives ADD COLUMN IF NOT EXISTS restore_error TEXT;

CREATE INDEX IF NOT EXISTS idx_archives_fractal_id ON archives(fractal_id);
CREATE INDEX IF NOT EXISTS idx_archives_status ON archives(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_archives_active_operation ON archives(fractal_id) WHERE status IN ('in_progress', 'restoring');

-- Sessions table for shared session storage across replicas
CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    selected_fractal VARCHAR(36),
    selected_prism VARCHAR(36)
);
CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- ============================
-- Fix FK constraints: created_by/added_by/granted_by on shared resources
-- should SET NULL on user deletion, not CASCADE (which would destroy shared data).
-- ============================
DO $$ BEGIN
  -- fractals.created_by
  ALTER TABLE fractals DROP CONSTRAINT IF EXISTS fractals_created_by_fkey;
  ALTER TABLE fractals ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE fractals ADD CONSTRAINT fractals_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- alerts.created_by
  ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_created_by_fkey;
  ALTER TABLE alerts ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE alerts ADD CONSTRAINT alerts_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- webhook_actions.created_by
  ALTER TABLE webhook_actions DROP CONSTRAINT IF EXISTS webhook_actions_created_by_fkey;
  ALTER TABLE webhook_actions ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE webhook_actions ADD CONSTRAINT webhook_actions_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- groups.created_by
  ALTER TABLE groups DROP CONSTRAINT IF EXISTS groups_created_by_fkey;
  ALTER TABLE groups ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE groups ADD CONSTRAINT groups_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- group_members.added_by
  ALTER TABLE group_members DROP CONSTRAINT IF EXISTS group_members_added_by_fkey;
  ALTER TABLE group_members ALTER COLUMN added_by DROP NOT NULL;
  ALTER TABLE group_members ADD CONSTRAINT group_members_added_by_fkey
    FOREIGN KEY (added_by) REFERENCES users(username) ON DELETE SET NULL;

  -- fractal_permissions.granted_by
  ALTER TABLE fractal_permissions DROP CONSTRAINT IF EXISTS fractal_permissions_granted_by_fkey;
  ALTER TABLE fractal_permissions ALTER COLUMN granted_by DROP NOT NULL;
  ALTER TABLE fractal_permissions ADD CONSTRAINT fractal_permissions_granted_by_fkey
    FOREIGN KEY (granted_by) REFERENCES users(username) ON DELETE SET NULL;

  -- fractal_actions.created_by
  ALTER TABLE fractal_actions DROP CONSTRAINT IF EXISTS fractal_actions_created_by_fkey;
  ALTER TABLE fractal_actions ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE fractal_actions ADD CONSTRAINT fractal_actions_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- dictionaries.created_by
  ALTER TABLE dictionaries DROP CONSTRAINT IF EXISTS dictionaries_created_by_fkey;
  ALTER TABLE dictionaries ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE dictionaries ADD CONSTRAINT dictionaries_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- dictionary_actions.created_by
  ALTER TABLE dictionary_actions DROP CONSTRAINT IF EXISTS dictionary_actions_created_by_fkey;
  ALTER TABLE dictionary_actions ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE dictionary_actions ADD CONSTRAINT dictionary_actions_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- api_keys.created_by
  ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_created_by_fkey;
  ALTER TABLE api_keys ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE api_keys ADD CONSTRAINT api_keys_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- notebooks.created_by
  ALTER TABLE notebooks DROP CONSTRAINT IF EXISTS notebooks_created_by_fkey;
  ALTER TABLE notebooks ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE notebooks ADD CONSTRAINT notebooks_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- dashboards.created_by
  ALTER TABLE dashboards DROP CONSTRAINT IF EXISTS dashboards_created_by_fkey;
  ALTER TABLE dashboards ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE dashboards ADD CONSTRAINT dashboards_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- normalizers.created_by
  ALTER TABLE normalizers DROP CONSTRAINT IF EXISTS normalizers_created_by_fkey;
  ALTER TABLE normalizers ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE normalizers ADD CONSTRAINT normalizers_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- ingest_tokens.created_by
  ALTER TABLE ingest_tokens DROP CONSTRAINT IF EXISTS ingest_tokens_created_by_fkey;
  ALTER TABLE ingest_tokens ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE ingest_tokens ADD CONSTRAINT ingest_tokens_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- saved_queries.created_by
  ALTER TABLE saved_queries DROP CONSTRAINT IF EXISTS saved_queries_created_by_fkey;
  ALTER TABLE saved_queries ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE saved_queries ADD CONSTRAINT saved_queries_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- context_links.created_by
  ALTER TABLE context_links DROP CONSTRAINT IF EXISTS context_links_created_by_fkey;
  ALTER TABLE context_links ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE context_links ADD CONSTRAINT context_links_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- chat_conversations.created_by
  ALTER TABLE chat_conversations DROP CONSTRAINT IF EXISTS chat_conversations_created_by_fkey;
  ALTER TABLE chat_conversations ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE chat_conversations ADD CONSTRAINT chat_conversations_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- chat_instructions.created_by
  ALTER TABLE chat_instructions DROP CONSTRAINT IF EXISTS chat_instructions_created_by_fkey;
  ALTER TABLE chat_instructions ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE chat_instructions ADD CONSTRAINT chat_instructions_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- instruction_libraries.created_by
  ALTER TABLE instruction_libraries DROP CONSTRAINT IF EXISTS instruction_libraries_created_by_fkey;
  ALTER TABLE instruction_libraries ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE instruction_libraries ADD CONSTRAINT instruction_libraries_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- instruction_pages.created_by
  ALTER TABLE instruction_pages DROP CONSTRAINT IF EXISTS instruction_pages_created_by_fkey;
  ALTER TABLE instruction_pages ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE instruction_pages ADD CONSTRAINT instruction_pages_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- archives.created_by
  ALTER TABLE archives DROP CONSTRAINT IF EXISTS archives_created_by_fkey;
  ALTER TABLE archives ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE archives ADD CONSTRAINT archives_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- alert_feeds.created_by (already SET NULL, ensure constraint name matches)
  ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_created_by_fkey;
  ALTER TABLE alert_feeds ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE alert_feeds ADD CONSTRAINT alert_feeds_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

  -- prisms.created_by (ensure constraint is correct for pre-existing installs)
  ALTER TABLE prisms DROP CONSTRAINT IF EXISTS prisms_created_by_fkey;
  ALTER TABLE prisms ALTER COLUMN created_by DROP NOT NULL;
  ALTER TABLE prisms ADD CONSTRAINT prisms_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;
END $$;
