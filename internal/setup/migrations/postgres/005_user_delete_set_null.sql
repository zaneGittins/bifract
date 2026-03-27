-- Fix FK constraints on shared resources: use SET NULL instead of CASCADE
-- when a user is deleted, so shared resources (fractals, alerts, notebooks, etc.)
-- are preserved rather than destroyed.

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

-- archives.created_by (was missing ON DELETE clause entirely)
ALTER TABLE archives DROP CONSTRAINT IF EXISTS archives_created_by_fkey;
ALTER TABLE archives ALTER COLUMN created_by DROP NOT NULL;
ALTER TABLE archives ADD CONSTRAINT archives_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

-- alert_feeds.created_by (ensure constraint is correct)
ALTER TABLE alert_feeds DROP CONSTRAINT IF EXISTS alert_feeds_created_by_fkey;
ALTER TABLE alert_feeds ALTER COLUMN created_by DROP NOT NULL;
ALTER TABLE alert_feeds ADD CONSTRAINT alert_feeds_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;

-- prisms.created_by (ensure constraint is correct for pre-existing installs)
ALTER TABLE prisms DROP CONSTRAINT IF EXISTS prisms_created_by_fkey;
ALTER TABLE prisms ALTER COLUMN created_by DROP NOT NULL;
ALTER TABLE prisms ADD CONSTRAINT prisms_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES users(username) ON DELETE SET NULL;
