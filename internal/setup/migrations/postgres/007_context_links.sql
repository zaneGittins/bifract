-- Context Links table for external lookup integrations
CREATE TABLE IF NOT EXISTS context_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    short_name VARCHAR(255) NOT NULL UNIQUE,
    match_fields TEXT[] NOT NULL DEFAULT '{}',
    validation_regex VARCHAR(500) DEFAULT '',
    context_link TEXT NOT NULL,
    redirect_warning BOOLEAN DEFAULT true,
    enabled BOOLEAN DEFAULT true,
    is_default BOOLEAN DEFAULT false,
    created_by VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_context_links_enabled ON context_links(enabled) WHERE enabled = true;
CREATE INDEX IF NOT EXISTS idx_context_links_short_name ON context_links(short_name);

DROP TRIGGER IF EXISTS update_context_links_updated_at ON context_links;
CREATE TRIGGER update_context_links_updated_at BEFORE UPDATE ON context_links
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

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
