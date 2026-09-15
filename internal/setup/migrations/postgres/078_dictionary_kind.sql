-- Dictionary kind. 'value' is the existing HASHED dictionary, which probes the key
-- byte for byte. 'network' builds an IP_TRIE whose keys are CIDR ranges, so a
-- lookup resolves an address to the longest range containing it.
ALTER TABLE dictionaries ADD COLUMN IF NOT EXISTS kind VARCHAR(16) NOT NULL DEFAULT 'value';
