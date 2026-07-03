-- Iceberg SQL catalog tables (apache/iceberg-go v0.6.0), backing the archive
-- sidecar (bifract-archiver). Pre-created here (migration-managed) so the
-- archiver runs with init_catalog_tables=false. Column layout matches what
-- iceberg-go's SQL catalog expects exactly. Idempotent; empty until archiving
-- is enabled, so presence does not imply the feature is active.
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name               VARCHAR NOT NULL,
    table_namespace            VARCHAR NOT NULL,
    table_name                 VARCHAR NOT NULL,
    iceberg_type               VARCHAR,
    metadata_location          VARCHAR,
    previous_metadata_location VARCHAR,
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name   VARCHAR NOT NULL,
    namespace      VARCHAR NOT NULL,
    property_key   VARCHAR NOT NULL,
    property_value VARCHAR,
    PRIMARY KEY (catalog_name, namespace, property_key)
);
