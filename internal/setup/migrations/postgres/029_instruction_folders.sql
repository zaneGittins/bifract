-- Single-level folders for organising instruction pages within a library.
-- Pages reference a folder via instruction_pages.folder_id (NULL = library root).
-- ON DELETE SET NULL keeps pages when a folder is deleted (they move to the root).
-- Page names stay unique per library (not per folder) so [[wikilinks]] resolve.
CREATE TABLE IF NOT EXISTS instruction_folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    library_id UUID NOT NULL REFERENCES instruction_libraries(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_if_library ON instruction_folders(library_id);

DROP TRIGGER IF EXISTS update_instruction_folders_updated_at ON instruction_folders;
CREATE TRIGGER update_instruction_folders_updated_at BEFORE UPDATE ON instruction_folders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE instruction_pages ADD COLUMN IF NOT EXISTS folder_id UUID REFERENCES instruction_folders(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_ip_folder ON instruction_pages(folder_id);
