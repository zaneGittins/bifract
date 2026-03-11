-- Allow ai_summary section type in notebook_sections
ALTER TABLE notebook_sections DROP CONSTRAINT IF EXISTS notebook_sections_section_type_check;
ALTER TABLE notebook_sections ADD CONSTRAINT notebook_sections_section_type_check
    CHECK (section_type IN ('markdown', 'query', 'ai_summary'));

-- Enforce at most one ai_summary section per notebook
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_ai_summary_per_notebook
    ON notebook_sections (notebook_id) WHERE section_type = 'ai_summary';
