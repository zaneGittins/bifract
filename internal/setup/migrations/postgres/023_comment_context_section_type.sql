-- Allow comment_context section type in notebook_sections
ALTER TABLE notebook_sections DROP CONSTRAINT IF EXISTS notebook_sections_section_type_check;
ALTER TABLE notebook_sections ADD CONSTRAINT notebook_sections_section_type_check
    CHECK (section_type IN ('markdown', 'query', 'ai_summary', 'comment_context'));
