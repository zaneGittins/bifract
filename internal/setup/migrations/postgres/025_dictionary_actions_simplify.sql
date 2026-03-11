-- Simplify dictionary_actions: replace dictionary_id/key_field/field_mappings
-- with dictionary_name (target dictionary auto-created at execution time).
ALTER TABLE dictionary_actions ADD COLUMN IF NOT EXISTS dictionary_name VARCHAR(255) NOT NULL DEFAULT '';

-- Backfill dictionary_name from the linked dictionaries table.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dictionary_actions' AND column_name='dictionary_id') THEN
    UPDATE dictionary_actions SET dictionary_name = d.name
    FROM dictionaries d
    WHERE dictionary_actions.dictionary_id = d.id
      AND dictionary_actions.dictionary_name = '';
  END IF;
END $$;

-- Drop legacy columns.
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS dictionary_id;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS key_field;
ALTER TABLE dictionary_actions DROP COLUMN IF EXISTS field_mappings;
