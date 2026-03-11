-- Add query column to comments table for storing the active Quandrix query at comment time
ALTER TABLE comments ADD COLUMN IF NOT EXISTS query TEXT DEFAULT '';
