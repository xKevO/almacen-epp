-- 002_unique_items_name.sql
PRAGMA foreign_keys = ON;

-- Permite usar ON CONFLICT(name) en items
CREATE UNIQUE INDEX IF NOT EXISTS ux_items_name ON items(name);
