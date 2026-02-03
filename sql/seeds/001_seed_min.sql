-- 001_seed_min.sql
-- Seed mínimo: proyectos y ubicaciones base
PRAGMA foreign_keys = ON;

-- =========================
-- Proyectos
-- =========================
INSERT INTO projects (code, name, is_active)
SELECT 'OBRAS', 'Obras civiles', 1
WHERE NOT EXISTS (SELECT 1 FROM projects WHERE code = 'OBRAS');

INSERT INTO projects (code, name, is_active)
SELECT 'RELAV', 'Relavera', 1
WHERE NOT EXISTS (SELECT 1 FROM projects WHERE code = 'RELAV');

-- =========================
-- Ubicaciones / Zonas
-- =========================
-- Zona de Obras Civiles
INSERT INTO locations (project_id, code, name, is_segregation, is_active)
SELECT
  (SELECT project_id FROM projects WHERE code='OBRAS'),
  'Z-OBRAS',
  'Zona Obras civiles',
  0,
  1
WHERE NOT EXISTS (SELECT 1 FROM locations WHERE code='Z-OBRAS');

-- Zona de Relavera
INSERT INTO locations (project_id, code, name, is_segregation, is_active)
SELECT
  (SELECT project_id FROM projects WHERE code='RELAV'),
  'Z-RELAV',
  'Zona Relavera',
  0,
  1
WHERE NOT EXISTS (SELECT 1 FROM locations WHERE code='Z-RELAV');

-- Zona de Segregación (común, sin proyecto)
INSERT INTO locations (project_id, code, name, is_segregation, is_active)
SELECT
  NULL,
  'SEGR',
  'Segregación',
  1,
  1
WHERE NOT EXISTS (SELECT 1 FROM locations WHERE code='SEGR');
