PRAGMA foreign_keys = ON;

INSERT INTO projects (code, name) VALUES
('OBRAS', 'Obras Civiles'),
('RELAV', 'Relavera')
ON CONFLICT(code) DO NOTHING;

INSERT INTO warehouses (name) VALUES
('ALMACEN PRINCIPAL')
ON CONFLICT DO NOTHING;

-- Zonas por proyecto (mismo almacén físico, separado por áreas)
INSERT INTO locations (warehouse_id, project_id, code, name, is_segregation)
SELECT w.warehouse_id, p.project_id, 'Z-OBRAS', 'Zona Obras Civiles', 0
FROM warehouses w, projects p
WHERE w.name='ALMACEN PRINCIPAL' AND p.code='OBRAS'
AND NOT EXISTS (SELECT 1 FROM locations WHERE code='Z-OBRAS');

INSERT INTO locations (warehouse_id, project_id, code, name, is_segregation)
SELECT w.warehouse_id, p.project_id, 'Z-RELAV', 'Zona Relavera', 0
FROM warehouses w, projects p
WHERE w.name='ALMACEN PRINCIPAL' AND p.code='RELAV'
AND NOT EXISTS (SELECT 1 FROM locations WHERE code='Z-RELAV');

-- Segregación (zona general sin proyecto)
INSERT INTO locations (warehouse_id, project_id, code, name, is_segregation)
SELECT w.warehouse_id, NULL, 'SEGR', 'Segregación', 1
FROM warehouses w
WHERE w.name='ALMACEN PRINCIPAL'
AND NOT EXISTS (SELECT 1 FROM locations WHERE code='SEGR');
