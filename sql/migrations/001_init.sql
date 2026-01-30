-- 001_init.sql (V1)
PRAGMA foreign_keys = ON;

-- === Catálogos ===
CREATE TABLE IF NOT EXISTS projects (
  project_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  code           TEXT UNIQUE NOT NULL,
  name           TEXT NOT NULL,
  is_active      INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS warehouses (
  warehouse_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locations (
  location_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  warehouse_id   INTEGER NOT NULL,
  project_id     INTEGER, -- NULL si es zona general (ej: segregación)
  code           TEXT NOT NULL,
  name           TEXT NOT NULL,
  is_segregation INTEGER NOT NULL DEFAULT 0,
  is_active      INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
  FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

CREATE TABLE IF NOT EXISTS employees (
  employee_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  dni            TEXT UNIQUE,
  fotocheck_code TEXT UNIQUE,
  full_name      TEXT NOT NULL,
  phone          TEXT,
  address        TEXT,
  is_active      INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS items (
  item_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  sku                TEXT UNIQUE,
  name               TEXT NOT NULL,
  category           TEXT NOT NULL DEFAULT 'EPP', -- EPP / HERRAMIENTA (futuro)
  unit               TEXT NOT NULL DEFAULT 'UND',
  has_size           INTEGER NOT NULL DEFAULT 0,
  useful_life_days   INTEGER, -- vida útil estándar
  min_stock          INTEGER NOT NULL DEFAULT 0,
  is_active          INTEGER NOT NULL DEFAULT 1
);

-- === Movimientos (Kardex) ===
-- Un solo lugar de verdad: los movimientos. El stock se calcula con SUM().
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  txn_datetime     TEXT NOT NULL, -- ISO string
  txn_type         TEXT NOT NULL, -- IN, OUT, RETURN_STOCK, RETURN_SEGR, TRANSFER_OUT, TRANSFER_IN, ADJUST, BAJA
  project_id       INTEGER NOT NULL,
  location_id      INTEGER NOT NULL,
  item_id          INTEGER NOT NULL,
  qty              INTEGER NOT NULL,
  size             TEXT,          -- talla (si aplica)
  employee_id      INTEGER,       -- si está asociado a entrega/devolución
  request_number   TEXT,          -- nro solicitud a Lima (si aplica)
  reference        TEXT,          -- guía, OC, acta, etc.
  notes            TEXT,
  created_by       TEXT,          -- usuario (V2), en V1 puede ser tu nombre
  FOREIGN KEY (project_id) REFERENCES projects(project_id),
  FOREIGN KEY (location_id) REFERENCES locations(location_id),
  FOREIGN KEY (item_id) REFERENCES items(item_id),
  FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- === Entregas al personal (documento) ===
CREATE TABLE IF NOT EXISTS issue_header (
  issue_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  issue_datetime  TEXT NOT NULL,
  project_id      INTEGER NOT NULL,
  employee_id     INTEGER NOT NULL,
  request_number  TEXT,
  notes           TEXT,
  FOREIGN KEY (project_id) REFERENCES projects(project_id),
  FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS issue_detail (
  issue_detail_id INTEGER PRIMARY KEY AUTOINCREMENT,
  issue_id        INTEGER NOT NULL,
  item_id         INTEGER NOT NULL,
  qty             INTEGER NOT NULL,
  size            TEXT,
  useful_life_days INTEGER,
  next_renewal_date TEXT,
  FOREIGN KEY (issue_id) REFERENCES issue_header(issue_id),
  FOREIGN KEY (item_id) REFERENCES items(item_id)
);

-- Índices útiles (performance)
CREATE INDEX IF NOT EXISTS idx_txn_project_item_date ON transactions(project_id, item_id, txn_datetime);
CREATE INDEX IF NOT EXISTS idx_txn_employee_date ON transactions(employee_id, txn_datetime);