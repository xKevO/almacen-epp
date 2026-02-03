import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from app.db.connection import get_engine


def parse_int(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except:
        return None


def clean_str(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    return None if s == "" or s.lower() == "nan" else s


def detect_header_row(df):
    for i in range(min(len(df), 60)):
        v = str(df.iloc[i, 0]).upper() if not pd.isna(df.iloc[i, 0]) else ""
        if "DESCRIPCION" in v and "EPP" in v:
            return i
    return None


def infer_has_size(desc, talla):
    d = (desc or "").upper()
    t = (talla or "").upper()
    if t and t not in ("-", "0", "N/A"):
        return True
    # patrones típicos: T/41, T-41, TALLA 41, etc.
    return bool(re.search(r"\bT\s*/\s*\d+\b|\bTALLA\b|\bT\s*-\s*\d+\b", d))


def parse_date_from_filename(name):
    # busca dd_mm_yyyy o dd-mm-yyyy
    m = re.search(r"(\d{2})[._-](\d{2})[._-](\d{4})", name)
    if not m:
        return None
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd} 13:00:00"


def read_kardex_total(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name="KARDEX TOTAL", header=None, dtype=str)
    header_row = detect_header_row(raw)
    if header_row is None:
        raise RuntimeError(
            f"No pude detectar encabezado 'DESCRIPCION DE EPP' en {path.name}"
        )

    # encabezado + datos
    header = raw.iloc[header_row].tolist()
    data = raw.iloc[header_row + 1 :].copy()

    # usamos solo las primeras 7 columnas (evita el pivot a la derecha)
    data = data.iloc[:, :7]
    header = header[:7]
    data.columns = [
        str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(header)
    ]

    # limpiar filas vacías
    data = data.dropna(how="all")
    # normalizar nombres de columnas (por si hay espacios)
    data.columns = [re.sub(r"\s+", " ", c).strip() for c in data.columns]

    # columnas esperadas (en tus kardex)
    # DESCRIPCION DE EPP | TALLAS | CANTIDAD /REQUERIDA | STOCK | CONSUMO 25-Ene | % TOTAL DEL STOCK
    # Las tomamos por contains para no depender 100%
    def pick(col_contains):
        for c in data.columns:
            if col_contains in c.upper():
                return c
        return None

    c_desc = pick("DESCRIPCION")
    c_talla = pick("TALLA")
    c_req = pick("REQUER")
    c_stock = pick("STOCK")

    if not c_desc or not c_stock:
        raise RuntimeError(
            f"No encontré columnas claves (DESCRIPCION/STOCK) en {path.name}. Columnas: {list(data.columns)}"
        )

    out = pd.DataFrame()
    out["description"] = data[c_desc].apply(clean_str)
    out["size"] = data[c_talla].apply(clean_str) if c_talla else None
    out["required_qty"] = data[c_req].apply(parse_int) if c_req else None
    out["stock_qty"] = data[c_stock].apply(parse_int)

    # filtrar filas válidas
    out = out[out["description"].notna()]
    out = out[out["stock_qty"].notna()]  # solo lo que tenga stock numérico
    out = out[out["stock_qty"] >= 0]  # evitamos negativos raros
    return out


def upsert_items_and_seed_stock(
    kardex_path: Path, project_code: str, location_code: str
):
    engine = get_engine()

    # leer
    df = read_kardex_total(kardex_path)

    # timestamp para el movimiento inicial
    txn_datetime = parse_date_from_filename(kardex_path.name) or datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")
    reference = f"INIT_STOCK_{project_code}_{txn_datetime[:10]}"

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))

        # obtener project_id y location_id
        project_id = conn.execute(
            text("SELECT project_id FROM projects WHERE code=:c"), {"c": project_code}
        ).scalar_one()
        location_id = conn.execute(
            text("SELECT location_id FROM locations WHERE code=:c"),
            {"c": location_code},
        ).scalar_one()

        # idempotencia: si ya sembraste este INIT, no lo duplica
        existing = conn.execute(
            text(
                """
            SELECT COUNT(*) FROM transactions
            WHERE txn_type='ADJUST' AND reference=:ref AND project_id=:pid AND location_id=:lid
            """
            ),
            {"ref": reference, "pid": project_id, "lid": location_id},
        ).scalar_one()

        if existing and existing > 0:
            print(
                f"⚠️ Ya existe seed de stock para {project_code} con reference={reference}. No duplico."
            )
            return

        items_upserted = 0
        txn_inserted = 0

        for _, r in df.iterrows():
            desc = r["description"].strip()
            talla = r["size"]
            stock = int(r["stock_qty"])
            req = r["required_qty"]
            has_size = 1 if infer_has_size(desc, talla) else 0
            min_stock = int(req) if req is not None else 0

            # Upsert item por name (sku lo dejamos para después)
            conn.execute(
                text(
                    """
                INSERT INTO items (sku, name, category, unit, has_size, useful_life_days, min_stock, is_active)
                VALUES (NULL, :name, 'EPP', 'UND', :has_size, NULL, :min_stock, 1)
                ON CONFLICT(name) DO UPDATE SET
                    has_size = CASE WHEN excluded.has_size=1 THEN 1 ELSE items.has_size END,
                    min_stock = CASE WHEN excluded.min_stock > items.min_stock THEN excluded.min_stock ELSE items.min_stock END,
                    is_active = 1
                """
                ),
                {"name": desc, "has_size": has_size, "min_stock": min_stock},
            )
            items_upserted += 1

            item_id = conn.execute(
                text("SELECT item_id FROM items WHERE name=:n"), {"n": desc}
            ).scalar_one()

            # Sembrar stock como ADJUST (+)
            # Si talla viene, la guardamos en size, si no, NULL
            if stock > 0:
                conn.execute(
                    text(
                        """
                    INSERT INTO transactions (
                        txn_datetime, txn_type, project_id, location_id, item_id, qty, size,
                        employee_id, request_number, reference, notes, created_by
                    ) VALUES (
                        :dt, 'ADJUST', :pid, :lid, :iid, :qty, :size,
                        NULL, NULL, :ref, 'Seed inicial desde KARDEX TOTAL', 'system'
                    )
                    """
                    ),
                    {
                        "dt": txn_datetime,
                        "pid": project_id,
                        "lid": location_id,
                        "iid": item_id,
                        "qty": stock,
                        "size": talla,
                        "ref": reference,
                    },
                )
                txn_inserted += 1

        print(
            f"✅ {project_code}: items procesados={len(df)} | transacciones ADJUST insertadas={txn_inserted} | reference={reference}"
        )


def main():
    # uso:
    # python scripts/import_items_stock_from_kardex.py data/raw/kardex_obras.xlsm data/raw/kardex_relav.xlsm
    obras = Path("data/raw/kardex_obras.xlsm")
    relav = Path("data/raw/kardex_relav.xlsm")

    if len(sys.argv) >= 3:
        obras = Path(sys.argv[1])
        relav = Path(sys.argv[2])

    if not obras.exists():
        raise FileNotFoundError(f"No existe: {obras}")
    if not relav.exists():
        raise FileNotFoundError(f"No existe: {relav}")

    upsert_items_and_seed_stock(obras, project_code="OBRAS", location_code="Z-OBRAS")
    upsert_items_and_seed_stock(relav, project_code="RELAV", location_code="Z-RELAV")


if __name__ == "__main__":
    main()
