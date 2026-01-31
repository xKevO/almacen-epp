import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from app.db.connection import get_engine


SHEET = "PERSONAL ACTIVO"


def clean_digits(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\D+", "", s)
    return s


def main():
    excel_path = Path("data/raw/personal.xlsx")  # fijo para tu V1
    if len(sys.argv) >= 2:
        excel_path = Path(sys.argv[1]).expanduser()

    if not excel_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {excel_path}")

    # Leemos SIN headers porque tus headers están en la fila 5 (índice 4)
    raw = pd.read_excel(excel_path, sheet_name=SHEET, header=None, dtype=str)

    # Fila 4 (índice 4) = nombres de columnas reales
    header = raw.iloc[4].tolist()

    # Datos empiezan en la fila 6 (índice 5)
    data = raw.iloc[5:].copy()
    data.columns = header
    data = data.dropna(how="all")

    # Renombrar columnas NaN (en tu excel hay 2 columnas sin nombre)
    fixed_cols = []
    nan_count = 0
    for c in data.columns:
        if c is None or (isinstance(c, float) and pd.isna(c)) or str(c).strip() == "":
            nan_count += 1
            fixed_cols.append("TIEMPO_MESES" if nan_count == 1 else "TIEMPO_DIAS")
        else:
            fixed_cols.append(str(c).strip())
    data.columns = fixed_cols

    # Columnas clave según tu plantilla
    col_dni = "NRO DOCUMENTO"
    col_fot = "CODIGO"
    col_full = "APELLIDOS Y NOMBRES COMPLETOS\n(NO TOCAR ESTA FILA)"
    col_tel = "CELULAR"
    col_dir = "DIRECCION"

    # Filtrar filas que NO son personas (STAFF, CONDUCTORES, etc.)
    data[col_dni] = data[col_dni].apply(clean_digits)
    data = data[(data[col_dni].notna()) & (data[col_dni] != "")]

    # (opcional) filtrar solo DNI 8 dígitos
    data = data[data[col_dni].str.len().isin([8])]

    # Construir dataset de importación
    out = pd.DataFrame()
    out["dni"] = data[col_dni].str.strip()
    out["fotocheck_code"] = data[col_fot].astype(str).str.strip() if col_fot in data.columns else None
    out["full_name"] = data[col_full].astype(str).str.strip()
    out["phone"] = data[col_tel].astype(str).str.strip() if col_tel in data.columns else None
    out["address"] = data[col_dir].astype(str).str.strip() if col_dir in data.columns else None

    # Limpieza extra
    out["fotocheck_code"] = out["fotocheck_code"].replace({"nan": None, "None": None, "": None})
    out["phone"] = out["phone"].replace({"nan": None, "None": None, "": None})
    out["address"] = out["address"].replace({"nan": None, "None": None, "": None})

    out = out.drop_duplicates(subset=["dni"])
    out = out[(out["full_name"].notna()) & (out["full_name"] != "")]

    print("📄 Archivo:", excel_path.name)
    print("🧾 Hoja:", SHEET)
    print("👥 Registros válidos detectados:", len(out))
    print("\n🔎 Preview (5 filas):")
    print(out.head(5).to_string(index=False))

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))

        for _, r in out.iterrows():
            conn.execute(
                text("""
                INSERT INTO employees (dni, fotocheck_code, full_name, phone, address, is_active)
                VALUES (:dni, :fotocheck_code, :full_name, :phone, :address, 1)
                ON CONFLICT(dni) DO UPDATE SET
                    fotocheck_code=excluded.fotocheck_code,
                    full_name=excluded.full_name,
                    phone=excluded.phone,
                    address=excluded.address,
                    is_active=1
                """),
                {
                    "dni": r["dni"],
                    "fotocheck_code": r["fotocheck_code"],
                    "full_name": r["full_name"],
                    "phone": r["phone"],
                    "address": r["address"],
                }
            )

        total = conn.execute(text("SELECT COUNT(*) FROM employees")).scalar_one()

    print(f"\n✅ Importación OK. Total en employees: {total}")


if __name__ == "__main__":
    main()
