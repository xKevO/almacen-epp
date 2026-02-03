import io
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Kardex", layout="wide")
st.title("📒 Kardex / Historial de movimientos")


engine = get_engine()

# Estrategia definitiva de fechas/horas:
# - Guardar en BD en UTC (txn_datetime en UTC)
# - Mostrar y filtrar en hora local (America/Lima), convirtiendo a UTC al consultar
LOCAL_TZ = ZoneInfo("America/Lima")
UTC_TZ = timezone.utc


# -------------------------
# Helpers
# -------------------------
def load_catalogs(conn):
    projects = pd.read_sql(
        text(
            "SELECT project_id, code, name FROM projects WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )
    items = pd.read_sql(
        text(
            "SELECT item_id, name, has_size FROM items WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )
    employees = pd.read_sql(
        text("SELECT employee_id, full_name, dni FROM employees ORDER BY full_name"),
        conn,
    )
    locations = pd.read_sql(
        text(
            """
        SELECT l.location_id, l.code, l.name,
               COALESCE(p.code, '—') AS project_code
        FROM locations l
        LEFT JOIN projects p ON p.project_id = l.project_id
        ORDER BY l.name
        """
        ),
        conn,
    )
    return projects, items, employees, locations


def query_transactions(filters: dict) -> pd.DataFrame:
    where = ["t.txn_datetime >= :dt_start", "t.txn_datetime <= :dt_end"]
    params = {
        "dt_start": filters["dt_start"],
        "dt_end": filters["dt_end"],
    }

    if filters.get("project_id"):
        where.append("t.project_id = :project_id")
        params["project_id"] = filters["project_id"]

    # SQLite + SQLAlchemy(text) no acepta "IN :param" con tuplas.
    # Expandimos a IN (:tt0, :tt1, ...)
    if filters.get("txn_types"):
        txn_types = list(filters["txn_types"])
        if txn_types:
            keys = []
            for idx, val in enumerate(txn_types):
                k = f"tt{idx}"
                keys.append(k)
                params[k] = val
            where.append(f"t.txn_type IN ({', '.join(':' + k for k in keys)})")

    if filters.get("item_id"):
        where.append("t.item_id = :item_id")
        params["item_id"] = filters["item_id"]

    if filters.get("employee_id"):
        where.append("t.employee_id = :employee_id")
        params["employee_id"] = filters["employee_id"]

    if filters.get("location_id"):
        where.append("t.location_id = :location_id")
        params["location_id"] = filters["location_id"]

    size_mode = filters.get("size_mode", "cualquiera")
    size_value = filters.get("size_value")

    if size_mode == "sin talla":
        where.append("t.size IS NULL")
    elif size_mode == "talla específica" and size_value:
        where.append("t.size = :size")
        params["size"] = size_value.strip()

    if filters.get("text_search"):
        where.append(
            "(COALESCE(t.reference,'') LIKE :q OR COALESCE(t.notes,'') LIKE :q)"
        )
        params["q"] = f"%{filters['text_search'].strip()}%"

    # Filtro por motivo (guardado dentro de notes en V1)
    if filters.get("motivo"):
        where.append("COALESCE(t.notes,'') LIKE :motivo_q")
        params["motivo_q"] = f"%{filters['motivo']}%"

    q = f"""
    SELECT
      t.transaction_id AS id,
      t.txn_datetime AS fecha_hora,
      t.txn_type AS tipo_code,
      p.code AS proyecto,
      l.code AS ubicacion,
      e.full_name AS trabajador,
      e.dni AS dni,
      i.name AS epp,
      t.size AS talla,
      t.qty AS cantidad,
      t.reference AS guia_remision,
      t.notes AS notas,
      t.created_by AS creado_por
    FROM transactions t
    LEFT JOIN projects p ON p.project_id = t.project_id
    LEFT JOIN locations l ON l.location_id = t.location_id
    LEFT JOIN employees e ON e.employee_id = t.employee_id
    LEFT JOIN items i ON i.item_id = t.item_id
    WHERE {" AND ".join(where)}
    ORDER BY t.txn_datetime DESC, t.transaction_id DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn, params=params)

        # Etiquetas en español (manteniendo el código original para filtros/metricas)
        tipo_map = {
            "IN": "Ingreso",
            "OUT": "Entrega",
            "TRANSFER_IN": "Transferencia (entrada)",
            "TRANSFER_OUT": "Transferencia (salida)",
            "RETURN": "Devolución",
            "ADJUST": "Ajuste",
        }
        df["tipo"] = df["tipo_code"].map(tipo_map).fillna(df["tipo_code"])

        # Mostrar fecha/hora en zona local (America/Lima).
        # En BD se asume UTC; si viene naive, la tratamos como UTC.
        if "fecha_hora" in df.columns and not df.empty:
            fh = pd.to_datetime(df["fecha_hora"], errors="coerce")
            try:
                fh = fh.dt.tz_localize(
                    "UTC", nonexistent="shift_forward", ambiguous="NaT"
                )
            except Exception:
                # Si ya tiene tz o falla localize, intentamos convertir directamente
                pass
            try:
                fh = fh.dt.tz_convert(LOCAL_TZ)
            except Exception:
                # Si quedó naive por algún motivo, lo dejamos como está
                pass
            df["fecha_hora"] = fh.dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def query_kardex_item(
    project_id: int, item_id: int, size_mode: str, size_value: str | None
) -> pd.DataFrame:
    where = ["t.project_id = :pid", "t.item_id = :iid"]
    params = {"pid": project_id, "iid": item_id}

    if size_mode == "sin talla":
        where.append("t.size IS NULL")
    elif size_mode == "talla específica" and size_value:
        where.append("t.size = :size")
        params["size"] = size_value.strip()

    q = f"""
    SELECT
      t.transaction_id AS id,
      t.txn_datetime AS fecha_hora,
      t.txn_type AS tipo_code,
      l.code AS ubicacion,
      e.full_name AS trabajador,
      t.size AS talla,
      t.qty AS cantidad,
      t.reference AS guia_remision,
      t.notes AS notas
    FROM transactions t
    LEFT JOIN locations l ON l.location_id = t.location_id
    LEFT JOIN employees e ON e.employee_id = t.employee_id
    WHERE {" AND ".join(where)}
    ORDER BY t.txn_datetime ASC, t.transaction_id ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn, params=params)

    # Mostrar fecha/hora en zona local (America/Lima). En BD se asume UTC.
    if "fecha_hora" in df.columns and not df.empty:
        fh = pd.to_datetime(df["fecha_hora"], errors="coerce")
        try:
            fh = fh.dt.tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT")
        except Exception:
            pass
        try:
            fh = fh.dt.tz_convert(LOCAL_TZ)
        except Exception:
            pass
        df["fecha_hora"] = fh.dt.strftime("%Y-%m-%d %H:%M:%S")

    if df.empty:
        return df

    df["stock_acumulado"] = df["cantidad"].cumsum()
    tipo_map = {
        "IN": "Ingreso",
        "OUT": "Entrega",
        "TRANSFER_IN": "Transferencia (entrada)",
        "TRANSFER_OUT": "Transferencia (salida)",
        "RETURN": "Devolución",
        "ADJUST": "Ajuste",
    }
    df["tipo"] = df["tipo_code"].map(tipo_map).fillna(df["tipo_code"])
    return df


# -------------------------
# Catálogos
# -------------------------
with engine.connect() as conn:
    projects, items, employees, locations = load_catalogs(conn)

if projects.empty:
    st.error("No hay proyectos. Ejecuta el seed 001_seed_min.sql.")
    st.stop()

tab1, tab2 = st.tabs(["🧾 Movimientos (historial)", "📦 Kardex por ítem"])

# =========================
# TAB 1: Movimientos
# =========================
with tab1:
    st.subheader("Filtros")

    col1, col2, col3, col4 = st.columns([1.2, 1.2, 1.2, 1.2])

    today = pd.Timestamp.now().normalize()
    default_start = (today - pd.Timedelta(days=30)).date()

    with col1:
        date_start = st.date_input("Desde", value=default_start)
    with col2:
        date_end = st.date_input("Hasta", value=today.date())

    # Convertimos rango seleccionado (hora local) a UTC para que el filtro calce con lo guardado en BD
    dt_start_local = datetime.combine(date_start, time.min).replace(tzinfo=LOCAL_TZ)
    dt_end_local = datetime.combine(date_end, time.max).replace(tzinfo=LOCAL_TZ)
    dt_start = dt_start_local.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S")
    dt_end = dt_end_local.astimezone(UTC_TZ).strftime("%Y-%m-%d %H:%M:%S")

    with col3:
        proj_opt = st.selectbox(
            "Proyecto",
            ["(Todos)"] + projects["code"].tolist(),
            index=0,
        )
        project_id = None
        if proj_opt != "(Todos)":
            project_id = int(
                projects.loc[projects["code"] == proj_opt, "project_id"].values[0]
            )

    with col4:
        tipo_labels = {
            "Ingreso (IN)": "IN",
            "Entrega (OUT)": "OUT",
            "Transferencia entrada (TRANSFER_IN)": "TRANSFER_IN",
            "Transferencia salida (TRANSFER_OUT)": "TRANSFER_OUT",
            "Devolución (RETURN)": "RETURN",
            "Ajuste (ADJUST)": "ADJUST",
        }
        txn_types_labels = st.multiselect(
            "Tipo de movimiento",
            options=list(tipo_labels.keys()),
            default=["Ingreso (IN)", "Entrega (OUT)"],
        )
        txn_types = [tipo_labels[x] for x in txn_types_labels]

    col5, col6, col7, col8 = st.columns([1.6, 1.6, 1.2, 1.6])

    with col5:
        item_opt = st.selectbox("EPP", ["(Todos)"] + items["name"].tolist(), index=0)
        item_id = None
        if item_opt != "(Todos)":
            item_id = int(items.loc[items["name"] == item_opt, "item_id"].values[0])

    with col6:
        worker_opt = st.selectbox(
            "Trabajador", ["(Todos)"] + employees["full_name"].tolist(), index=0
        )
        employee_id = None
        if worker_opt != "(Todos)":
            employee_id = int(
                employees.loc[
                    employees["full_name"] == worker_opt, "employee_id"
                ].values[0]
            )

    with col7:
        size_mode = st.selectbox(
            "Filtro talla", ["cualquiera", "talla específica", "sin talla"], index=0
        )
        size_value = None
        if size_mode == "talla específica":
            size_value = st.text_input("Talla", value="").strip() or None

    with col8:
        motivo_opts = [
            "(Todos)",
            "Entrega inicial",
            "Renovación",
            "Desgaste",
            "Reposición",
            "Cambio de talla",
            "Otro",
        ]
        motivo_sel = st.selectbox("Motivo (según notas)", motivo_opts, index=0)
        text_search = st.text_input("Buscar en guía/notas", value="")

    loc_codes = locations["code"].dropna().tolist()
    loc_map = {
        "(Todas)": None,
        **{
            c: int(locations.loc[locations["code"] == c, "location_id"].values[0])
            for c in loc_codes
        },
    }
    location_opt = st.selectbox("Ubicación", list(loc_map.keys()), index=0)
    location_id = loc_map.get(location_opt)

    filters = {
        "dt_start": dt_start,
        "dt_end": dt_end,
        "project_id": project_id,
        "txn_types": txn_types,
        "item_id": item_id,
        "employee_id": employee_id,
        "location_id": location_id,
        "size_mode": size_mode,
        "size_value": size_value,
        "text_search": text_search,
        "motivo": None if motivo_sel == "(Todos)" else motivo_sel,
    }

    df = query_transactions(filters)

    st.divider()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Movimientos", len(df))
    with c2:
        st.metric(
            "Entradas (IN)", int((df["tipo_code"] == "IN").sum()) if not df.empty else 0
        )
    with c3:
        st.metric(
            "Salidas (OUT)",
            int((df["tipo_code"] == "OUT").sum()) if not df.empty else 0,
        )

    st.subheader("Resultado")
    st.dataframe(df, use_container_width=True, height=520)

    if not df.empty:
        col_dl1, col_dl2 = st.columns(2)

        with col_dl1:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Descargar CSV",
                data=csv_bytes,
                file_name="kardex_movimientos.csv",
                mime="text/csv",
            )

        with col_dl2:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="kardex")
            st.download_button(
                "⬇️ Descargar Excel",
                data=output.getvalue(),
                file_name="kardex_movimientos.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("No hay movimientos con esos filtros.")

# =========================
# TAB 2: Kardex por ítem
# =========================
with tab2:
    st.subheader("Kardex por ítem (stock acumulado)")

    colA, colB, colC = st.columns([1.2, 2.2, 1.2])

    with colA:
        proj_code = st.selectbox(
            "Proyecto (obligatorio)",
            projects["code"].tolist(),
            index=0,
        )
        pid = int(projects.loc[projects["code"] == proj_code, "project_id"].values[0])

    with colB:
        item_name = st.selectbox("EPP (obligatorio)", items["name"].tolist(), index=0)
        iid = int(items.loc[items["name"] == item_name, "item_id"].values[0])
        has_size = int(items.loc[items["name"] == item_name, "has_size"].values[0])

    with colC:
        size_mode2 = st.selectbox(
            "Talla", ["cualquiera", "talla específica", "sin talla"], index=0
        )
        size_value2 = None
        if size_mode2 == "talla específica" and has_size == 1:
            size_value2 = (
                st.text_input("Talla exacta (ej: T/39)", value="").strip() or None
            )
        elif size_mode2 == "talla específica" and has_size == 0:
            st.caption("Este EPP no maneja talla.")

    dfk = query_kardex_item(pid, iid, size_mode2, size_value2)

    st.divider()

    if dfk.empty:
        st.info("No hay movimientos para ese EPP/proyecto (con ese filtro de talla).")
    else:
        st.dataframe(dfk, use_container_width=True, height=520)

        current_stock = int(dfk["stock_acumulado"].iloc[-1])
        st.success(f"📦 Stock actual (según kardex): **{current_stock} UND**")

        col_dl1, col_dl2 = st.columns(2)

        with col_dl1:
            csv_bytes = dfk.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Descargar CSV del kardex",
                data=csv_bytes,
                file_name="kardex_item.csv",
                mime="text/csv",
            )

        with col_dl2:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                dfk.to_excel(writer, index=False, sheet_name="kardex_item")
            st.download_button(
                "⬇️ Descargar Excel del kardex",
                data=output.getvalue(),
                file_name="kardex_item.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.caption(
    "Kardex basado en transactions. IN suma, OUT resta (qty negativa). No se editan movimientos; se corrige con nuevos movimientos."
)
