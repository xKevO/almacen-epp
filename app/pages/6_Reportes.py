import pandas as pd
import streamlit as st
from db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Reportes KPI", layout="wide")
st.title("📊 KPIs de Rotación / Consumo de EPP")

engine = get_engine()


# -------------------------
# Helpers
# -------------------------
def load_projects(conn) -> pd.DataFrame:
    return pd.read_sql(
        text(
            "SELECT project_id, code, name FROM projects WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def load_items(conn) -> pd.DataFrame:
    return pd.read_sql(
        text(
            "SELECT item_id, name, has_size FROM items WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def load_employees(conn) -> pd.DataFrame:
    return pd.read_sql(
        text("SELECT employee_id, full_name, dni FROM employees ORDER BY full_name"),
        conn,
    )


def query_consumo(filters: dict) -> pd.DataFrame:
    """
    Consumo = movimientos OUT (qty negativa). Para reportes usaremos unidades positivas: consumo_und = -qty.
    """
    where = [
        "t.txn_datetime >= :dt_start",
        "t.txn_datetime <= :dt_end",
        "t.txn_type = 'OUT'",
    ]
    params = {
        "dt_start": filters["dt_start"],
        "dt_end": filters["dt_end"],
    }

    if filters.get("project_id"):
        where.append("t.project_id = :pid")
        params["pid"] = filters["project_id"]

    if filters.get("item_id"):
        where.append("t.item_id = :iid")
        params["iid"] = filters["item_id"]

    if filters.get("employee_id"):
        where.append("t.employee_id = :eid")
        params["eid"] = filters["employee_id"]

    # Motivo (V1: está dentro de notes)
    if filters.get("motivo"):
        where.append("COALESCE(t.notes,'') LIKE :motivo_q")
        params["motivo_q"] = f"%{filters['motivo']}%"

    q = f"""
    SELECT
      t.transaction_id,
      t.txn_datetime AS fecha_hora,
      p.code AS proyecto,
      e.full_name AS trabajador,
      e.dni AS dni,
      i.name AS epp,
      t.size AS talla,
      (-t.qty) AS consumo_und,
      t.notes AS notas
    FROM transactions t
    LEFT JOIN projects p ON p.project_id = t.project_id
    LEFT JOIN employees e ON e.employee_id = t.employee_id
    LEFT JOIN items i ON i.item_id = t.item_id
    WHERE {" AND ".join(where)}
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn, params=params)

    # Normalizamos fecha
    if not df.empty:
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
        df["fecha"] = df["fecha_hora"].dt.date
        df["mes"] = df["fecha_hora"].dt.to_period("M").astype(str)

    return df


# -------------------------
# Carga catálogos
# -------------------------
with engine.connect() as conn:
    projects = load_projects(conn)
    items = load_items(conn)
    employees = load_employees(conn)

if projects.empty:
    st.error("No hay proyectos en la base. Ejecuta el seed 001_seed_min.sql.")
    st.stop()

if items.empty:
    st.error("No hay EPP en la base. Ejecuta el seed 002_master_epp.sql.")
    st.stop()

# -------------------------
# Filtros
# -------------------------
st.subheader("Filtros")

today = pd.Timestamp.now().normalize()
default_start = (today - pd.Timedelta(days=30)).date()

c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.6, 1.6])

with c1:
    date_start = st.date_input("Desde", value=default_start)
with c2:
    date_end = st.date_input("Hasta", value=today.date())

dt_start = f"{date_start} 00:00:00"
dt_end = f"{date_end} 23:59:59"

with c3:
    proj_opt = st.selectbox(
        "Proyecto",
        ["(Todos)"] + projects["code"].tolist(),
        index=0,
        format_func=lambda c: (
            "(Todos)"
            if c == "(Todos)"
            else projects.loc[projects["code"] == c, "name"].values[0]
        ),
    )
    project_id = None
    if proj_opt != "(Todos)":
        project_id = int(
            projects.loc[projects["code"] == proj_opt, "project_id"].values[0]
        )

with c4:
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
    motivo = None if motivo_sel == "(Todos)" else motivo_sel

c5, c6, c7 = st.columns([2.2, 2.2, 1.6])

with c5:
    item_opt = st.selectbox("EPP", ["(Todos)"] + items["name"].tolist(), index=0)
    item_id = None
    if item_opt != "(Todos)":
        item_id = int(items.loc[items["name"] == item_opt, "item_id"].values[0])

with c6:
    worker_opt = st.selectbox(
        "Trabajador", ["(Todos)"] + employees["full_name"].tolist(), index=0
    )
    employee_id = None
    if worker_opt != "(Todos)":
        employee_id = int(
            employees.loc[employees["full_name"] == worker_opt, "employee_id"].values[0]
        )

with c7:
    gran = st.selectbox("Agrupar por", ["Mes", "Día"], index=0)

filters = {
    "dt_start": dt_start,
    "dt_end": dt_end,
    "project_id": project_id,
    "item_id": item_id,
    "employee_id": employee_id,
    "motivo": motivo,
}

df = query_consumo(filters)

st.divider()

# -------------------------
# KPIs superiores
# -------------------------
st.subheader("Resumen (solo Entregas / OUT)")

if df.empty:
    st.info("No hay entregas (OUT) en el rango seleccionado.")
    st.stop()

total_und = int(df["consumo_und"].sum())
movs = int(len(df))
trabajadores = (
    int(df["dni"].nunique()) if "dni" in df.columns else int(df["trabajador"].nunique())
)

# Top item y top trabajador
top_item = (
    df.groupby("epp", dropna=False)["consumo_und"]
    .sum()
    .sort_values(ascending=False)
    .head(1)
)
top_worker = (
    df.groupby("trabajador", dropna=False)["consumo_und"]
    .sum()
    .sort_values(ascending=False)
    .head(1)
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Unidades entregadas", f"{total_und}")
k2.metric("N° entregas (movimientos)", f"{movs}")
k3.metric("Trabajadores con entregas", f"{trabajadores}")
k4.metric(
    "Mayor consumo",
    f"{top_item.index[0]} ({int(top_item.iloc[0])} und)" if not top_item.empty else "—",
)

st.caption(
    "Nota: El consumo se calcula como `-qty` para movimientos OUT (qty negativa)."
)

st.divider()

# -------------------------
# Series temporales (rotación)
# -------------------------
st.subheader("Rotación en el tiempo")

if gran == "Mes":
    serie = (
        df.groupby("mes")["consumo_und"]
        .sum()
        .reset_index()
        .rename(columns={"consumo_und": "unidades"})
    )
    serie = serie.sort_values("mes")
    st.line_chart(serie.set_index("mes")["unidades"])
else:
    serie = (
        df.groupby("fecha")["consumo_und"]
        .sum()
        .reset_index()
        .rename(columns={"consumo_und": "unidades"})
    )
    serie = serie.sort_values("fecha")
    st.line_chart(serie.set_index("fecha")["unidades"])

st.divider()

# -------------------------
# Top rankings
# -------------------------
colA, colB = st.columns(2)

with colA:
    st.subheader("Top 10 EPP consumidos")
    top_epp = (
        df.groupby("epp")["consumo_und"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    top_epp = top_epp.rename(columns={"consumo_und": "unidades"})
    st.dataframe(top_epp, use_container_width=True, height=360)
    if not top_epp.empty:
        st.bar_chart(top_epp.set_index("epp")["unidades"])

with colB:
    st.subheader("Top 10 trabajadores por consumo")
    top_trab = (
        df.groupby(["trabajador", "dni"])["consumo_und"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    top_trab = top_trab.rename(columns={"consumo_und": "unidades"})
    st.dataframe(top_trab, use_container_width=True, height=360)

st.divider()

# -------------------------
# Consumo por motivo (KPI clave)
# -------------------------
st.subheader("Consumo por motivo de entrega")


# Extraemos motivo desde notas (V1)
def extract_motivo(notes: str | None) -> str:
    if not notes:
        return "No especificado"
    motivos = [
        "Entrega inicial",
        "Renovación",
        "Desgaste",
        "Reposición",
        "Cambio de talla",
        "Otro",
    ]
    for m in motivos:
        if m.lower() in notes.lower():
            return m
    return "No especificado"


df_m = df.copy()
df_m["motivo"] = df_m["notas"].apply(extract_motivo)

consumo_motivo = (
    df_m.groupby("motivo")["consumo_und"]
    .sum()
    .reset_index()
    .rename(columns={"consumo_und": "unidades"})
    .sort_values("unidades", ascending=False)
)

if consumo_motivo.empty:
    st.info("No hay datos suficientes para mostrar consumo por motivo.")
else:
    colM1, colM2 = st.columns([1.2, 1.8])

    with colM1:
        st.dataframe(consumo_motivo, use_container_width=True, height=260)

    with colM2:
        st.bar_chart(consumo_motivo.set_index("motivo")["unidades"])

# -------------------------
# Matriz EPP vs Proyecto (solo si estás en Todos)
# -------------------------
st.subheader("Consumo por proyecto (comparativo)")

if project_id is None:
    piv = (
        df.groupby(["proyecto", "epp"])["consumo_und"]
        .sum()
        .reset_index()
        .pivot(index="epp", columns="proyecto", values="consumo_und")
        .fillna(0)
        .astype(int)
    )
    st.dataframe(piv, use_container_width=True, height=420)
else:
    st.info("Selecciona '(Todos)' en Proyecto para ver el comparativo entre proyectos.")

st.divider()

# -------------------------
# Exportación
# -------------------------
st.subheader("Exportar datos base (para BI)")

csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "⬇️ Descargar datos de consumo (CSV)",
    data=csv_bytes,
    file_name="kpi_consumo_out.csv",
    mime="text/csv",
)

st.caption(
    "Estos datos (OUT) son la base para dashboards en Power BI/Tableau. Más adelante agregaremos indicadores de stock crítico y vida útil."
)
