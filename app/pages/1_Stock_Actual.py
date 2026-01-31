import pandas as pd
import streamlit as st
from db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Stock Actual", layout="wide")

st.title("📦 Stock Actual por Proyecto")

engine = get_engine()

# Selector de proyecto
with engine.connect() as conn:
    projects = pd.read_sql(
        text("SELECT code, name FROM projects WHERE is_active=1 ORDER BY name"), conn
    )

project = st.selectbox(
    "Selecciona proyecto",
    projects["code"],
    format_func=lambda c: projects.loc[projects["code"] == c, "name"].values[0],
)

show_zero = st.checkbox("Mostrar EPP sin stock", value=False)

query = """
SELECT
    i.name AS epp,
    COALESCE(t.size, '-') AS talla,
    COALESCE(SUM(t.qty), 0) AS stock
FROM items i
LEFT JOIN transactions t
    ON t.item_id = i.item_id
    AND t.project_id = (SELECT project_id FROM projects WHERE code = :project)
WHERE i.is_active = 1
GROUP BY i.name, t.size
ORDER BY i.name, talla;
"""

with engine.connect() as conn:
    df = pd.read_sql(text(query), conn, params={"project": project})

if not show_zero:
    df = df[df["stock"] > 0]

st.subheader("Stock disponible")

if df.empty:
    st.info("No hay stock disponible para este proyecto.")
else:
    st.dataframe(df, use_container_width=True)

st.caption("Fuente: Kardex (transactions)")
