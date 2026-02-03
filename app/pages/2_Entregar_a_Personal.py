from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Entregar EPP", layout="wide")
st.title("👷‍♂️ Entregar EPP a Personal")

engine = get_engine()


# -------------------------
# Helpers
# -------------------------
def get_projects(conn):
    return pd.read_sql(
        text(
            "SELECT project_id, code, name FROM projects WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def get_locations_for_project(conn, project_code: str):
    q = """
    SELECT l.location_id, l.code, l.name, l.is_segregation
    FROM locations l
    LEFT JOIN projects p ON p.project_id = l.project_id
    WHERE (p.code = :pcode)
    ORDER BY l.name;
    """
    return pd.read_sql(text(q), conn, params={"pcode": project_code})


def get_workers(conn):
    # Tabla employees creada por tu importador de personal.xlsx
    return pd.read_sql(
        text("SELECT employee_id, full_name, dni FROM employees ORDER BY full_name"),
        conn,
    )


def get_items(conn):
    return pd.read_sql(
        text(
            "SELECT item_id, name, has_size FROM items WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def get_stock(conn, project_id: int, item_id: int, size: str | None):
    q = """
    SELECT COALESCE(SUM(qty), 0) AS stock
    FROM transactions
    WHERE project_id=:pid
      AND item_id=:iid
      AND ((:size IS NULL AND size IS NULL) OR (:size IS NOT NULL AND size=:size));
    """
    return conn.execute(
        text(q), {"pid": project_id, "iid": item_id, "size": size}
    ).scalar_one()


# -------------------------
# Carga catálogos
# -------------------------
with engine.connect() as conn:
    projects = get_projects(conn)
    workers = get_workers(conn)
    items = get_items(conn)

if projects.empty:
    st.error("No hay proyectos en la base de datos. Ejecuta el seed 001_seed_min.sql.")
    st.stop()

if workers.empty:
    st.error(
        "No hay personal cargado (tabla employees). Importa el archivo personal.xlsx primero."
    )
    st.stop()

if items.empty:
    st.error("No hay EPP (tabla items). Ejecuta el seed 002_master_epp.sql primero.")
    st.stop()

# -------------------------
# UI - Selecciones
# -------------------------
col1, col2 = st.columns(2)

with col1:
    project_code = st.selectbox(
        "Proyecto",
        projects["code"],
        format_func=lambda c: projects.loc[projects["code"] == c, "name"].values[0],
    )
    project_id = int(
        projects.loc[projects["code"] == project_code, "project_id"].values[0]
    )

with engine.connect() as conn:
    locations = get_locations_for_project(conn, project_code)

with col2:
    if locations.empty:
        st.error("No hay ubicaciones asociadas al proyecto. Revisa locations / seed.")
        st.stop()
    location_code = st.selectbox(
        "Ubicación (salida)",
        locations["code"],
        format_func=lambda c: locations.loc[locations["code"] == c, "name"].values[0],
    )
    location_id = int(
        locations.loc[locations["code"] == location_code, "location_id"].values[0]
    )

st.divider()

colA, colB = st.columns(2)

with colA:
    worker_name = st.selectbox("Trabajador", workers["full_name"])
    worker_id = int(
        workers.loc[workers["full_name"] == worker_name, "employee_id"].values[0]
    )
    worker_dni = workers.loc[workers["full_name"] == worker_name, "dni"].values[0]
    st.caption(f"DNI: {worker_dni}")

with colB:
    item_name = st.selectbox("EPP", items["name"])
    item_row = items[items["name"] == item_name].iloc[0]
    item_id = int(item_row["item_id"])
    has_size = int(item_row["has_size"])

size = None
if has_size == 1:
    size = st.text_input("Talla (ej: T/39)", value="").strip() or None
else:
    st.text_input("Talla", value="(no aplica)", disabled=True)

qty = st.number_input("Cantidad a entregar", min_value=1, step=1)

motivo = st.selectbox(
    "Motivo",
    [
        "Entrega inicial",
        "Renovación",
        "Desgaste",
        "Reposición",
        "Cambio de talla",
        "Otro",
    ],
)
observacion = st.text_area("Observación (opcional)", value="", height=90)

# -------------------------
# Stock disponible
# -------------------------
with engine.connect() as conn:
    stock = get_stock(conn, project_id, item_id, size)

st.info(
    f"📦 Stock disponible ({project_code}) para **{item_name}** {('(' + size + ')' if size else '')}: **{stock}** UND"
)

if qty > stock:
    st.error(
        "❌ Stock insuficiente para esta entrega. Ajusta la cantidad o registra un ingreso primero."
    )
    can_proceed = False
else:
    can_proceed = True

# -------------------------
# NIVEL 2 DE SEGURIDAD: doble confirmación + anti-duplicado + ID visible
# -------------------------
if "pending_out" not in st.session_state:
    st.session_state["pending_out"] = None
if "force_duplicate_out" not in st.session_state:
    st.session_state["force_duplicate_out"] = False

payload = {
    "project_code": project_code,
    "project_id": project_id,
    "location_code": location_code,
    "location_id": location_id,
    "worker_name": worker_name,
    "worker_id": worker_id,
    "item_name": item_name,
    "item_id": item_id,
    "size": size,
    "qty": int(qty),
    "motivo": motivo,
    "observacion": observacion.strip() or None,
}

st.subheader("Confirmación de entrega")

confirm = st.checkbox(
    "Confirmo que los datos son correctos y deseo continuar", disabled=not can_proceed
)

if st.button("➡️ Revisar entrega", disabled=(not can_proceed or not confirm)):
    if has_size == 1 and not size:
        st.error("Este EPP requiere talla.")
        st.stop()
    st.session_state["pending_out"] = payload
    st.session_state["force_duplicate_out"] = False
    st.rerun()

pending = st.session_state.get("pending_out")

if pending:
    st.warning(
        f"""
**Resumen de entrega**
- Proyecto: **{pending['project_code']}**
- Ubicación: **{pending['location_code']}**
- Trabajador: **{pending['worker_name']}**
- EPP: **{pending['item_name']}**
- Talla: **{pending['size'] or '-'}**
- Cantidad: **-{pending['qty']} UND**
- Motivo: **{pending['motivo']}**
""",
        icon="⚠️",
    )

    # Anti-duplicado (misma entrega en los últimos 10 segundos)
    dup_q = """
    SELECT transaction_id
    FROM transactions
    WHERE txn_type='OUT'
      AND project_id=:pid
      AND employee_id=:eid
      AND item_id=:iid
      AND qty=:neg_qty
      AND ((:size IS NULL AND size IS NULL) OR (:size IS NOT NULL AND size=:size))
      AND txn_datetime >= datetime(:now_utc,'-10 seconds')
    ORDER BY transaction_id DESC
    LIMIT 1;
    """

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as conn:
        dup = conn.execute(
            text(dup_q),
            {
                "pid": pending["project_id"],
                "eid": pending["worker_id"],
                "iid": pending["item_id"],
                "neg_qty": -pending["qty"],
                "size": pending["size"],
                "now_utc": now_utc,
            },
        ).fetchone()

    if dup and not st.session_state["force_duplicate_out"]:
        st.error(
            "⚠️ Posible duplicado detectado (misma entrega en los últimos 10 segundos)."
        )
        st.session_state["force_duplicate_out"] = st.checkbox(
            "Registrar de todas formas", value=False
        )

    colX, colY = st.columns(2)

    with colX:
        if st.button(
            "✅ Confirmar entrega",
            type="primary",
            disabled=(dup is not None and not st.session_state["force_duplicate_out"]),
        ):
            # Revalidar stock justo antes de registrar (seguridad extra)
            with engine.connect() as conn:
                current_stock = get_stock(
                    conn, pending["project_id"], pending["item_id"], pending["size"]
                )
            if pending["qty"] > current_stock:
                st.error(
                    f"❌ Stock cambió mientras confirmabas. Stock actual: {current_stock}. "
                    "Actualiza y vuelve a intentar."
                )
                st.stop()

            with engine.begin() as conn:
                conn.execute(text("PRAGMA foreign_keys=ON;"))
                conn.execute(
                    text(
                        """
                    INSERT INTO transactions (
                        txn_datetime, txn_type, project_id, location_id,
                        item_id, qty, size, employee_id,
                        request_number, reference, notes, created_by
                    ) VALUES (
                        :now_utc,'OUT',:pid,:lid,
                        :iid,:qty,:size,:eid,
                        NULL,NULL,:notes,'kevin'
                    )
                    """
                    ),
                    {
                        "pid": pending["project_id"],
                        "lid": pending["location_id"],
                        "iid": pending["item_id"],
                        "qty": -pending["qty"],  # salida es negativa
                        "size": pending["size"],
                        "eid": pending["worker_id"],
                        "notes": f"{pending['motivo']}"
                        + (
                            f" | {pending['observacion']}"
                            if pending["observacion"]
                            else ""
                        ),
                        "now_utc": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    },
                )
                txn_id = conn.execute(text("SELECT last_insert_rowid();")).scalar_one()

            st.success(
                f"✅ Entrega registrada correctamente | ID movimiento: **{txn_id}**"
            )
            st.session_state["pending_out"] = None
            st.session_state["force_duplicate_out"] = False
            st.rerun()

    with colY:
        if st.button("❌ Cancelar"):
            st.session_state["pending_out"] = None
            st.session_state["force_duplicate_out"] = False
            st.rerun()
