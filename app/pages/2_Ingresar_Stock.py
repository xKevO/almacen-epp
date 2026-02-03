import pandas as pd
import streamlit as st
from db.connection import get_engine
from sqlalchemy import text

st.set_page_config(page_title="Ingresar Stock", layout="wide")
st.title("📥 Ingresar Stock")

engine = get_engine()


# Helpers
def get_projects(conn):
    return pd.read_sql(
        text(
            "SELECT project_id, code, name FROM projects WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def get_locations(conn, project_code: str):
    # Zonas del proyecto + segregación
    q = """
    SELECT l.location_id, l.code, l.name, l.is_segregation
    FROM locations l
    LEFT JOIN projects p ON p.project_id = l.project_id
    WHERE (p.code = :pcode) OR (l.project_id IS NULL AND l.is_segregation=1)
    ORDER BY l.is_segregation DESC, l.name;
    """
    return pd.read_sql(text(q), conn, params={"pcode": project_code})


def get_items(conn):
    return pd.read_sql(
        text(
            "SELECT item_id, name, has_size FROM items WHERE is_active=1 ORDER BY name"
        ),
        conn,
    )


def get_current_stock(conn, project_id: int, item_id: int, size: str | None):
    q = """
    SELECT COALESCE(SUM(qty), 0) AS stock
    FROM transactions
    WHERE project_id=:pid AND item_id=:iid AND (
        (:size IS NULL AND size IS NULL) OR (:size IS NOT NULL AND size=:size)
    );
    """
    return conn.execute(
        text(q), {"pid": project_id, "iid": item_id, "size": size}
    ).scalar_one()


# UI
with engine.connect() as conn:
    projects = get_projects(conn)
    items = get_items(conn)

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
    locations = get_locations(conn, project_code)

with col2:
    location_code = st.selectbox(
        "Ubicación",
        locations["code"],
        format_func=lambda c: locations.loc[locations["code"] == c, "name"].values[0],
    )
    location_id = int(
        locations.loc[locations["code"] == location_code, "location_id"].values[0]
    )

st.divider()

colA, colB, colC = st.columns([2, 1, 1])

with colA:
    item_name = st.selectbox("EPP", items["name"])
    item_row = items[items["name"] == item_name].iloc[0]
    item_id = int(item_row["item_id"])
    has_size = int(item_row["has_size"])

with colB:
    size = None
    if has_size == 1:
        size = st.text_input("Talla (ej: T/39)", value="")
        size = size.strip() or None
    else:
        st.text_input("Talla", value="(no aplica)", disabled=True)

with colC:
    qty = st.number_input("Cantidad a ingresar", min_value=1, step=1)

guia_remision = st.text_input("Guía de remisión (opcional)", value="")
requerimiento = st.text_input("N° requerimiento a Lima (opcional)", value="")
notes = st.text_area("Notas adicionales (opcional)", value="", height=80)

# Preview stock actual
with engine.connect() as conn:
    current_stock = get_current_stock(conn, project_id, item_id, size)

st.info(
    f"📦 Stock actual ({project_code}) para **{item_name}** {('(' + size + ')' if size else '')}: **{current_stock}** UND"
)

# --- NIVEL 2 DE SEGURIDAD: DOBLE CONFIRMACIÓN + ANTIDUPLICADO ---
if "pending_in" not in st.session_state:
    st.session_state["pending_in"] = None
if "force_duplicate_in" not in st.session_state:
    st.session_state["force_duplicate_in"] = False

payload = {
    "project_code": project_code,
    "project_id": project_id,
    "location_code": location_code,
    "location_id": location_id,
    "item_name": item_name,
    "item_id": item_id,
    "size": size,
    "qty": int(qty),
    "guia": guia_remision.strip() or None,
    "req": requerimiento.strip() or None,
    "notes": notes.strip() or None,
}

st.subheader("Confirmación de ingreso")

confirm = st.checkbox("Confirmo que los datos son correctos y deseo continuar")

if st.button("➡️ Revisar ingreso", disabled=not confirm):
    if has_size == 1 and not size:
        st.error("Este EPP requiere talla.")
        st.stop()
    st.session_state["pending_in"] = payload
    st.session_state["force_duplicate_in"] = False
    st.rerun()

pending = st.session_state.get("pending_in")

if pending:
    st.warning(
        f"""
**Resumen del ingreso**
- Proyecto: {pending['project_code']}
- Ubicación: {pending['location_code']}
- EPP: {pending['item_name']}
- Talla: {pending['size'] or '-'}
- Cantidad: +{pending['qty']}
- Guía: {pending['guia'] or '-'}
- Requerimiento: {pending['req'] or '-'}
""",
        icon="⚠️",
    )

    dup_q = """
    SELECT transaction_id
    FROM transactions
    WHERE txn_type='IN'
      AND project_id=:pid
      AND location_id=:lid
      AND item_id=:iid
      AND qty=:qty
      AND ((:size IS NULL AND size IS NULL) OR (:size IS NOT NULL AND size=:size))
      AND txn_datetime >= datetime('now','-10 seconds')
    LIMIT 1;
    """

    with engine.connect() as conn:
        dup = conn.execute(
            text(dup_q),
            {
                "pid": pending["project_id"],
                "lid": pending["location_id"],
                "iid": pending["item_id"],
                "qty": pending["qty"],
                "size": pending["size"],
            },
        ).fetchone()

    if dup and not st.session_state["force_duplicate_in"]:
        st.error("⚠️ Posible duplicado detectado.")
        st.session_state["force_duplicate_in"] = st.checkbox(
            "Registrar de todas formas"
        )

    colA, colB = st.columns(2)

    with colA:
        if st.button(
            "✅ Confirmar ingreso",
            type="primary",
            disabled=(dup is not None and not st.session_state["force_duplicate_in"]),
        ):
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
                        datetime('now'),'IN',:pid,:lid,
                        :iid,:qty,:size,NULL,
                        NULL,:ref,:notes,'kevin'
                    )
                    """
                    ),
                    {
                        "pid": pending["project_id"],
                        "lid": pending["location_id"],
                        "iid": pending["item_id"],
                        "qty": pending["qty"],
                        "size": pending["size"],
                        "ref": pending["guia"],
                        "notes": (
                            ("REQ: " + pending["req"])
                            if pending["req"]
                            else pending["notes"]
                        ),
                    },
                )
                txn_id = conn.execute(text("SELECT last_insert_rowid();")).scalar_one()

            st.success(f"✅ Ingreso registrado | ID movimiento: {txn_id}")
            st.session_state["pending_in"] = None
            st.session_state["force_duplicate_in"] = False
            st.rerun()

    with colB:
        if st.button("❌ Cancelar"):
            st.session_state["pending_in"] = None
            st.session_state["force_duplicate_in"] = False
            st.rerun()

st.caption(
    "Esto crea un movimiento IN en transactions. El stock visible se calcula por SUM(qty)."
)
