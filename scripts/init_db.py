from pathlib import Path

from sqlalchemy import text

from app.db.connection import get_engine

SCHEMA_PATH = Path("sql/migrations/001_init.sql")


def main() -> None:
    engine = get_engine()
    sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with engine.begin() as conn:
        # SQLite: asegurar FK
        conn.execute(text("PRAGMA foreign_keys = ON;"))
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))

    print("✅ BD inicializada y schema aplicado correctamente.")


if __name__ == "__main__":
    main()
