from pathlib import Path
from sqlalchemy import text

from app.db.connection import get_engine

SEED_PATH = Path("sql/seeds/001_seed_min.sql")


def main() -> None:
    engine = get_engine()
    sql = SEED_PATH.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))

    print("✅ Seeds aplicados (proyectos, almacén, zonas, segregación).")


if __name__ == "__main__":
    main()
