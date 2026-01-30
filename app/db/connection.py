import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL no está definido. Revisa tu archivo .env")
    return url


def get_engine() -> Engine:
    url = get_database_url()
    # SQLite necesita este flag para trabajar bien con Streamlit (hilos)
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, echo=False, connect_args=connect_args)
