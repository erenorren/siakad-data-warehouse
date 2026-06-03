# etl/config.py
import os
from pathlib import Path
import psycopg2

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

DB_HOST = "localhost"
DB_PORT = "5432"
OLTP_NAME = "siakad_oltp"
DW_NAME = "siakad_dw"
DB_USER = "postgres"
DB_PASS = "postgres"  # Sesuaikan dengan password database Anda

def get_oltp_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=OLTP_NAME, user=DB_USER, password=DB_PASS
    )

def get_dw_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DW_NAME, user=DB_USER, password=DB_PASS
    )