# etl/config.py
import os
from pathlib import Path
import psycopg2

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

# etl/config.py

DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "postgres"  # Sesuaikan dengan password Anda

# 1. PASTIKAN NAMA DATABASE INI SAMA PERSIS DENGAN YANG ADA DI FOTO ANDA
OLTP_NAME = "oltp_siakad"  # Database tempat tabel 'tahun_akademik' berada
DW_NAME = "dw_siakad"      # Database target Data Warehouse

def get_oltp_connection():
    return psycopg2.connect(
        # Hati-hati di bagian dbname, pastikan menggunakan OLTP_NAME, bukan DW_NAME
        host=DB_HOST, port=DB_PORT, dbname=OLTP_NAME, user=DB_USER, password=DB_PASS
    )

def get_dw_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DW_NAME, user=DB_USER, password=DB_PASS
    )