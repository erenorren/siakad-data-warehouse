# etl/dim_prodi_etl.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Prodi & Fakultas (OLTP)", retries=2)
def extract_prodi_source():
    conn_oltp = get_oltp_connection()
    df_prodi = pd.read_sql("SELECT id_prodi, kode_prodi, nama_prodi, jenjang, id_fakultas, akreditasi_prodi FROM program_studi;", conn_oltp)
    df_fak = pd.read_sql("SELECT id_fakultas, kode_fakultas, nama_fakultas FROM fakultas;", conn_oltp)
    conn_oltp.close()
    return df_prodi, df_fak

@task(name="Transform: Gabung Prodi & Fakultas")
def transform_dim_prodi(df_prodi: pd.DataFrame, df_fak: pd.DataFrame) -> pd.DataFrame:
    df_merged = pd.merge(df_prodi, df_fak, on="id_fakultas", how="left")
    df_merged['id_prodi_sumber'] = df_merged['id_prodi'].astype(int)
    df_merged['akreditasi_prodi'] = df_merged['akreditasi_prodi'].fillna('B')
    
    return df_merged[['id_prodi_sumber', 'kode_prodi', 'nama_prodi', 'jenjang', 'kode_fakultas', 'nama_fakultas', 'akreditasi_prodi']]

@task(name="Load: dim_prodi (DW)")
def load_dim_prodi(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO dim_prodi (id_prodi_sumber, kode_prodi, nama_prodi, jenjang, kode_fakultas, nama_fakultas, akreditasi_prodi)
        VALUES %s ON CONFLICT (id_prodi_sumber) DO UPDATE SET
            nama_prodi = EXCLUDED.nama_prodi,
            akreditasi_prodi = EXCLUDED.akreditasi_prodi;
    """
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke dim_prodi.")
    except Exception as e:
        print(f"Gagal memuat data: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Prodi")
def flow_etl_dim_prodi():
    df_p, df_f = extract_prodi_source()
    df_ready = transform_dim_prodi(df_p, df_f)
    load_dim_prodi(df_ready)

if __name__ == "__main__":
    flow_etl_dim_prodi()