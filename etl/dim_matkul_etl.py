# etl/dim_matkul_etl.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Mata Kuliah (OLTP)", retries=2)
def extract_matkul_source() -> pd.DataFrame:
    conn_oltp = get_oltp_connection()
    df = pd.read_sql("SELECT id_mk, kode_mk, nama_mk, sks, semester_ke, jenis_mk, id_prodi FROM mata_kuliah;", conn_oltp)
    conn_oltp.close()
    return df

@task(name="Transform: Map SK Prodi Mata Kuliah")
def transform_dim_matkul(df_mk: pd.DataFrame) -> pd.DataFrame:
    conn_dw = get_dw_connection()
    df_prodi_dw = pd.read_sql("SELECT id_prodi_sumber, sk_prodi FROM dim_prodi;", conn_dw)
    conn_dw.close()
    
    prodi_map = dict(zip(df_prodi_dw['id_prodi_sumber'], df_prodi_dw['sk_prodi']))
    
    df_mk['id_mk_sumber'] = df_mk['id_mk'].astype(int)
    df_mk['sks'] = df_mk['sks'].astype(int)
    df_mk['semester_ke'] = df_mk['semester_ke'].astype(int)
    df_mk['sk_prodi'] = df_mk['id_prodi'].map(prodi_map).fillna(1).astype(int)
    
    return df_mk[['id_mk_sumber', 'kode_mk', 'nama_mk', 'sks', 'semester_ke', 'jenis_mk', 'sk_prodi']]

@task(name="Load: dim_mata_kuliah (DW)")
def load_dim_matkul(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO dim_mata_kuliah (id_mk_sumber, kode_mk, nama_mk, sks, semester_ke, jenis_mk, sk_prodi)
        VALUES %s ON CONFLICT (id_mk_sumber) DO UPDATE SET
            nama_mk = EXCLUDED.nama_mk,
            sks = EXCLUDED.sks,
            semester_ke = EXCLUDED.semester_ke;
    """
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke dim_mata_kuliah.")
    except Exception as e:
        print(f"Gagal memuat data: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Mata Kuliah")
def flow_etl_dim_matkul():
    df_src = extract_matkul_source()
    df_ready = transform_dim_matkul(df_src)
    load_dim_matkul(df_ready)

if __name__ == "__main__":
    flow_etl_dim_matkul()