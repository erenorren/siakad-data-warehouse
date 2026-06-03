# etl/dim_waktu_etl.py
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Tahun Akademik (OLTP)", retries=2)
def extract_waktu_source() -> pd.DataFrame:
    query = "SELECT id_ta, kode_ta, tahun_mulai, semester FROM tahun_akademik;"
    conn_oltp = get_oltp_connection()
    df = pd.read_sql(query, conn_oltp)
    conn_oltp.close()
    return df

@task(name="Transform: Aturan Dimensi Waktu")
def transform_dim_waktu(df: pd.DataFrame) -> pd.DataFrame:
    print("Mentransformasi data waktu akademik...")
    label_map = {1: 'Ganjil', 2: 'Genap'}
    
    df['id_ta_sumber'] = df['id_ta'].astype(int)
    df['tahun_akademik'] = df['tahun_mulai'].astype(int)
    df['semester'] = df['semester'].astype(int)
    df['label_semester'] = df['semester'].map(label_map)
    df['kuartal'] = (df['semester'] * 2) - 1
    df['tahun_ajaran'] = df['tahun_akademik'].apply(lambda th: f"{th}/{th+1}")
    df['periode_label'] = df.apply(lambda r: f"Semester {r['label_semester']} {r['tahun_ajaran']}", axis=1)
    
    return df[['id_ta_sumber', 'kode_ta', 'tahun_akademik', 'semester', 'label_semester', 'kuartal', 'tahun_ajaran', 'periode_label']]

@task(name="Load: dim_waktu (DW)")
def load_dim_waktu(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    insert_query = """
        INSERT INTO dim_waktu (id_ta_sumber, kode_ta, tahun_akademik, semester, label_semester, kuartal, tahun_ajaran, periode_label)
        VALUES %s ON CONFLICT (id_ta_sumber) DO UPDATE SET
            kode_ta = EXCLUDED.kode_ta,
            tahun_akademik = EXCLUDED.tahun_akademik,
            semester = EXCLUDED.semester,
            label_semester = EXCLUDED.label_semester,
            periode_label = EXCLUDED.periode_label;
    """
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke dim_waktu.")
    except Exception as e:
        print(f"Gagal memuat data: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Waktu")
def flow_etl_dim_waktu():
    df_src = extract_waktu_source()
    df_ready = transform_dim_waktu(df_src)
    load_dim_waktu(df_ready)

if __name__ == "__main__":
    flow_etl_dim_waktu()