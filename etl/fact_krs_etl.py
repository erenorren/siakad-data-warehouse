# etl/fact_krs_etl.py
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: KRS & Detail Matkul (OLTP)", retries=2)
def extract_krs_source():
    conn_oltp = get_oltp_connection()
    df_krs = pd.read_sql("SELECT id_krs, id_mhs, id_mk, id_ta, id_dosen, status_krs FROM krs;", conn_oltp)
    df_mk = pd.read_sql("SELECT id_mk, sks FROM mata_kuliah;", conn_oltp)
    conn_oltp.close()
    return df_krs, df_mk

@task(name="Transform: Map Keys ke Tabel Fakta KRS")
def transform_fact_krs(df_krs: pd.DataFrame, df_mk: pd.DataFrame) -> pd.DataFrame:
    conn_dw = get_dw_connection()
    waktu_map = dict(pd.read_sql("SELECT id_ta_sumber, sk_waktu FROM dim_waktu;", conn_dw).to_numpy()) if hasattr(psycopg2, 'DataFrame') else dict(pd.read_sql("SELECT id_ta_sumber, sk_waktu FROM dim_waktu;", conn_dw).values)
    
    # Membaca data dimensi pendukung lainnya
    mhs_map = dict(pd.read_sql("SELECT id_mhs_sumber, sk_mahasiswa FROM dim_mahasiswa WHERE is_current = 1;", conn_dw).values)
    mk_df = pd.read_sql("SELECT id_mk_sumber, sk_mk, sk_prodi FROM dim_mata_kuliah;", conn_dw)
    dosen_map = dict(pd.read_sql("SELECT id_dosen_sumber, sk_dosen FROM dim_dosen;", conn_dw).values)
    conn_dw.close()
    
    mk_map = dict(zip(mk_df['id_mk_sumber'], mk_df['sk_mk']))
    mk_prodi_map = dict(zip(mk_df['id_mk_sumber'], mk_df['sk_prodi']))
    sks_map = dict(zip(df_mk['id_mk'], df_mk['sks']))
    
    df_krs['id_krs_sumber'] = df_krs['id_krs'].astype(int)
    df_krs['sk_waktu'] = df_krs['id_ta'].map(waktu_map).fillna(1).astype(int)
    df_krs['sk_mahasiswa'] = df_krs['id_mhs'].map(mhs_map).fillna(1).astype(int)
    df_krs['sk_mk'] = df_krs['id_mk'].map(mk_map).fillna(1).astype(int)
    df_krs['sk_dosen'] = df_krs['id_dosen'].map(dosen_map).fillna(1).astype(int)
    df_krs['sk_prodi'] = df_krs['id_mk'].map(mk_prodi_map).fillna(1).astype(int)
    df_krs['sks_diambil'] = df_krs['id_mk'].map(sks_map).fillna(3).astype(int)
    df_krs['jumlah_krs'] = 1
    df_krs['tgl_load'] = str(datetime.now())
    
    return df_krs[['id_krs_sumber', 'sk_waktu', 'sk_mahasiswa', 'sk_mk', 'sk_dosen', 'sk_prodi', 'sks_diambil', 'status_krs', 'jumlah_krs', 'tgl_load']]

@task(name="Load: fact_krs (DW)")
def load_fact_krs(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        cursor.execute("TRUNCATE TABLE fact_krs RESTART IDENTITY CASCADE;")
        insert_query = """
            INSERT INTO fact_krs (id_krs_sumber, sk_waktu, sk_mahasiswa, sk_mk, sk_dosen, sk_prodi, sks_diambil, status_krs, jumlah_krs, tgl_load)
            VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke fact_krs.")
    except Exception as e:
        print(f"Gagal memuat fakta: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Fact KRS")
def flow_etl_fact_krs():
    df_k, df_m = extract_krs_source()
    df_ready = transform_fact_krs(df_k, df_m)
    load_fact_krs(df_ready)

if __name__ == "__main__":
    flow_etl_fact_krs()