# etl/fact_pendaftaran_etl.py
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Pendaftaran & Mahasiswa (OLTP)", retries=2)
def extract_pendaftaran_source():
    conn_oltp = get_oltp_connection()
    df_pend = pd.read_sql("SELECT id_pendaftaran, id_mhs, id_ta, jalur_masuk, status_diterima FROM pendaftaran;", conn_oltp)
    df_mhs = pd.read_sql("SELECT id_mhs, id_prodi, kode_provinsi_asal FROM mahasiswa;", conn_oltp)
    conn_oltp.close()
    return df_pend, df_mhs

@task(name="Transform: Map Keys ke Tabel Fakta Pendaftaran")
def transform_fact_pendaftaran(df_pend: pd.DataFrame, df_mhs: pd.DataFrame) -> pd.DataFrame:
    conn_dw = get_dw_connection()
    df_waktu = pd.read_sql("SELECT id_ta_sumber, sk_waktu FROM dim_waktu;", conn_dw)
    df_mhs_dw = pd.read_sql("SELECT id_mhs_sumber, sk_mahasiswa FROM dim_mahasiswa WHERE is_current = 1;", conn_dw)
    df_prodi = pd.read_sql("SELECT id_prodi_sumber, sk_prodi FROM dim_prodi;", conn_dw)
    df_demo = pd.read_sql("SELECT kode_provinsi, sk_demografi FROM dim_demografi_ekonomi;", conn_dw)
    conn_dw.close()
    
    # Buat Dictionary mapping SK
    waktu_map = dict(zip(df_waktu['id_ta_sumber'], df_waktu['sk_waktu']))
    mhs_map = dict(zip(df_mhs_dw['id_mhs_sumber'], df_mhs_dw['sk_mahasiswa']))
    prodi_map = dict(zip(df_prodi['id_prodi_sumber'], df_prodi['sk_prodi']))
    demo_map = dict(zip(df_demo['kode_provinsi'], df_demo['sk_demografi']))
    
    # Gabungkan transaksional OLTP
    df_merged = pd.merge(df_pend, df_mhs, on="id_mhs", how="inner")
    
    df_merged['id_pendaftaran_sumber'] = df_merged['id_pendaftaran'].astype(int)
    df_merged['sk_waktu'] = df_merged['id_ta'].map(waktu_map).fillna(1).astype(int)
    df_merged['sk_mahasiswa'] = df_merged['id_mhs'].map(mhs_map).fillna(1).astype(int)
    df_merged['sk_prodi'] = df_merged['id_prodi'].map(prodi_map).fillna(1).astype(int)
    df_merged['sk_demografi'] = df_merged['kode_provinsi_asal'].map(demo_map).fillna(1).astype(int)
    
    df_merged['status_diterima'] = df_merged['status_diterima'].apply(lambda x: 1 if str(x).lower() in ['true', '1'] else 0)
    df_merged['jumlah_pendaftar'] = 1
    df_merged['tgl_load'] = str(datetime.now())
    
    return df_merged[['id_pendaftaran_sumber', 'sk_waktu', 'sk_mahasiswa', 'sk_prodi', 'sk_demografi', 'jalur_masuk', 'status_diterima', 'jumlah_pendaftar', 'tgl_load']]

@task(name="Load: fact_pendaftaran (DW)")
def load_fact_pendaftaran(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        cursor.execute("TRUNCATE TABLE fact_pendaftaran RESTART IDENTITY CASCADE;")
        insert_query = """
            INSERT INTO fact_pendaftaran (id_pendaftaran_sumber, sk_waktu, sk_mahasiswa, sk_prodi, sk_demografi, jalur_masuk, status_diterima, jumlah_pendaftar, tgl_load)
            VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke fact_pendaftaran.")
    except Exception as e:
        print(f"Gagal memuat fakta: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Fact Pendaftaran")
def flow_etl_fact_pendaftaran():
    df_p, df_m = extract_pendaftaran_source()
    df_ready = transform_fact_pendaftaran(df_p, df_m)
    load_fact_pendaftaran(df_ready)

if __name__ == "__main__":
    flow_etl_fact_pendaftaran()