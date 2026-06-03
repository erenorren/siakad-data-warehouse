# etl/dim_dosen_etl.py
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Dosen (OLTP)", retries=2)
def extract_dosen_source() -> pd.DataFrame:
    conn_oltp = get_oltp_connection()
    df = pd.read_sql("SELECT id_dosen, nidn, nama_dosen, jabatan_akademik, pendidikan_terakhir, id_prodi, status_aktif FROM dosen;", conn_oltp)
    conn_oltp.close()
    return df

@task(name="Transform: SCD Type 1 Dosen & Map SK Prodi")
def transform_dim_dosen(df_dosen: pd.DataFrame) -> pd.DataFrame:
    conn_dw = get_dw_connection()
    df_prodi_dw = pd.read_sql("SELECT id_prodi_sumber, sk_prodi FROM dim_prodi;", conn_dw)
    conn_dw.close()
    
    prodi_map = dict(zip(df_prodi_dw['id_prodi_sumber'], df_prodi_dw['sk_prodi']))
    
    df_dosen['id_dosen_sumber'] = df_dosen['id_dosen'].astype(int)
    df_dosen['sk_prodi'] = df_dosen['id_prodi'].map(prodi_map).fillna(1).astype(int)
    df_dosen['status_aktif'] = df_dosen['status_aktif'].apply(lambda x: 1 if str(x).lower() in ['true', '1', 'aktif'] else 0)
    df_dosen['tgl_update_dw'] = str(date.today())
    
    return df_dosen[['id_dosen_sumber', 'nidn', 'nama_dosen', 'jabatan_akademik', 'pendidikan_terakhir', 'sk_prodi', 'status_aktif', 'tgl_update_dw']]

@task(name="Load: dim_dosen (DW - SCD Type 1)")
def load_dim_dosen(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    # Logika SCD Type 1: Timpa langsung data lama jika ada kecocokan Natural Key
    insert_query = """
        INSERT INTO dim_dosen (id_dosen_sumber, nidn, nama_dosen, jabatan_akademik, pendidikan_terakhir, sk_prodi, status_aktif, tgl_update_dw)
        VALUES %s ON CONFLICT (id_dosen_sumber) DO UPDATE SET
            nidn = EXCLUDED.nidn,
            nama_dosen = EXCLUDED.nama_dosen,
            jabatan_akademik = EXCLUDED.jabatan_akademik,
            pendidikan_terakhir = EXCLUDED.pendidikan_terakhir,
            sk_prodi = EXCLUDED.sk_prodi,
            status_aktif = EXCLUDED.status_aktif,
            tgl_update_dw = EXCLUDED.tgl_update_dw;
    """
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat/memperbarui {len(df)} baris di dim_dosen (SCD1).")
    except Exception as e:
        print(f"Gagal memuat data: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Dosen")
def flow_etl_dim_dosen():
    df_src = extract_dosen_source()
    df_ready = transform_dim_dosen(df_src)
    load_dim_dosen(df_ready)

if __name__ == "__main__":
    flow_etl_dim_dosen()