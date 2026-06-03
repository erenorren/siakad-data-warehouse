# etl/fact_kelulusan_etl.py
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import get_oltp_connection, get_dw_connection

@task(name="Extract: Nilai & Data Pendukung (OLTP)", retries=2)
def extract_kelulusan_source():
    conn_oltp = get_oltp_connection()
    df_nilai = pd.read_sql("SELECT id_nilai, id_krs, nilai_angka, nilai_huruf, bobot_nilai, ip_semester, ipk_kumulatif FROM nilai_mahasiswa;", conn_oltp)
    df_krs = pd.read_sql("SELECT id_krs, id_mhs, id_mk, id_ta, id_dosen FROM krs;", conn_oltp)
    df_mhs = pd.read_sql("SELECT id_mhs, kode_provinsi_asal, status_mhs FROM mahasiswa;", conn_oltp)
    df_mk = pd.read_sql("SELECT id_mk, sks FROM mata_kuliah;", conn_oltp)
    conn_oltp.close()
    return df_nilai, df_krs, df_mhs, df_mk

@task(name="Transform: Hitung Tepat Waktu & Map Keys")
def transform_fact_kelulusan(df_nilai: pd.DataFrame, df_krs: pd.DataFrame, df_mhs: pd.DataFrame, df_mk: pd.DataFrame) -> pd.DataFrame:
    # Ambil data kunci dari DW
    conn_dw = get_dw_connection()
    waktu_map = dict(pd.read_sql("SELECT id_ta_sumber, sk_waktu FROM dim_waktu;", conn_dw).values)
    mhs_map = dict(pd.read_sql("SELECT id_mhs_sumber, sk_mahasiswa FROM dim_mahasiswa WHERE is_current = 1;", conn_dw).values)
    mk_df = pd.read_sql("SELECT id_mk_sumber, sk_mk, sk_prodi FROM dim_mata_kuliah;", conn_dw)
    dosen_map = dict(pd.read_sql("SELECT id_dosen_sumber, sk_dosen FROM dim_dosen;", conn_dw).values)
    demo_map = dict(pd.read_sql("SELECT kode_provinsi, sk_demografi FROM dim_demografi_ekonomi;", conn_dw).values)
    conn_dw.close()
    
    mk_map = dict(zip(mk_df['id_mk_sumber'], mk_df['sk_mk']))
    mk_prodi_map = dict(zip(mk_df['id_mk_sumber'], mk_df['sk_prodi']))
    sks_mk_map = dict(zip(df_mk['id_mk'], df_mk['sks']))
    mhs_status_map = dict(zip(df_mhs['id_mhs'], df_mhs['status_mhs']))
    mhs_prov_map = dict(zip(df_mhs['id_mhs'], df_mhs['kode_provinsi_asal']))
    
    # Hitung jumlah total semester aktif yang pernah diambil mahasiswa di KRS
    mhs_smt_count = df_krs.groupby('id_mhs')['id_ta'].nunique().to_dict()
    
    # Gabung data transaksional
    df_merged = pd.merge(df_nilai, df_krs, on="id_krs", how="inner")
    
    facts = []
    for _, row in df_merged.iterrows():
        id_mhs = int(row['id_mhs'])
        id_mk = int(row['id_mk'])
        bobot = float(row['bobot_nilai']) if row['bobot_nilai'] else 0.0
        
        lulus_flag = 1 if bobot >= 1.0 else 0
        
        # Logika bendera KPI Kelulusan Tepat Waktu (Lulus <= 8 semester)
        n_smt = mhs_smt_count.get(id_mhs, 0)
        status_mhs = mhs_status_map.get(id_mhs, '')
        tepat_waktu_flag = 1 if (n_smt <= 8 and status_mhs.strip().lower() == 'lulus') else 0
        
        facts.append((
            int(row['id_nilai']),
            waktu_map.get(int(row['id_ta']), 1),
            mhs_map.get(id_mhs, 1),
            mk_map.get(id_mk, 1),
            dosen_map.get(int(row['id_dosen']), 1),
            mk_prodi_map.get(id_mk, 1),
            demo_map.get(mhs_prov_map.get(id_mhs, ''), 1),
            float(row['nilai_angka']) if row['nilai_angka'] else None,
            row['nilai_huruf'], bobot,
            float(row['ip_semester']) if row['ip_semester'] else None,
            float(row['ipk_kumulatif']) if row['ipk_kumulatif'] else None,
            sks_mk_map.get(id_mk, 3),
            lulus_flag, tepat_waktu_flag,
            str(datetime.now())
        ))
        
    return pd.DataFrame(facts, columns=[
        'id_nilai_sumber', 'sk_waktu', 'sk_mahasiswa', 'sk_mk', 'sk_dosen', 
        'sk_prodi', 'sk_demografi', 'nilai_angka', 'nilai_huruf', 'bobot_nilai', 
        'ip_semester', 'ipk_kumulatif', 'sks_mk', 'lulus_flag', 'tepat_waktu_flag', 'tgl_load'
    ])

@task(name="Load: fact_kelulusan (DW)")
def load_fact_kelulusan(df: pd.DataFrame):
    if df.empty: return
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        cursor.execute("TRUNCATE TABLE fact_kelulusan RESTART IDENTITY CASCADE;")
        insert_query = """
            INSERT INTO fact_kelulusan (
                id_nilai_sumber, sk_waktu, sk_mahasiswa, sk_mk, sk_dosen, sk_prodi, sk_demografi,
                nilai_angka, nilai_huruf, bobot_nilai, ip_semester, ipk_kumulatif, sks_mk,
                lulus_flag, tepat_waktu_flag, tgl_load
            ) VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke fact_kelulusan untuk analisis 4 KPI.")
    except Exception as e:
        print(f"Gagal memuat fakta kelulusan: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Fact Kelulusan")
def flow_etl_fact_kelulusan():
    df_n, df_k, df_m, df_mk = extract_kelulusan_source()
    df_ready = transform_fact_kelulusan(df_n, df_k, df_m, df_mk)
    load_fact_kelulusan(df_ready)

if __name__ == "__main__":
    flow_etl_fact_kelulusan()