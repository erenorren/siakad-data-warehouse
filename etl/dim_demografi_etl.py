# etl/dim_demografi_etl.py
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import DATA_DIR, get_oltp_connection, get_dw_connection

@task(name="Extract: Provinsi (OLTP) & UMP (CSV)", retries=2)
def extract_demografi_source():
    # Extract OLTP
    conn_oltp = get_oltp_connection()
    df_prov = pd.read_sql("SELECT kode_provinsi, nama_provinsi FROM provinsi;", conn_oltp)
    conn_oltp.close()
    
    # Extract CSV Eksternal
    df_ump = pd.read_csv(DATA_DIR / 'ump_provinsi.csv')
    return df_prov, df_ump

# etl/dim_demografi_etl.py

@task(name="Transform: Gabung & Kategori Ekonomi")
def transform_dim_demografi(df_prov: pd.DataFrame, df_ump: pd.DataFrame) -> pd.DataFrame:
    print("Mentransformasi data demografi ekonomi...")
    
    # 1. Samakan tipe data (Langkah dari sesi sebelumnya)
    df_prov['kode_provinsi'] = df_prov['kode_provinsi'].astype(str)
    df_ump['kode_provinsi'] = df_ump['kode_provinsi'].astype(str)
    
    # 2. HAPUS kolom nama_provinsi dari CSV agar tidak bentrok (_x dan _y) saat merge
    if 'nama_provinsi' in df_ump.columns:
        df_ump = df_ump.drop(columns=['nama_provinsi'])
        
    # 3. Jalankan merge (sekarang nama_provinsi murni hanya milik df_prov)
    df_merged = pd.merge(df_prov, df_ump, on="kode_provinsi", how="left")
    
    # Isi nilai kosong dengan 0 jika ada provinsi yang tidak terdata di CSV
    df_merged['ump_2026'] = df_merged['ump_2026'].fillna(0).astype(int)
    
    # Terapkan aturan bisnis untuk kategori ekonomi
    def tentukan_kategori(ump):
        if ump >= 3500000: return 'Tinggi'
        elif ump >= 2500000: return 'Menengah'
        return 'Rendah'
        
    df_merged['kategori_ekonomi'] = df_merged['ump_2026'].apply(tentukan_kategori)
    df_merged['ump_terbaru'] = df_merged['ump_2026']
    df_merged['tahun_ump_terbaru'] = 2026
    df_merged['sumber_data'] = 'BPS Indonesia'
    df_merged['tgl_update_dw'] = str(date.today())
    
    return df_merged[[
        'kode_provinsi', 'nama_provinsi', 'ump_2023', 'ump_2024', 
        'ump_2025', 'ump_2026', 'ump_terbaru', 'tahun_ump_terbaru', 
        'kategori_ekonomi', 'sumber_data', 'tgl_update_dw'
    ]]

@task(name="Load: dim_demografi_ekonomi (DW)")
def load_dim_demografi(df: pd.DataFrame):
    if df.empty:
        print("Data kosong. Melewati proses Load.")
        return
        
    data_tuples = [tuple(x) for x in df.to_numpy()]
    insert_query = """
        INSERT INTO dim_demografi_ekonomi (
            kode_provinsi, nama_provinsi, ump_2023, ump_2024, ump_2025, 
            ump_2026, ump_terbaru, tahun_ump_terbaru, kategori_ekonomi, sumber_data, tgl_update_dw
        ) VALUES %s ON CONFLICT (kode_provinsi) DO UPDATE SET
            nama_provinsi = EXCLUDED.nama_provinsi,
            ump_2026 = EXCLUDED.ump_2026,
            kategori_ekonomi = EXCLUDED.kategori_ekonomi,
            tgl_update_dw = EXCLUDED.tgl_update_dw;
    """
    
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print(f"Berhasil memuat {len(df)} baris ke dim_demografi_ekonomi.")
    except Exception as e:
        print(f"Terjadi error saat Load: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Demografi")
def flow_etl_dim_demografi():
    df_prov, df_ump = extract_demografi_source()
    df_transformed = transform_dim_demografi(df_prov, df_ump)
    load_dim_demografi(df_transformed)

if __name__ == "__main__":
    flow_etl_dim_demografi()