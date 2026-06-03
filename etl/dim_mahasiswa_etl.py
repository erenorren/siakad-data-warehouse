# etl/dim_mahasiswa_etl.py
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from config import DATA_DIR, get_oltp_connection, get_dw_connection

@task(name="Extract: Mahasiswa OLTP & Excel Akreditasi")
def extract_mahasiswa_source():
    conn_oltp = get_oltp_connection()
    df_mhs = pd.read_sql("SELECT * FROM mahasiswa;", conn_oltp)
    conn_oltp.close()
    
    df_akreditasi = pd.read_excel(DATA_DIR / "akreditasi_sma.xlsx")
    return df_mhs, df_akreditasi

@task(name="Extract Target: Current dim_mahasiswa (DWH)")
def extract_current_dim_mhs() -> pd.DataFrame:
    # Mengambil data yang aktif saat ini untuk pengecekan histori SCD Type 2
    query = "SELECT id_mhs_sumber, status_mhs FROM dim_mahasiswa WHERE is_current = 1;"
    conn_dw = get_dw_connection()
    df_existing = pd.read_sql(query, conn_dw)
    conn_dw.close()
    return df_existing

@task(name="Transform & Handle SCD Type 2 Mahasiswa")
def transform_scd2_mahasiswa(df_mhs: pd.DataFrame, df_akreditasi: pd.DataFrame, df_existing: pd.DataFrame):
    print("Memproses logika SCD Type 2 untuk data mahasiswa...")
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    today = str(date.today())
    
    # Get SK Prodi mapping dari DW
    df_prodi_dw = pd.read_sql("SELECT id_prodi_sumber, sk_prodi FROM dim_prodi;", conn_dw)
    prodi_map = dict(zip(df_prodi_dw['id_prodi_sumber'], df_prodi_dw['sk_prodi']))
    
    # Bersihkan nama sekolah agar proses join string aman
    df_mhs['nama_sma_asal'] = df_mhs['nama_sma_asal'].str.strip()
    df_akreditasi['Nama Sekolah'] = df_akreditasi['Nama Sekolah'].str.strip()
    akreditasi_map = dict(zip(df_akreditasi['Nama Sekolah'], df_akreditasi['Akreditasi']))
    
    # Buat map data eksisting dari DWH {id_mhs_sumber: status_mhs}
    existing_map = dict(zip(df_existing['id_mhs_sumber'], df_existing['status_mhs']))
    
    new_records = []
    
    for _, row in df_mhs.iterrows():
        id_src = int(row['id_mhs'])
        status_now = row['status_mhs']
        sk_prodi = prodi_map.get(int(row['id_prodi']), None)
        akreditasi_sma = akreditasi_map.get(row['nama_sma_asal'], 'Tidak Terakreditasi')
        
        # Penanganan SCD Type 2
        if id_src in existing_map:
            status_old = existing_map[id_src]
            if status_old != status_now:
                # 1. Update data lama: set expired dan nonaktifkan
                update_query = """
                    UPDATE dim_mahasiswa 
                    SET tgl_kadaluarsa = %s, is_current = 0 
                    WHERE id_mhs_sumber = %s AND is_current = 1;
                """
                cursor.execute(update_query, (today, id_src))
                print(f"SCD2: Perubahan status terdeteksi untuk ID {id_src} ({status_old} -> {status_now}). Record lama ditutup.")
                
                # 2. Siapkan baris baru untuk di-insert
                new_records.append((
                    id_src, row['nim'], row['nama_mhs'], row['jenis_kelamin'],
                    row['kode_provinsi_asal'], row['nama_sma_asal'], akreditasi_sma,
                    sk_prodi, int(row['tahun_masuk']), status_now, today, '9999-12-31', 1
                ))
        else:
            # Jika benar-benar data baru (belum ada di DW)
            new_records.append((
                id_src, row['nim'], row['nama_mhs'], row['jenis_kelamin'],
                row['kode_provinsi_asal'], row['nama_sma_asal'], akreditasi_sma,
                sk_prodi, int(row['tahun_masuk']), status_now, today, '9999-12-31', 1
            ))
            
    conn_dw.commit()
    cursor.close()
    conn_dw.close()
    return new_records

@task(name="Load: New Records to dim_mahasiswa")
def load_dim_mahasiswa(new_records: list):
    if not new_records:
        print("Tidak ada perubahan status atau data mahasiswa baru.")
        return
        
    insert_query = """
        INSERT INTO dim_mahasiswa (
            id_mhs_sumber, nim, nama_mhs, jenis_kelamin, kode_provinsi_asal,
            nama_sma_asal, akreditasi_sma_asal, sk_prodi, tahun_masuk, status_mhs,
            tgl_efektif, tgl_kadaluarsa, is_current
        ) VALUES %s;
    """
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    try:
        execute_values(cursor, insert_query, new_records)
        conn_dw.commit()
        print(f"Berhasil memuat {len(new_records)} baris data baru/perubahan status ke dim_mahasiswa.")
    except Exception as e:
        print(f"Gagal memuat data ke DW: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Mahasiswa (SCD2)")
def flow_etl_dim_mahasiswa():
    df_mhs, df_akred = extract_mahasiswa_source()
    df_existing = extract_current_dim_mhs()
    new_records = transform_scd2_mahasiswa(df_mhs, df_akred, df_existing)
    load_dim_mahasiswa(new_records)

if __name__ == "__main__":
    flow_etl_dim_mahasiswa()