# etl/pipeline.py
from prefect import flow

# Mengimpor semua sub-flow modular dari berkas tabel masing-masing
from dim_waktu_etl import flow_etl_dim_waktu
from dim_prodi_etl import flow_etl_dim_prodi
from dim_demografi_etl import flow_etl_dim_demografi
from dim_mahasiswa_etl import flow_etl_dim_mahasiswa
from dim_dosen_etl import flow_etl_dim_dosen
from dim_matkul_etl import flow_etl_dim_matkul

from fact_pendaftaran_etl import flow_etl_fact_pendaftaran
from fact_krs_etl import flow_etl_fact_krs
from fact_kelulusan_etl import flow_etl_fact_kelulusan

@flow(name="SIAKAD DW Master Pipeline", description="Pipeline Utama Pengisian Data Warehouse SIAKAD")
def siakad_dw_master_pipeline():
    print("=" * 60)
    print("Memulai Eksekusi Pipeline Utama SIAKAD...")
    print("=" * 60)
    
    # 1. Jalankan master dimensi tingkat pertama (tanpa dependency antar-dimensi)
    print("\n[Tahap 1/3] Memproses Dimensi Master Awal...")
    flow_etl_dim_waktu()
    flow_etl_dim_prodi()
    flow_etl_dim_demografi()
    
    # 2. Jalankan master dimensi tingkat kedua (yang butuh mapping SK dari tahap 1)
    print("\n[Tahap 2/3] Memproses Dimensi Master Dependen...")
    flow_etl_dim_mahasiswa() # Membutuhkan sk_prodi dari dim_prodi
    flow_etl_dim_dosen()     # Membutuhkan sk_prodi dari dim_prodi
    flow_etl_dim_matkul()    # Membutuhkan sk_prodi dari dim_prodi
    
    # 3. Jalankan semua tabel fakta setelah seluruh record master dimensi di DW lengkap
    print("\n[Tahap 3/3] Memproses Seluruh Tabel Fakta (Transactional)...")
    flow_etl_fact_pendaftaran()
    flow_etl_fact_krs()
    flow_etl_fact_kelulusan()
    
    print("\n" + "=" * 60)
    print("PROYEK UAS DATA WAREHOUSE: PIPELINE ETL SELESAI DIEKSEKUSI!")
    print("=" * 60)

if __name__ == "__main__":
    siakad_dw_master_pipeline()