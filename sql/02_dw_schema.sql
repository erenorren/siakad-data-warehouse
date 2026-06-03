-- ============================================================
-- SIAKAD Data Warehouse Schema (Star Schema)
-- Sistem Informasi Akademik Universitas
-- Dokumentasi struktur tabel DW
-- Database aktual dibuat oleh: etl/etl_pipeline.py
-- Engine: SQLite (siakad_dw.db)
-- ============================================================

-- Aktifkan fitur Foreign Key di SQLite
PRAGMA foreign_keys = ON;

-- Drop existing DW tables jika ada (urut dari fakta ke dimensi)
DROP TABLE IF EXISTS fact_kelulusan;
DROP TABLE IF EXISTS fact_krs;
DROP TABLE IF EXISTS fact_pendaftaran;
DROP TABLE IF EXISTS dim_dosen;
DROP TABLE IF EXISTS dim_mata_kuliah;
DROP TABLE IF EXISTS dim_mahasiswa;
DROP TABLE IF EXISTS dim_prodi;
DROP TABLE IF EXISTS dim_demografi_ekonomi;
DROP TABLE IF EXISTS dim_waktu;

-- ============================================================
-- DIMENSI
-- ============================================================

-- ------------------------------------------------------------
-- dim_waktu
-- Sumber: tabel tahun_akademik (OLTP)
-- ------------------------------------------------------------
CREATE TABLE dim_waktu (
    sk_waktu        INTEGER PRIMARY KEY AUTOINCREMENT,
    id_ta_sumber    INT,
    kode_ta         TEXT,
    tahun_akademik  INT,
    semester        INT,        -- 1=Ganjil, 2=Genap
    label_semester  TEXT,       -- 'Ganjil' / 'Genap'
    kuartal         INT,
    tahun_ajaran    TEXT,       -- contoh: '2023/2024'
    periode_label   TEXT        -- contoh: 'Semester Ganjil 2023/2024'
);

-- ------------------------------------------------------------
-- dim_prodi
-- Sumber: tabel program_studi + fakultas (OLTP)
-- ------------------------------------------------------------
CREATE TABLE dim_prodi (
    sk_prodi        INTEGER PRIMARY KEY AUTOINCREMENT,
    id_prodi_sumber INT,
    kode_prodi      TEXT,
    nama_prodi      TEXT,
    jenjang         TEXT,       -- S1, S2, D3
    kode_fakultas   TEXT,
    nama_fakultas   TEXT,
    akreditasi_prodi TEXT
);

-- ------------------------------------------------------------
-- dim_demografi_ekonomi
-- Sumber: data/ump_provinsi.csv (Data Publik BPS)
-- SCD Type 0 (referensi statis, diupdate manual)
-- ------------------------------------------------------------
CREATE TABLE dim_demografi_ekonomi (
    sk_demografi        INTEGER PRIMARY KEY AUTOINCREMENT,
    kode_provinsi       TEXT UNIQUE,
    nama_provinsi       TEXT,
    ump_2023            INT,            -- UMP tahun 2023 (Rupiah)
    ump_2024            INT,            -- UMP tahun 2024 (Rupiah)
    ump_2025            INT,            -- UMP tahun 2025 (Rupiah)
    ump_2026            INT,            -- UMP tahun 2026 (Rupiah)
    ump_terbaru         INT,            -- = ump_2026
    tahun_ump_terbaru   INT,            -- = 2026
    kategori_ekonomi    TEXT,           -- 'Tinggi' / 'Menengah' / 'Rendah'
    sumber_data         TEXT,           -- 'BPS Indonesia'
    tgl_update_dw       TEXT
);

-- ------------------------------------------------------------
-- dim_mahasiswa
-- Sumber: tabel mahasiswa (OLTP) + akreditasi_sma.xlsx (Publik)
-- SCD Type 2: perubahan status_mhs menghasilkan record baru
-- ------------------------------------------------------------
CREATE TABLE dim_mahasiswa (
    sk_mahasiswa        INTEGER PRIMARY KEY,
    id_mhs_sumber       INT,
    nim                 TEXT,
    nama_mhs            TEXT,
    jenis_kelamin       TEXT,
    kode_provinsi_asal  TEXT,
    nama_provinsi_asal  TEXT,
    nama_sma_asal       TEXT,
    akreditasi_sma_asal TEXT,   -- A, B, C, TT (Hasil enrichment dari akreditasi_sma.xlsx)
    sk_prodi            INT REFERENCES dim_prodi(sk_prodi),
    tahun_masuk         INT,
    status_mhs          TEXT,   -- Aktif, Lulus, DO, Cuti
    -- SCD Type 2 columns
    tgl_efektif         TEXT,   -- tanggal record ini mulai berlaku
    tgl_kadaluarsa      TEXT,   -- '9999-12-31' jika masih aktif
    is_current          INT     -- 1=aktif, 0=historis
);

-- ------------------------------------------------------------
-- dim_dosen
-- Sumber: tabel dosen (OLTP)
-- SCD Type 1: update langsung, tidak menyimpan historis
-- ------------------------------------------------------------
CREATE TABLE dim_dosen (
    sk_dosen            INTEGER PRIMARY KEY,
    id_dosen_sumber     INT,
    nidn                TEXT,
    nama_dosen          TEXT,
    jabatan_akademik    TEXT,
    pendidikan_terakhir TEXT,
    sk_prodi            INT REFERENCES dim_prodi(sk_prodi),
    status_aktif        INT,    -- 1=aktif, 0=tidak aktif
    tgl_update_dw       TEXT
);

-- ------------------------------------------------------------
-- dim_mata_kuliah
-- Sumber: tabel mata_kuliah (OLTP)
-- ------------------------------------------------------------
CREATE TABLE dim_mata_kuliah (
    sk_mk           INTEGER PRIMARY KEY,
    id_mk_sumber    INT,
    kode_mk         TEXT,
    nama_mk         TEXT,
    sks             INT,
    semester_ke     INT,
    jenis_mk        TEXT,       -- Wajib / Pilihan
    sk_prodi        INT REFERENCES dim_prodi(sk_prodi)
);

-- ============================================================
-- TABEL FAKTA
-- ============================================================

-- ------------------------------------------------------------
-- fact_pendaftaran
-- Grain: 1 baris = 1 pendaftaran mahasiswa baru
-- ------------------------------------------------------------
CREATE TABLE fact_pendaftaran (
    sk_pendaftaran          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_pendaftaran_sumber   INT,
    sk_waktu                INT REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa            INT REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_prodi                INT REFERENCES dim_prodi(sk_prodi),
    sk_demografi            INT REFERENCES dim_demografi_ekonomi(sk_demografi),
    jalur_masuk             TEXT,   -- SNBP, SNBT, Mandiri
    status_diterima         INT,    -- 1=diterima, 0=tidak
    jumlah_pendaftar        INT,    -- selalu 1 (untuk agregasi)
    tgl_load                TEXT
);

-- ------------------------------------------------------------
-- fact_krs
-- Grain: 1 baris = 1 pengambilan mata kuliah per mahasiswa per semester
-- ------------------------------------------------------------
CREATE TABLE fact_krs (
    sk_krs          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_krs_sumber   INT,
    sk_waktu                INT REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa            INT REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_mk                   INT REFERENCES dim_mata_kuliah(sk_mk),
    sk_dosen                INT REFERENCES dim_dosen(sk_dosen),
    sk_prodi                INT REFERENCES dim_prodi(sk_prodi),
    sks_diambil     INT,
    status_krs      TEXT,   -- Disetujui, Pending, Dibatalkan
    jumlah_krs      INT,    -- selalu 1 (untuk agregasi)
    tgl_load        TEXT
);

-- ------------------------------------------------------------
-- fact_kelulusan
-- Grain: 1 baris = 1 nilai mata kuliah per mahasiswa per semester
-- ------------------------------------------------------------
CREATE TABLE fact_kelulusan (
    sk_kelulusan    INTEGER PRIMARY KEY AUTOINCREMENT,
    id_nilai_sumber INT,
    sk_waktu                INT REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa            INT REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_mk                   INT REFERENCES dim_mata_kuliah(sk_mk),
    sk_dosen                INT REFERENCES dim_dosen(sk_dosen),
    sk_prodi                INT REFERENCES dim_prodi(sk_prodi),
    sk_demografi            INT REFERENCES dim_demografi_ekonomi(sk_demografi),
    nilai_angka     REAL,
    nilai_huruf     TEXT,   -- A, AB, B, BC, C, D, E
    bobot_nilai     REAL,   -- 4.0, 3.5, 3.0, dst
    ip_semester     REAL,
    ipk_kumulatif   REAL,
    sks_mk          INT,
    lulus_flag      INT,    -- 1=lulus (bobot >= 1.0), 0=tidak
    tepat_waktu_flag INT,   -- 1=lulus <= 8 semester, 0=tidak
    tgl_load        TEXT
);

-- ============================================================
-- CATATAN IMPLEMENTASI SCD
-- ============================================================
-- dim_mahasiswa : SCD Type 2
--   Trigger     : perubahan kolom status_mhs
--   Mekanisme   : record lama di-set is_current=0, tgl_kadaluarsa=today
--                 record baru dibuat dengan tgl_efektif=today, is_current=1
--
-- dim_dosen     : SCD Type 1
--   Trigger     : perubahan data dosen (jabatan, pendidikan, dll)
--   Mekanisme   : UPDATE langsung, tidak ada historis
-- ============================================================