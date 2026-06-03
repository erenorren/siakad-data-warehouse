-- ============================================================
-- SIAKAD OLTP Schema (Sumber Internal / Operational Database)
-- Sistem Informasi Akademik Universitas
-- Dokumentasi struktur tabel operasional (OLTP)
-- Data aktual dibuat oleh: etl/generate_dummy_data.py
-- ============================================================

-- Drop existing tables if any
DROP TABLE IF EXISTS nilai_mahasiswa CASCADE;
DROP TABLE IF EXISTS krs CASCADE;
DROP TABLE IF EXISTS pendaftaran CASCADE;
DROP TABLE IF EXISTS mahasiswa CASCADE;
DROP TABLE IF EXISTS dosen CASCADE;
DROP TABLE IF EXISTS mata_kuliah CASCADE;
DROP TABLE IF EXISTS program_studi CASCADE;
DROP TABLE IF EXISTS fakultas CASCADE;
DROP TABLE IF EXISTS tahun_akademik CASCADE;
DROP TABLE IF EXISTS provinsi CASCADE;

-- ------------------------------------------------------------
-- Master: Provinsi
-- Sumber: generate_dummy_data.py
-- ------------------------------------------------------------
CREATE TABLE provinsi (
    kode_provinsi   VARCHAR(10)  PRIMARY KEY,
    nama_provinsi   VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- Master: Fakultas
-- ------------------------------------------------------------
CREATE TABLE fakultas (
    id_fakultas     SERIAL       PRIMARY KEY,
    kode_fakultas   VARCHAR(10)  NOT NULL UNIQUE,
    nama_fakultas   VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- Master: Program Studi
-- ------------------------------------------------------------
CREATE TABLE program_studi (
    id_prodi        SERIAL       PRIMARY KEY,
    kode_prodi      VARCHAR(10)  NOT NULL UNIQUE,
    nama_prodi      VARCHAR(100) NOT NULL,
    jenjang         VARCHAR(5)   NOT NULL,   -- S1, S2, D3
    id_fakultas     INT          NOT NULL REFERENCES fakultas(id_fakultas),
    akreditasi_prodi CHAR(2)     NOT NULL    -- A, B, C
);

-- ------------------------------------------------------------
-- Master: Tahun Akademik
-- ------------------------------------------------------------
CREATE TABLE tahun_akademik (
    id_ta           SERIAL      PRIMARY KEY,
    kode_ta         VARCHAR(20) NOT NULL UNIQUE,
    tahun_mulai     INT         NOT NULL,
    semester        INT         NOT NULL    -- 1=Ganjil, 2=Genap
);

-- ------------------------------------------------------------
-- Master: Dosen
-- ------------------------------------------------------------
CREATE TABLE dosen (
    id_dosen            SERIAL       PRIMARY KEY,
    nidn                VARCHAR(20)  NOT NULL UNIQUE,
    nama_dosen          VARCHAR(100) NOT NULL,
    id_prodi            INT          NOT NULL REFERENCES program_studi(id_prodi),
    jabatan_akademik    VARCHAR(50),          -- Asisten Ahli, Lektor, dst
    pendidikan_terakhir VARCHAR(5),           -- S2, S3
    tgl_masuk           DATE,
    status_aktif        BOOLEAN      NOT NULL DEFAULT TRUE
);

-- ------------------------------------------------------------
-- Master: Mata Kuliah
-- ------------------------------------------------------------
CREATE TABLE mata_kuliah (
    id_mk           SERIAL       PRIMARY KEY,
    kode_mk         VARCHAR(15)  NOT NULL UNIQUE,
    nama_mk         VARCHAR(100) NOT NULL,
    sks             INT          NOT NULL,
    semester_ke     INT          NOT NULL,   -- semester ke-1 s/d ke-8
    id_prodi        INT          NOT NULL REFERENCES program_studi(id_prodi),
    jenis_mk        VARCHAR(20)  NOT NULL    -- Wajib, Pilihan
);

-- ------------------------------------------------------------
-- Transaksi: Mahasiswa
-- ------------------------------------------------------------
CREATE TABLE mahasiswa (
    id_mhs              SERIAL       PRIMARY KEY,
    nim                 VARCHAR(20)  NOT NULL UNIQUE,
    nama_mhs            VARCHAR(100) NOT NULL,
    jenis_kelamin       CHAR(1)      NOT NULL,   -- L / P
    tgl_lahir           DATE,
    kode_provinsi_asal  VARCHAR(10)  REFERENCES provinsi(kode_provinsi),
    nama_sma_asal       VARCHAR(150),
    id_prodi            INT          NOT NULL REFERENCES program_studi(id_prodi),
    tahun_masuk         INT          NOT NULL,
    status_mhs          VARCHAR(20)  NOT NULL    -- Aktif, Lulus, DO, Cuti
);

-- ------------------------------------------------------------
-- Transaksi: Pendaftaran Mahasiswa Baru
-- ------------------------------------------------------------
CREATE TABLE pendaftaran (
    id_pendaftaran  SERIAL      PRIMARY KEY,
    id_mhs          INT         NOT NULL REFERENCES mahasiswa(id_mhs),
    id_ta           INT         NOT NULL REFERENCES tahun_akademik(id_ta),
    jalur_masuk     VARCHAR(30) NOT NULL,   -- SNBP, SNBT, Mandiri
    tanggal_daftar  DATE        NOT NULL,
    status_diterima BOOLEAN     NOT NULL
);

-- ------------------------------------------------------------
-- Transaksi: Kartu Rencana Studi (KRS)
-- ------------------------------------------------------------
CREATE TABLE krs (
    id_krs      SERIAL      PRIMARY KEY,
    id_mhs      INT         NOT NULL REFERENCES mahasiswa(id_mhs),
    id_mk       INT         NOT NULL REFERENCES mata_kuliah(id_mk),
    id_ta       INT         NOT NULL REFERENCES tahun_akademik(id_ta),
    id_dosen    INT         NOT NULL REFERENCES dosen(id_dosen),
    tanggal_krs DATE        NOT NULL,
    status_krs  VARCHAR(20) NOT NULL    -- Disetujui, Pending, Dibatalkan
);

-- ------------------------------------------------------------
-- Transaksi: Nilai Mahasiswa
-- Tabel fakta utama OLTP (minimal 10.000 baris)
-- ------------------------------------------------------------
CREATE TABLE nilai_mahasiswa (
    id_nilai        SERIAL      PRIMARY KEY,
    id_krs          INT         NOT NULL REFERENCES krs(id_krs),
    nilai_angka     DECIMAL(5,2),
    nilai_huruf     CHAR(2),            -- A, AB, B, BC, C, D, E
    bobot_nilai     DECIMAL(3,1),       -- 4.0, 3.5, 3.0, dst
    ip_semester     DECIMAL(3,2),
    ipk_kumulatif   DECIMAL(3,2),
    tgl_input       DATE        NOT NULL
);

-- ------------------------------------------------------------
-- Data Publik Eksternal: UMP Provinsi (dari BPS)
-- Sumber: data/ump_provinsi.csv
-- Diintegrasikan ke DW melalui ETL, tidak masuk OLTP
-- ------------------------------------------------------------
-- kode_provinsi, nama_provinsi, ump_2023, ump_2024, ump_2025, ump_2026

-- ------------------------------------------------------------
-- Data Publik Eksternal: Akreditasi SMA (dari Kemendikbud)
-- Sumber: data/akreditasi_sma.xlsx
-- Diintegrasikan ke DW melalui ETL, tidak masuk OLTP
-- ------------------------------------------------------------
-- Nama Sekolah, Akreditasi