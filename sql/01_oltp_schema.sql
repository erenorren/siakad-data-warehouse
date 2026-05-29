-- ============================================================
-- SIAKAD OLTP Schema (Sumber Internal / Operational Database)
-- Sistem Informasi Akademik Universitas
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
-- ------------------------------------------------------------
CREATE TABLE provinsi (
    kode_provinsi   CHAR(2)      PRIMARY KEY,
    nama_provinsi   VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- Master: Fakultas
-- ------------------------------------------------------------
CREATE TABLE fakultas (
    id_fakultas   SERIAL       PRIMARY KEY,
    kode_fakultas CHAR(3)      UNIQUE NOT NULL,
    nama_fakultas VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- Master: Program Studi
-- ------------------------------------------------------------
CREATE TABLE program_studi (
    id_prodi   SERIAL       PRIMARY KEY,
    kode_prodi VARCHAR(10)  UNIQUE NOT NULL,
    nama_prodi VARCHAR(100) NOT NULL,
    jenjang    CHAR(2)      NOT NULL CHECK (jenjang IN ('D3','S1','S2','S3')),
    id_fakultas INT         NOT NULL REFERENCES fakultas(id_fakultas),
    akreditasi_prodi CHAR(1) DEFAULT 'B'
);

-- ------------------------------------------------------------
-- Master: Tahun Akademik
-- ------------------------------------------------------------
CREATE TABLE tahun_akademik (
    id_ta       SERIAL      PRIMARY KEY,
    kode_ta     VARCHAR(10) UNIQUE NOT NULL,  -- e.g. '2021/2022-1'
    tahun_mulai INT         NOT NULL,
    semester    SMALLINT    NOT NULL CHECK (semester IN (1,2)),
    is_aktif    BOOLEAN     DEFAULT FALSE
);

-- ------------------------------------------------------------
-- Master: Dosen
-- ------------------------------------------------------------
CREATE TABLE dosen (
    id_dosen     SERIAL       PRIMARY KEY,
    nidn         VARCHAR(20)  UNIQUE NOT NULL,
    nama_dosen   VARCHAR(150) NOT NULL,
    id_prodi     INT          NOT NULL REFERENCES program_studi(id_prodi),
    jabatan_akademik VARCHAR(30) DEFAULT 'Asisten Ahli',
    pendidikan_terakhir CHAR(2) DEFAULT 'S2',
    tgl_masuk    DATE,
    status_aktif BOOLEAN DEFAULT TRUE
);

-- ------------------------------------------------------------
-- Master: Mata Kuliah
-- ------------------------------------------------------------
CREATE TABLE mata_kuliah (
    id_mk      SERIAL       PRIMARY KEY,
    kode_mk    VARCHAR(10)  UNIQUE NOT NULL,
    nama_mk    VARCHAR(150) NOT NULL,
    sks        SMALLINT     NOT NULL CHECK (sks BETWEEN 1 AND 6),
    semester_ke SMALLINT    NOT NULL CHECK (semester_ke BETWEEN 1 AND 8),
    id_prodi   INT          NOT NULL REFERENCES program_studi(id_prodi),
    jenis_mk   VARCHAR(20)  DEFAULT 'Wajib' CHECK (jenis_mk IN ('Wajib','Pilihan'))
);

-- ------------------------------------------------------------
-- Transaksi: Mahasiswa
-- ------------------------------------------------------------
CREATE TABLE mahasiswa (
    id_mhs       SERIAL       PRIMARY KEY,
    nim          VARCHAR(20)  UNIQUE NOT NULL,
    nama_mhs     VARCHAR(150) NOT NULL,
    jenis_kelamin CHAR(1)     NOT NULL CHECK (jenis_kelamin IN ('L','P')),
    tgl_lahir    DATE,
    kode_provinsi_asal CHAR(2) REFERENCES provinsi(kode_provinsi),
    nama_sma_asal      VARCHAR(200),
    akreditasi_sma_asal CHAR(2) DEFAULT 'B',  -- A, B, C, atau TT (Tidak Terakreditasi)
    id_prodi     INT          NOT NULL REFERENCES program_studi(id_prodi),
    tahun_masuk  INT          NOT NULL,
    status_mhs   VARCHAR(20)  DEFAULT 'Aktif'
        CHECK (status_mhs IN ('Aktif','Cuti','DO','Lulus','Mengundurkan Diri'))
);

-- ------------------------------------------------------------
-- Transaksi: Pendaftaran (Penerimaan Mahasiswa Baru)
-- ------------------------------------------------------------
CREATE TABLE pendaftaran (
    id_pendaftaran SERIAL    PRIMARY KEY,
    id_mhs         INT       NOT NULL REFERENCES mahasiswa(id_mhs),
    id_ta          INT       NOT NULL REFERENCES tahun_akademik(id_ta),
    jalur_masuk    VARCHAR(30) NOT NULL
        CHECK (jalur_masuk IN ('SNBP','SNBT','Mandiri','Beasiswa')),
    tanggal_daftar DATE      NOT NULL,
    status_diterima BOOLEAN  DEFAULT TRUE,
    UNIQUE(id_mhs, id_ta)
);

-- ------------------------------------------------------------
-- Transaksi: KRS (Kartu Rencana Studi)
-- ------------------------------------------------------------
CREATE TABLE krs (
    id_krs     SERIAL   PRIMARY KEY,
    id_mhs     INT      NOT NULL REFERENCES mahasiswa(id_mhs),
    id_mk      INT      NOT NULL REFERENCES mata_kuliah(id_mk),
    id_ta      INT      NOT NULL REFERENCES tahun_akademik(id_ta),
    id_dosen   INT      NOT NULL REFERENCES dosen(id_dosen),
    tanggal_krs DATE    NOT NULL,
    status_krs VARCHAR(15) DEFAULT 'Disetujui'
        CHECK (status_krs IN ('Disetujui','Menunggu','Dibatalkan')),
    UNIQUE(id_mhs, id_mk, id_ta)
);

-- ------------------------------------------------------------
-- Transaksi: Nilai Mahasiswa
-- ------------------------------------------------------------
CREATE TABLE nilai_mahasiswa (
    id_nilai    SERIAL   PRIMARY KEY,
    id_krs      INT      NOT NULL REFERENCES krs(id_krs),
    nilai_angka NUMERIC(5,2),
    nilai_huruf CHAR(2),
    bobot_nilai NUMERIC(3,2),  -- 4.0, 3.7, 3.3, dst
    ip_semester NUMERIC(4,2),  -- IP semester (dihitung per mahasiswa per semester)
    ipk_kumulatif NUMERIC(4,2),
    tgl_input   DATE    DEFAULT CURRENT_DATE,
    UNIQUE(id_krs)
);

-- Index untuk performa query
CREATE INDEX idx_mhs_prodi      ON mahasiswa(id_prodi);
CREATE INDEX idx_mhs_provinsi   ON mahasiswa(kode_provinsi_asal);
CREATE INDEX idx_krs_mhs        ON krs(id_mhs);
CREATE INDEX idx_krs_ta         ON krs(id_ta);
CREATE INDEX idx_nilai_krs      ON nilai_mahasiswa(id_krs);
CREATE INDEX idx_pendaftaran_ta ON pendaftaran(id_ta);
