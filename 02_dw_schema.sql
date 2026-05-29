-- ============================================================
-- SIAKAD Data Warehouse Schema (Star Schema)
-- Sistem Informasi Akademik Universitas
-- ============================================================

-- Drop existing DW tables
DROP TABLE IF EXISTS fact_kelulusan CASCADE;
DROP TABLE IF EXISTS fact_krs CASCADE;
DROP TABLE IF EXISTS fact_pendaftaran CASCADE;
DROP TABLE IF EXISTS dim_dosen CASCADE;
DROP TABLE IF EXISTS dim_mata_kuliah CASCADE;
DROP TABLE IF EXISTS dim_prodi CASCADE;
DROP TABLE IF EXISTS dim_mahasiswa CASCADE;
DROP TABLE IF EXISTS dim_waktu CASCADE;
DROP TABLE IF EXISTS dim_demografi_ekonomi CASCADE;

-- ============================================================
-- DIMENSI
-- ============================================================

-- ------------------------------------------------------------
-- Dim Waktu (Time Dimension)
-- ------------------------------------------------------------
CREATE TABLE dim_waktu (
    sk_waktu       SERIAL       PRIMARY KEY,
    id_ta_sumber   INT,                          -- NK dari OLTP
    kode_ta        VARCHAR(10)  NOT NULL,
    tahun_akademik INT          NOT NULL,
    semester       SMALLINT     NOT NULL,
    label_semester VARCHAR(20),                  -- 'Ganjil' / 'Genap'
    kuartal        SMALLINT,
    tahun_ajaran   VARCHAR(10),                  -- '2021/2022'
    periode_label  VARCHAR(30)                   -- 'Semester Ganjil 2021/2022'
);

-- ------------------------------------------------------------
-- Dim Prodi (Program Studi Dimension)
-- ------------------------------------------------------------
CREATE TABLE dim_prodi (
    sk_prodi       SERIAL       PRIMARY KEY,
    id_prodi_sumber INT,                         -- NK dari OLTP
    kode_prodi     VARCHAR(10)  NOT NULL,
    nama_prodi     VARCHAR(100) NOT NULL,
    jenjang        CHAR(2)      NOT NULL,
    kode_fakultas  CHAR(3),
    nama_fakultas  VARCHAR(100),
    akreditasi_prodi CHAR(1)
);

-- ------------------------------------------------------------
-- Dim Mahasiswa (SCD Type 2 – status berubah)
-- ------------------------------------------------------------
CREATE TABLE dim_mahasiswa (
    sk_mahasiswa   SERIAL       PRIMARY KEY,
    id_mhs_sumber  INT          NOT NULL,        -- NK dari OLTP
    nim            VARCHAR(20)  NOT NULL,
    nama_mhs       VARCHAR(150) NOT NULL,
    jenis_kelamin  CHAR(1),
    kode_provinsi_asal CHAR(2),
    nama_provinsi_asal VARCHAR(100),
    nama_sma_asal  VARCHAR(200),
    akreditasi_sma_asal CHAR(2),
    sk_prodi       INT,                          -- FK ke dim_prodi
    tahun_masuk    INT,
    status_mhs     VARCHAR(20),
    -- SCD Type 2 columns
    tgl_efektif    DATE         NOT NULL,
    tgl_kadaluarsa DATE         DEFAULT '9999-12-31',
    is_current     BOOLEAN      DEFAULT TRUE
);

-- ------------------------------------------------------------
-- Dim Dosen (SCD Type 1 – update langsung)
-- ------------------------------------------------------------
CREATE TABLE dim_dosen (
    sk_dosen       SERIAL       PRIMARY KEY,
    id_dosen_sumber INT         NOT NULL,        -- NK dari OLTP
    nidn           VARCHAR(20)  NOT NULL,
    nama_dosen     VARCHAR(150) NOT NULL,
    jabatan_akademik VARCHAR(30),
    pendidikan_terakhir CHAR(2),
    sk_prodi       INT,                          -- FK ke dim_prodi
    status_aktif   BOOLEAN,
    tgl_update_dw  DATE         DEFAULT CURRENT_DATE
);

-- ------------------------------------------------------------
-- Dim Mata Kuliah
-- ------------------------------------------------------------
CREATE TABLE dim_mata_kuliah (
    sk_mk          SERIAL       PRIMARY KEY,
    id_mk_sumber   INT          NOT NULL,        -- NK dari OLTP
    kode_mk        VARCHAR(10)  NOT NULL,
    nama_mk        VARCHAR(150) NOT NULL,
    sks            SMALLINT,
    semester_ke    SMALLINT,
    jenis_mk       VARCHAR(20),
    sk_prodi       INT                           -- FK ke dim_prodi
);

-- ------------------------------------------------------------
-- Dim Demografi Ekonomi (Enrichment Data Publik: UMP & Akreditasi SMA)
-- ------------------------------------------------------------
CREATE TABLE dim_demografi_ekonomi (
    sk_demografi        SERIAL       PRIMARY KEY,
    kode_provinsi       CHAR(2)      UNIQUE NOT NULL,
    nama_provinsi       VARCHAR(100) NOT NULL,
    -- Data UMP dari BPS (Upah Minimum Provinsi)
    ump_2021            BIGINT,
    ump_2022            BIGINT,
    ump_2023            BIGINT,
    ump_2024            BIGINT,
    ump_terbaru         BIGINT,                  -- UMP tahun terbaru
    tahun_ump_terbaru   INT,
    kategori_ekonomi    VARCHAR(20),             -- 'Tinggi','Menengah','Rendah'
    -- Metadata publik
    sumber_data         VARCHAR(100) DEFAULT 'BPS Indonesia',
    tgl_update_dw       DATE DEFAULT CURRENT_DATE
);

-- ============================================================
-- TABEL FAKTA
-- ============================================================

-- ------------------------------------------------------------
-- Fact Pendaftaran (Penerimaan Mahasiswa Baru)
-- ------------------------------------------------------------
CREATE TABLE fact_pendaftaran (
    sk_pendaftaran  SERIAL   PRIMARY KEY,
    id_pendaftaran_sumber INT,
    sk_waktu        INT      NOT NULL REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa    INT      NOT NULL REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_prodi        INT      NOT NULL REFERENCES dim_prodi(sk_prodi),
    sk_demografi    INT               REFERENCES dim_demografi_ekonomi(sk_demografi),
    -- Measures
    jalur_masuk     VARCHAR(30),
    status_diterima BOOLEAN,
    jumlah_pendaftar INT     DEFAULT 1,   -- additive measure
    -- Metadata ETL
    tgl_load        TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Fact KRS (Pengambilan Mata Kuliah)
-- ------------------------------------------------------------
CREATE TABLE fact_krs (
    sk_krs          SERIAL   PRIMARY KEY,
    id_krs_sumber   INT,
    sk_waktu        INT      NOT NULL REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa    INT      NOT NULL REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_mk           INT      NOT NULL REFERENCES dim_mata_kuliah(sk_mk),
    sk_dosen        INT      NOT NULL REFERENCES dim_dosen(sk_dosen),
    sk_prodi        INT      NOT NULL REFERENCES dim_prodi(sk_prodi),
    -- Measures
    sks_diambil     SMALLINT,
    status_krs      VARCHAR(15),
    jumlah_krs      INT      DEFAULT 1,
    -- Metadata ETL
    tgl_load        TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Fact Kelulusan / Nilai (Grain: 1 baris per mata kuliah per mahasiswa per semester)
-- ------------------------------------------------------------
CREATE TABLE fact_kelulusan (
    sk_kelulusan    SERIAL   PRIMARY KEY,
    id_nilai_sumber INT,
    sk_waktu        INT      NOT NULL REFERENCES dim_waktu(sk_waktu),
    sk_mahasiswa    INT      NOT NULL REFERENCES dim_mahasiswa(sk_mahasiswa),
    sk_mk           INT      NOT NULL REFERENCES dim_mata_kuliah(sk_mk),
    sk_dosen        INT      NOT NULL REFERENCES dim_dosen(sk_dosen),
    sk_prodi        INT      NOT NULL REFERENCES dim_prodi(sk_prodi),
    sk_demografi    INT               REFERENCES dim_demografi_ekonomi(sk_demografi),
    -- Measures
    nilai_angka     NUMERIC(5,2),
    nilai_huruf     CHAR(2),
    bobot_nilai     NUMERIC(3,2),
    ip_semester     NUMERIC(4,2),
    ipk_kumulatif   NUMERIC(4,2),
    sks_mk          SMALLINT,
    lulus_flag      BOOLEAN,             -- nilai >= D (bobot >= 1.0)
    tepat_waktu_flag BOOLEAN,            -- lulus <= 8 semester
    -- Metadata ETL
    tgl_load        TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- INDEX DW
-- ============================================================
CREATE INDEX idx_fp_waktu       ON fact_pendaftaran(sk_waktu);
CREATE INDEX idx_fp_prodi       ON fact_pendaftaran(sk_prodi);
CREATE INDEX idx_fp_demografi   ON fact_pendaftaran(sk_demografi);

CREATE INDEX idx_fkrs_waktu     ON fact_krs(sk_waktu);
CREATE INDEX idx_fkrs_mhs       ON fact_krs(sk_mahasiswa);
CREATE INDEX idx_fkrs_dosen     ON fact_krs(sk_dosen);

CREATE INDEX idx_fk_waktu       ON fact_kelulusan(sk_waktu);
CREATE INDEX idx_fk_mhs         ON fact_kelulusan(sk_mahasiswa);
CREATE INDEX idx_fk_prodi       ON fact_kelulusan(sk_prodi);
CREATE INDEX idx_fk_demografi   ON fact_kelulusan(sk_demografi);

-- ============================================================
-- ANALYTICAL VIEWS (Business Queries)
-- ============================================================

-- View: Rasio Kelulusan Tepat Waktu per Prodi per Tahun
CREATE OR REPLACE VIEW v_rasio_kelulusan_tepat_waktu AS
SELECT
    dp.nama_prodi,
    dp.jenjang,
    dw.tahun_akademik,
    COUNT(DISTINCT fk.sk_mahasiswa)                          AS total_mahasiswa,
    COUNT(DISTINCT CASE WHEN fk.tepat_waktu_flag THEN fk.sk_mahasiswa END) AS lulus_tepat_waktu,
    ROUND(
        COUNT(DISTINCT CASE WHEN fk.tepat_waktu_flag THEN fk.sk_mahasiswa END)::NUMERIC
        / NULLIF(COUNT(DISTINCT fk.sk_mahasiswa), 0) * 100, 2
    )                                                         AS rasio_tepat_waktu_pct
FROM fact_kelulusan fk
JOIN dim_prodi   dp ON fk.sk_prodi  = dp.sk_prodi
JOIN dim_waktu   dw ON fk.sk_waktu  = dw.sk_waktu
WHERE fk.lulus_flag = TRUE
GROUP BY dp.nama_prodi, dp.jenjang, dw.tahun_akademik
ORDER BY dw.tahun_akademik, dp.nama_prodi;

-- View: Tren IPK Rata-rata per Tahun per Prodi
CREATE OR REPLACE VIEW v_tren_ipk_tahunan AS
SELECT
    dp.nama_prodi,
    dp.jenjang,
    dw.tahun_akademik,
    dw.label_semester,
    ROUND(AVG(fk.ipk_kumulatif), 3)  AS rata_rata_ipk,
    COUNT(DISTINCT fk.sk_mahasiswa)  AS jumlah_mahasiswa
FROM fact_kelulusan fk
JOIN dim_prodi  dp ON fk.sk_prodi = dp.sk_prodi
JOIN dim_waktu  dw ON fk.sk_waktu = dw.sk_waktu
WHERE fk.ipk_kumulatif IS NOT NULL
GROUP BY dp.nama_prodi, dp.jenjang, dw.tahun_akademik, dw.label_semester
ORDER BY dw.tahun_akademik, dp.nama_prodi;

-- View: Korelasi UMP Asal vs IPK
CREATE OR REPLACE VIEW v_korelasi_ump_ipk AS
SELECT
    de.nama_provinsi,
    de.kategori_ekonomi,
    de.ump_terbaru,
    ROUND(AVG(fk.ipk_kumulatif), 3)  AS rata_rata_ipk,
    COUNT(DISTINCT fk.sk_mahasiswa)  AS jumlah_mahasiswa
FROM fact_kelulusan fk
JOIN dim_demografi_ekonomi de ON fk.sk_demografi = de.sk_demografi
WHERE fk.ipk_kumulatif IS NOT NULL
GROUP BY de.nama_provinsi, de.kategori_ekonomi, de.ump_terbaru
ORDER BY de.ump_terbaru DESC;

-- View: Korelasi Akreditasi SMA vs IPK
CREATE OR REPLACE VIEW v_korelasi_akreditasi_sma_ipk AS
SELECT
    dm.akreditasi_sma_asal,
    dp.nama_prodi,
    ROUND(AVG(fk.ipk_kumulatif), 3)  AS rata_rata_ipk,
    COUNT(DISTINCT fk.sk_mahasiswa)  AS jumlah_mahasiswa,
    ROUND(AVG(fk.nilai_angka), 2)    AS rata_rata_nilai
FROM fact_kelulusan fk
JOIN dim_mahasiswa dm ON fk.sk_mahasiswa = dm.sk_mahasiswa AND dm.is_current = TRUE
JOIN dim_prodi     dp ON fk.sk_prodi     = dp.sk_prodi
WHERE fk.ipk_kumulatif IS NOT NULL
GROUP BY dm.akreditasi_sma_asal, dp.nama_prodi
ORDER BY dm.akreditasi_sma_asal, dp.nama_prodi;
