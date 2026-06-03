"""
analytical_queries.py
Menjalankan 4 query analitik KPI dari SIAKAD DW dan menyimpan hasilnya ke CSV
"""
import sqlite3, csv, re
from pathlib import Path

SQL_PATH = Path(__file__).parent.parent / 'dw siakad.sql'
OUT_DIR  = Path(__file__).parent.parent / 'kpi'

# ── Konversi PostgreSQL dump → SQLite in-memory ───────────────
print("=" * 60)
print("  Memuat dan mengkonversi dw siakad.sql ke SQLite...")
print("=" * 60)

if not SQL_PATH.exists():
    raise FileNotFoundError(
        f"\n[ERROR] File tidak ditemukan: {SQL_PATH}"
        f"\nPastikan file dw siakad.sql ada di root folder repo"
    )

with open(SQL_PATH, 'r', encoding='utf-8') as f:
    content = f.read()

lines = content.split('\n')
sqlite_lines = []

for line in lines:
    line_s = line.strip().rstrip('\r')

    # Skip sequence blocks
    if re.match(r'(AS integer|START WITH|INCREMENT BY|NO MINVALUE|NO MAXVALUE|CACHE \d+;)', line_s):
        continue

    # Skip PostgreSQL-specific commands
    skip_prefixes = [
        '--', 'SET ', 'SELECT pg_catalog', 'ALTER TABLE', 'ALTER SEQUENCE',
        'CREATE SEQUENCE', 'CREATE INDEX', 'REVOKE', 'GRANT', '\\',
        'COPY ', '\\.', 'SELECT setval', 'pg_dump', 'Dumped', 'Started',
        'Completed', 'TOC', 'CONSTRAINT', 'ADD CONSTRAINT',
        'CREATE UNIQUE INDEX', 'OWNER TO', 'nextval'
    ]
    if any(line_s.startswith(x) for x in skip_prefixes):
        continue
    if not line_s:
        continue

    # Convert PostgreSQL syntax → SQLite syntax
    line_s = re.sub(r'CREATE TABLE public\.(\w+)',
                    r'CREATE TABLE IF NOT EXISTS \1', line_s)
    line_s = re.sub(r'INSERT INTO public\.(\w+)',
                    r'INSERT OR IGNORE INTO \1', line_s)
    line_s = re.sub(r"DEFAULT nextval\('[^']+'\)", '', line_s)
    line_s = re.sub(r'::\w+(\s*\[\])?', '', line_s)
    line_s = re.sub(r'character varying\(\d+\)', 'TEXT', line_s)
    line_s = re.sub(r'character varying', 'TEXT', line_s)
    line_s = line_s.replace('boolean', 'INTEGER')
    line_s = re.sub(r'\bnumeric(\(\d+,\d+\))?', 'REAL', line_s)
    line_s = line_s.replace('double precision', 'REAL')
    line_s = re.sub(r'\btrue\b', '1', line_s)
    line_s = re.sub(r'\bfalse\b', '0', line_s)

    sqlite_lines.append(line_s)

sqlite_script = '\n'.join(sqlite_lines)

conn = sqlite3.connect(':memory:')
conn.row_factory = sqlite3.Row

try:
    conn.executescript(sqlite_script)
    conn.commit()
except Exception as e:
    raise RuntimeError(f"[ERROR] Gagal konversi SQL: {e}")

# Verifikasi tabel
tables = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()
print(f"\n  Tabel tersedia:")
for t in tables:
    n = conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    print(f"    {t[0]:<30} {n:>8} baris")
print()


# ── Helper: run query & simpan CSV ───────────────────────────
def run_query(title, sql, outfile):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)
    rows = conn.execute(sql).fetchall()
    if rows:
        headers = list(rows[0].keys())
        print(f"  {'  |  '.join(headers)}")
        print('-'*60)
        for r in rows[:10]:
            print('  ' + '  |  '.join(str(r[h]) for h in headers))
        if len(rows) > 10:
            print(f"  ... dan {len(rows)-10} baris lainnya")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / outfile
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        w.writerows([dict(r) for r in rows])
    print(f"\n  Saved to: {outfile} ({len(rows)} baris)")
    return rows


# ── KPI 1: Rasio Kelulusan Tepat Waktu per Prodi ─────────────
run_query(
    "KPI 1: Rasio Kelulusan Tepat Waktu per Prodi",
    """
    SELECT
        dp.nama_prodi,
        dp.jenjang,
        dw.tahun_akademik,
        COUNT(DISTINCT fk.sk_mahasiswa)  AS total_mahasiswa_lulus,
        COUNT(DISTINCT CASE WHEN fk.tepat_waktu_flag=1 THEN fk.sk_mahasiswa END)
                                            AS lulus_tepat_waktu,
        ROUND(
          COUNT(DISTINCT CASE WHEN fk.tepat_waktu_flag=1 THEN fk.sk_mahasiswa END) * 100.0
            / MAX(COUNT(DISTINCT fk.sk_mahasiswa), 1)
        , 2) AS rasio_tepat_waktu_pct
    FROM fact_kelulusan fk
    JOIN dim_prodi  dp ON fk.sk_prodi = dp.sk_prodi
    JOIN dim_waktu  dw ON fk.sk_waktu = dw.sk_waktu
    WHERE fk.lulus_flag = 1
    GROUP BY dp.nama_prodi, dp.jenjang, dw.tahun_akademik
    ORDER BY dw.tahun_akademik, rasio_tepat_waktu_pct DESC
    """,
    "kpi1_rasio_kelulusan.csv"
)

# ── KPI 2: Tren IPK Rata-rata per Tahun ──────────────────────
run_query(
    "KPI 2: Tren IPK Rata-rata per Tahun Akademik",
    """
    SELECT
        dw.tahun_akademik,
        dw.label_semester,
        dw.periode_label,
        ROUND(AVG(fk.ipk_kumulatif), 3)  AS rata_rata_ipk,
        ROUND(MIN(fk.ipk_kumulatif), 3)  AS ipk_min,
        ROUND(MAX(fk.ipk_kumulatif), 3)  AS ipk_max,
        COUNT(DISTINCT fk.sk_mahasiswa)  AS jumlah_mahasiswa
    FROM fact_kelulusan fk
    JOIN dim_waktu dw ON fk.sk_waktu = dw.sk_waktu
    WHERE fk.ipk_kumulatif IS NOT NULL
    GROUP BY dw.tahun_akademik, dw.label_semester, dw.periode_label
    ORDER BY dw.tahun_akademik, dw.semester
    """,
    "kpi2_tren_ipk.csv"
)

# ── KPI 3: Korelasi UMP vs IPK ───────────────────────────────
run_query(
    "KPI 3: Korelasi UMP Provinsi Asal vs IPK Mahasiswa",
    """
    SELECT
        de.nama_provinsi,
        de.kategori_ekonomi,
        ROUND(de.ump_terbaru / 1000000.0, 2) AS ump_juta,
        ROUND(AVG(fk.ipk_kumulatif), 3)      AS rata_rata_ipk,
        COUNT(DISTINCT fk.sk_mahasiswa)       AS jumlah_mahasiswa
    FROM fact_kelulusan fk
    JOIN dim_demografi_ekonomi de ON fk.sk_demografi = de.sk_demografi
    WHERE fk.ipk_kumulatif IS NOT NULL
    GROUP BY de.nama_provinsi, de.kategori_ekonomi, de.ump_terbaru
    ORDER BY de.ump_terbaru DESC
    """,
    "kpi3_korelasi_ump_ipk.csv"
)

# ── KPI 4: Korelasi Akreditasi SMA vs IPK ────────────────────
run_query(
    "KPI 4: Korelasi Akreditasi SMA Asal vs IPK",
    """
    SELECT
        dm.akreditasi_sma_asal                AS akreditasi_sma,
        dp.nama_prodi,
        ROUND(AVG(fk.ipk_kumulatif), 3)       AS rata_rata_ipk,
        ROUND(AVG(fk.nilai_angka), 2)         AS rata_rata_nilai,
        COUNT(DISTINCT fk.sk_mahasiswa)        AS jumlah_mahasiswa
    FROM fact_kelulusan fk
    JOIN dim_mahasiswa dm ON fk.sk_mahasiswa = dm.sk_mahasiswa AND dm.is_current=1
    JOIN dim_prodi     dp ON fk.sk_prodi     = dp.sk_prodi
    WHERE fk.ipk_kumulatif IS NOT NULL
    GROUP BY dm.akreditasi_sma_asal, dp.nama_prodi
    ORDER BY dm.akreditasi_sma_asal, rata_rata_ipk DESC
    """,
    "kpi4_korelasi_akreditasi_ipk.csv"
)

conn.close()
print("\n\nSemua query analitik berhasil dijalankan!")