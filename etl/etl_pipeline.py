"""
etl_pipeline.py
ETL Pipeline SIAKAD Data Warehouse menggunakan Python + Prefect
Mengintegrasikan data OLTP internal (CSV dummy) dan data publik eksternal (UMP BPS)

Alur:
  Extract  → Baca CSV OLTP + CSV UMP BPS
  Transform → Bersihkan, validasi, bangun dimensi + tabel fakta
  Load     → Simpan ke SQLite (portable, tidak butuh server PostgreSQL)
             File: siakad_dw.db
"""
import os, csv, sqlite3, logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Prefect imports
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact

# ─────────────────────────────────────────────
# Konfigurasi
# ─────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR.parent / 'data'
DB_PATH    = BASE_DIR.parent / 'siakad_dw.db'

# ═══════════════════════════════════════════════════════════════
# EXTRACT TASKS
# ═══════════════════════════════════════════════════════════════

@task(name="extract_oltp_data", retries=2, retry_delay_seconds=5)
def extract_oltp_data() -> dict[str, list[dict]]:
    """Extract semua data dari sumber OLTP (CSV dummy)."""
    logger = get_run_logger()
    tables = [
        'provinsi','fakultas','program_studi','tahun_akademik',
        'dosen','mata_kuliah','mahasiswa','pendaftaran','krs','nilai_mahasiswa'
    ]
    data = {}
    for tbl in tables:
        path = DATA_DIR / f"{tbl}.csv"
        with open(path, encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        data[tbl] = rows
        logger.info(f"  Extract {tbl}: {len(rows)} baris")
    return data


@task(name="extract_public_ump", retries=2, retry_delay_seconds=5)
def extract_public_ump() -> list[dict]:
    """Extract data publik UMP BPS dari CSV."""
    logger = get_run_logger()
    path = DATA_DIR / 'ump_provinsi_bps.csv'
    with open(path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    logger.info(f"  Extract UMP BPS: {len(rows)} provinsi")
    return rows


# ═══════════════════════════════════════════════════════════════
# TRANSFORM TASKS
# ═══════════════════════════════════════════════════════════════

@task(name="transform_dim_waktu")
def transform_dim_waktu(oltp: dict) -> list[dict]:
    logger = get_run_logger()
    label_map = {1: 'Ganjil', 2: 'Genap'}
    dims = []
    for ta in oltp['tahun_akademik']:
        smt = int(ta['semester'])
        th  = int(ta['tahun_mulai'])
        dims.append({
            'id_ta_sumber' : int(ta['id_ta']),
            'kode_ta'      : ta['kode_ta'],
            'tahun_akademik': th,
            'semester'     : smt,
            'label_semester': label_map[smt],
            'kuartal'      : (smt * 2) - 1,
            'tahun_ajaran' : f"{th}/{th+1}",
            'periode_label': f"Semester {label_map[smt]} {th}/{th+1}",
        })
    logger.info(f"  dim_waktu: {len(dims)} baris")
    return dims


@task(name="transform_dim_prodi")
def transform_dim_prodi(oltp: dict) -> list[dict]:
    logger = get_run_logger()
    fak_map = {str(f['id_fakultas']): f for f in oltp['fakultas']}
    dims = []
    for p in oltp['program_studi']:
        fak = fak_map.get(str(p['id_fakultas']), {})
        dims.append({
            'id_prodi_sumber': int(p['id_prodi']),
            'kode_prodi'     : p['kode_prodi'],
            'nama_prodi'     : p['nama_prodi'],
            'jenjang'        : p['jenjang'],
            'kode_fakultas'  : fak.get('kode_fakultas',''),
            'nama_fakultas'  : fak.get('nama_fakultas',''),
            'akreditasi_prodi': p.get('akreditasi_prodi','B'),
        })
    logger.info(f"  dim_prodi: {len(dims)} baris")
    return dims


@task(name="transform_dim_demografi")
def transform_dim_demografi(ump_data: list[dict]) -> list[dict]:
    """Transform data publik UMP menjadi dim_demografi_ekonomi."""
    logger = get_run_logger()
    dims = []
    for row in ump_data:
        ump_2024 = int(row.get('ump_2024', 0) or 0)
        # Kategorikan berdasarkan UMP 2024
        if ump_2024 >= 3_500_000:
            kategori = 'Tinggi'
        elif ump_2024 >= 2_500_000:
            kategori = 'Menengah'
        else:
            kategori = 'Rendah'
        dims.append({
            'kode_provinsi'      : row['kode_provinsi'],
            'nama_provinsi'      : row['nama_provinsi'],
            'ump_2021'           : int(row.get('ump_2021', 0) or 0),
            'ump_2022'           : int(row.get('ump_2022', 0) or 0),
            'ump_2023'           : int(row.get('ump_2023', 0) or 0),
            'ump_2024'           : ump_2024,
            'ump_terbaru'        : ump_2024,
            'tahun_ump_terbaru'  : 2024,
            'kategori_ekonomi'   : kategori,
            'sumber_data'        : row.get('sumber','BPS Indonesia'),
            'tgl_update_dw'      : str(date.today()),
        })
    logger.info(f"  dim_demografi_ekonomi: {len(dims)} baris")
    return dims


@task(name="transform_dim_mahasiswa_scd2")
def transform_dim_mahasiswa_scd2(
    oltp: dict,
    dim_prodi: list[dict],
    dim_demografi: list[dict],
    conn: sqlite3.Connection
) -> tuple[list[dict], dict]:
    """
    SCD Type 2 untuk dim_mahasiswa.
    Cek apakah status_mhs berubah → jika ya, tutup record lama dan buat yang baru.
    """
    logger = get_run_logger()
    prov_map   = {p['kode_provinsi']: p['nama_provinsi'] for p in oltp['provinsi']}
    prodi_sk   = {p['id_prodi_sumber']: i+1 for i,p in enumerate(dim_prodi)}

    # Load existing SCD rows (dari DB)
    cur = conn.cursor()
    cur.execute("SELECT id_mhs_sumber, nim, status_mhs, sk_mahasiswa FROM dim_mahasiswa WHERE is_current=1")
    existing = {int(r[0]): {'nim':r[1],'status':r[2],'sk':r[3]} for r in cur.fetchall()}

    new_rows  = []
    sk_map    = {}   # id_mhs_sumber → sk_mahasiswa (current)
    sk_counter = cur.execute("SELECT COALESCE(MAX(sk_mahasiswa),0) FROM dim_mahasiswa").fetchone()[0]

    today = str(date.today())

    for mhs in oltp['mahasiswa']:
        id_src = int(mhs['id_mhs'])
        status_new = mhs['status_mhs']

        if id_src in existing:
            old = existing[id_src]
            if old['status'] != status_new:
                # Tutup record lama
                conn.execute(
                    "UPDATE dim_mahasiswa SET tgl_kadaluarsa=?, is_current=0 WHERE sk_mahasiswa=?",
                    (today, old['sk'])
                )
                logger.info(f"  SCD2: id_mhs={id_src} status {old['status']}→{status_new}")
                # Buat record baru
                sk_counter += 1
                row = _build_mhs_row(sk_counter, mhs, prodi_sk, prov_map, today)
                new_rows.append(row)
                sk_map[id_src] = sk_counter
            else:
                sk_map[id_src] = old['sk']
        else:
            sk_counter += 1
            row = _build_mhs_row(sk_counter, mhs, prodi_sk, prov_map, today)
            new_rows.append(row)
            sk_map[id_src] = sk_counter

    logger.info(f"  dim_mahasiswa: {len(new_rows)} baris baru/updated (SCD2)")
    return new_rows, sk_map


def _build_mhs_row(sk, mhs, prodi_sk, prov_map, today):
    return {
        'sk_mahasiswa'       : sk,
        'id_mhs_sumber'      : int(mhs['id_mhs']),
        'nim'                : mhs['nim'],
        'nama_mhs'           : mhs['nama_mhs'],
        'jenis_kelamin'      : mhs['jenis_kelamin'],
        'kode_provinsi_asal' : mhs['kode_provinsi_asal'],
        'nama_provinsi_asal' : prov_map.get(mhs['kode_provinsi_asal'], ''),
        'nama_sma_asal'      : mhs['nama_sma_asal'],
        'akreditasi_sma_asal': mhs['akreditasi_sma_asal'],
        'sk_prodi'           : prodi_sk.get(int(mhs['id_prodi']), 1),
        'tahun_masuk'        : int(mhs['tahun_masuk']),
        'status_mhs'         : mhs['status_mhs'],
        'tgl_efektif'        : today,
        'tgl_kadaluarsa'     : '9999-12-31',
        'is_current'         : 1,
    }


@task(name="transform_dim_dosen_scd1")
def transform_dim_dosen_scd1(oltp: dict, dim_prodi: list[dict]) -> list[dict]:
    """SCD Type 1 untuk dim_dosen (update langsung, tidak simpan historis)."""
    logger = get_run_logger()
    prodi_sk = {p['id_prodi_sumber']: i+1 for i,p in enumerate(dim_prodi)}
    dims = []
    for i, d in enumerate(oltp['dosen'], 1):
        dims.append({
            'sk_dosen'           : i,
            'id_dosen_sumber'    : int(d['id_dosen']),
            'nidn'               : d['nidn'],
            'nama_dosen'         : d['nama_dosen'],
            'jabatan_akademik'   : d['jabatan_akademik'],
            'pendidikan_terakhir': d['pendidikan_terakhir'],
            'sk_prodi'           : prodi_sk.get(int(d['id_prodi']), 1),
            'status_aktif'       : 1 if d['status_aktif'] == 'True' else 0,
            'tgl_update_dw'      : str(date.today()),
        })
    logger.info(f"  dim_dosen: {len(dims)} baris (SCD1)")
    return dims


@task(name="transform_dim_matkul")
def transform_dim_matkul(oltp: dict, dim_prodi: list[dict]) -> list[dict]:
    logger = get_run_logger()
    prodi_sk = {p['id_prodi_sumber']: i+1 for i,p in enumerate(dim_prodi)}
    dims = []
    for i, mk in enumerate(oltp['mata_kuliah'], 1):
        dims.append({
            'sk_mk'          : i,
            'id_mk_sumber'   : int(mk['id_mk']),
            'kode_mk'        : mk['kode_mk'],
            'nama_mk'        : mk['nama_mk'],
            'sks'            : int(mk['sks']),
            'semester_ke'    : int(mk['semester_ke']),
            'jenis_mk'       : mk['jenis_mk'],
            'sk_prodi'       : prodi_sk.get(int(mk['id_prodi']), 1),
        })
    logger.info(f"  dim_mata_kuliah: {len(dims)} baris")
    return dims


@task(name="transform_fact_pendaftaran")
def transform_fact_pendaftaran(
    oltp: dict, dim_waktu: list, dim_prodi: list,
    sk_mhs_map: dict, sk_demo_map: dict, mhs_prov_map: dict
) -> list[dict]:
    logger = get_run_logger()
    ta_sk  = {d['id_ta_sumber']: i+1 for i,d in enumerate(dim_waktu)}
    prodi_sk = {p['id_prodi_sumber']: i+1 for i,p in enumerate(dim_prodi)}
    mhs_map = {int(m['id_mhs']): m for m in oltp['mahasiswa']}

    facts = []
    for p in oltp['pendaftaran']:
        id_mhs  = int(p['id_mhs'])
        id_ta   = int(p['id_ta'])
        mhs     = mhs_map.get(id_mhs, {})
        kode_prov = mhs.get('kode_provinsi_asal','')

        facts.append({
            'id_pendaftaran_sumber': int(p['id_pendaftaran']),
            'sk_waktu'    : ta_sk.get(id_ta, 1),
            'sk_mahasiswa': sk_mhs_map.get(id_mhs, 1),
            'sk_prodi'    : prodi_sk.get(int(mhs.get('id_prodi', 1)), 1),
            'sk_demografi': sk_demo_map.get(kode_prov),
            'jalur_masuk' : p['jalur_masuk'],
            'status_diterima': 1 if p['status_diterima']=='True' else 0,
            'jumlah_pendaftar': 1,
            'tgl_load'    : str(datetime.now()),
        })
    logger.info(f"  fact_pendaftaran: {len(facts)} baris")
    return facts


@task(name="transform_fact_krs")
def transform_fact_krs(
    oltp: dict, dim_waktu: list, dim_prodi: list,
    sk_mhs_map: dict, dim_mk: list, dim_dosen: list
) -> list[dict]:
    logger = get_run_logger()
    ta_sk    = {d['id_ta_sumber']: i+1 for i,d in enumerate(dim_waktu)}
    mk_sk    = {d['id_mk_sumber']: d['sk_mk'] for d in dim_mk}
    dosen_sk = {d['id_dosen_sumber']: d['sk_dosen'] for d in dim_dosen}
    mhs_map  = {int(m['id_mhs']): m for m in oltp['mahasiswa']}

    prodi_sk = {}
    for d in dim_mk:
        prodi_sk[d['id_mk_sumber']] = d.get('sk_prodi', 1)

    facts = []
    mk_sks_map = {int(m['id_mk']): int(m['sks']) for m in oltp['mata_kuliah']}

    for krs in oltp['krs']:
        id_mhs  = int(krs['id_mhs'])
        id_mk   = int(krs['id_mk'])
        id_ta   = int(krs['id_ta'])
        id_dos  = int(krs['id_dosen'])
        mhs     = mhs_map.get(id_mhs, {})

        facts.append({
            'id_krs_sumber': int(krs['id_krs']),
            'sk_waktu'    : ta_sk.get(id_ta, 1),
            'sk_mahasiswa': sk_mhs_map.get(id_mhs, 1),
            'sk_mk'       : mk_sk.get(id_mk, 1),
            'sk_dosen'    : dosen_sk.get(id_dos, 1),
            'sk_prodi'    : prodi_sk.get(id_mk, 1),
            'sks_diambil' : mk_sks_map.get(id_mk, 3),
            'status_krs'  : krs['status_krs'],
            'jumlah_krs'  : 1,
            'tgl_load'    : str(datetime.now()),
        })
    logger.info(f"  fact_krs: {len(facts)} baris")
    return facts


@task(name="transform_fact_kelulusan")
def transform_fact_kelulusan(
    oltp: dict, dim_waktu: list, dim_prodi: list,
    sk_mhs_map: dict, dim_mk: list, dim_dosen: list,
    sk_demo_map: dict
) -> list[dict]:
    logger = get_run_logger()
    ta_sk    = {d['id_ta_sumber']: i+1 for i,d in enumerate(dim_waktu)}
    mk_sk    = {d['id_mk_sumber']: d['sk_mk'] for d in dim_mk}
    dosen_sk = {d['id_dosen_sumber']: d['sk_dosen'] for d in dim_dosen}
    mk_sks   = {int(m['id_mk']): int(m['sks']) for m in oltp['mata_kuliah']}
    mk_prodi = {d['id_mk_sumber']: d.get('sk_prodi',1) for d in dim_mk}

    # Hitung semester ke berapa mahasiswa ini (untuk tepat_waktu_flag)
    mhs_ta_count = {}
    for krs in oltp['krs']:
        id_mhs = int(krs['id_mhs'])
        mhs_ta_count[id_mhs] = mhs_ta_count.get(id_mhs, 0)

    krs_map = {int(k['id_krs']): k for k in oltp['krs']}
    mhs_map = {int(m['id_mhs']): m for m in oltp['mahasiswa']}

    # Hitung jumlah semester per mahasiswa
    mhs_smt_count = {}
    for krs in oltp['krs']:
        mid = int(krs['id_mhs'])
        mhs_smt_count[mid] = mhs_smt_count.get(mid, set())
        mhs_smt_count[mid].add(krs['id_ta'])

    facts = []
    for n in oltp['nilai_mahasiswa']:
        id_krs  = int(n['id_krs'])
        krs     = krs_map.get(id_krs)
        if not krs:
            continue
        id_mhs  = int(krs['id_mhs'])
        id_mk   = int(krs['id_mk'])
        id_ta   = int(krs['id_ta'])
        id_dos  = int(krs['id_dosen'])
        mhs     = mhs_map.get(id_mhs, {})
        kode_prov = mhs.get('kode_provinsi_asal','')

        bobot = float(n['bobot_nilai']) if n['bobot_nilai'] else 0.0
        lulus = bobot >= 1.0   # minimal D

        # Tepat waktu: lulus dalam <= 8 semester (mahasiswa S1/D3)
        n_smt = len(mhs_smt_count.get(id_mhs, set()))
        tepat_waktu = (n_smt <= 8) if mhs.get('status_mhs') == 'Lulus' else None

        facts.append({
            'id_nilai_sumber' : int(n['id_nilai']),
            'sk_waktu'    : ta_sk.get(id_ta, 1),
            'sk_mahasiswa': sk_mhs_map.get(id_mhs, 1),
            'sk_mk'       : mk_sk.get(id_mk, 1),
            'sk_dosen'    : dosen_sk.get(id_dos, 1),
            'sk_prodi'    : mk_prodi.get(id_mk, 1),
            'sk_demografi': sk_demo_map.get(kode_prov),
            'nilai_angka' : float(n['nilai_angka']) if n['nilai_angka'] else None,
            'nilai_huruf' : n['nilai_huruf'],
            'bobot_nilai' : bobot,
            'ip_semester' : float(n['ip_semester']) if n['ip_semester'] else None,
            'ipk_kumulatif': float(n['ipk_kumulatif']) if n['ipk_kumulatif'] else None,
            'sks_mk'      : mk_sks.get(id_mk, 3),
            'lulus_flag'  : 1 if lulus else 0,
            'tepat_waktu_flag': 1 if tepat_waktu else 0,
            'tgl_load'    : str(datetime.now()),
        })
    logger.info(f"  fact_kelulusan: {len(facts)} baris")
    return facts


# ═══════════════════════════════════════════════════════════════
# LOAD TASKS
# ═══════════════════════════════════════════════════════════════

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@task(name="load_create_schema")
def load_create_schema() -> sqlite3.Connection:
    """Buat tabel DW di SQLite."""
    logger = get_run_logger()
    conn = get_connection()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS dim_waktu (
        sk_waktu INTEGER PRIMARY KEY AUTOINCREMENT,
        id_ta_sumber INT, kode_ta TEXT, tahun_akademik INT,
        semester INT, label_semester TEXT, kuartal INT,
        tahun_ajaran TEXT, periode_label TEXT);

    CREATE TABLE IF NOT EXISTS dim_prodi (
        sk_prodi INTEGER PRIMARY KEY AUTOINCREMENT,
        id_prodi_sumber INT, kode_prodi TEXT, nama_prodi TEXT,
        jenjang TEXT, kode_fakultas TEXT, nama_fakultas TEXT,
        akreditasi_prodi TEXT);

    CREATE TABLE IF NOT EXISTS dim_demografi_ekonomi (
        sk_demografi INTEGER PRIMARY KEY AUTOINCREMENT,
        kode_provinsi TEXT UNIQUE, nama_provinsi TEXT,
        ump_2021 INT, ump_2022 INT, ump_2023 INT, ump_2024 INT,
        ump_terbaru INT, tahun_ump_terbaru INT,
        kategori_ekonomi TEXT, sumber_data TEXT, tgl_update_dw TEXT);

    CREATE TABLE IF NOT EXISTS dim_mahasiswa (
        sk_mahasiswa INTEGER PRIMARY KEY,
        id_mhs_sumber INT, nim TEXT, nama_mhs TEXT,
        jenis_kelamin TEXT, kode_provinsi_asal TEXT,
        nama_provinsi_asal TEXT, nama_sma_asal TEXT,
        akreditasi_sma_asal TEXT, sk_prodi INT,
        tahun_masuk INT, status_mhs TEXT,
        tgl_efektif TEXT, tgl_kadaluarsa TEXT, is_current INT);

    CREATE TABLE IF NOT EXISTS dim_dosen (
        sk_dosen INTEGER PRIMARY KEY,
        id_dosen_sumber INT, nidn TEXT, nama_dosen TEXT,
        jabatan_akademik TEXT, pendidikan_terakhir TEXT,
        sk_prodi INT, status_aktif INT, tgl_update_dw TEXT);

    CREATE TABLE IF NOT EXISTS dim_mata_kuliah (
        sk_mk INTEGER PRIMARY KEY,
        id_mk_sumber INT, kode_mk TEXT, nama_mk TEXT,
        sks INT, semester_ke INT, jenis_mk TEXT, sk_prodi INT);

    CREATE TABLE IF NOT EXISTS fact_pendaftaran (
        sk_pendaftaran INTEGER PRIMARY KEY AUTOINCREMENT,
        id_pendaftaran_sumber INT,
        sk_waktu INT, sk_mahasiswa INT, sk_prodi INT, sk_demografi INT,
        jalur_masuk TEXT, status_diterima INT,
        jumlah_pendaftar INT, tgl_load TEXT);

    CREATE TABLE IF NOT EXISTS fact_krs (
        sk_krs INTEGER PRIMARY KEY AUTOINCREMENT,
        id_krs_sumber INT,
        sk_waktu INT, sk_mahasiswa INT, sk_mk INT,
        sk_dosen INT, sk_prodi INT,
        sks_diambil INT, status_krs TEXT,
        jumlah_krs INT, tgl_load TEXT);

    CREATE TABLE IF NOT EXISTS fact_kelulusan (
        sk_kelulusan INTEGER PRIMARY KEY AUTOINCREMENT,
        id_nilai_sumber INT,
        sk_waktu INT, sk_mahasiswa INT, sk_mk INT,
        sk_dosen INT, sk_prodi INT, sk_demografi INT,
        nilai_angka REAL, nilai_huruf TEXT, bobot_nilai REAL,
        ip_semester REAL, ipk_kumulatif REAL, sks_mk INT,
        lulus_flag INT, tepat_waktu_flag INT, tgl_load TEXT);
    """)
    conn.commit()
    logger.info("Schema DW berhasil dibuat")
    return conn


def bulk_insert(conn, table, rows, batch=2000):
    if not rows:
        return
    keys = list(rows[0].keys())
    ph   = ','.join(['?'] * len(keys))
    sql  = f"INSERT OR REPLACE INTO {table} ({','.join(keys)}) VALUES ({ph})"
    for i in range(0, len(rows), batch):
        chunk = [tuple(r[k] for k in keys) for r in rows[i:i+batch]]
        conn.executemany(sql, chunk)
    conn.commit()


@task(name="load_dimensions")
def load_dimensions(conn, dims: dict):
    logger = get_run_logger()
    for tbl, rows in dims.items():
        bulk_insert(conn, tbl, rows)
        logger.info(f"  Load {tbl}: {len(rows)} baris")


@task(name="load_facts")
def load_facts(conn, facts: dict):
    logger = get_run_logger()
    for tbl, rows in facts.items():
        bulk_insert(conn, tbl, rows)
        logger.info(f"  Load {tbl}: {len(rows)} baris")


# ═══════════════════════════════════════════════════════════════
# MAIN FLOW
# ═══════════════════════════════════════════════════════════════

@flow(name="SIAKAD-DW-ETL", log_prints=True)
def siakad_etl_flow():
    print("=" * 60)
    print("  SIAKAD Data Warehouse ETL Pipeline")
    print("  Powered by Prefect + Python")
    print("=" * 60)

    # ── EXTRACT ──────────────────────────────────────────────
    print("\n[1/3] EXTRACT")
    oltp     = extract_oltp_data()
    ump_data = extract_public_ump()

    # ── TRANSFORM ────────────────────────────────────────────
    print("\n[2/3] TRANSFORM")
    conn = load_create_schema()

    dim_waktu    = transform_dim_waktu(oltp)
    dim_prodi    = transform_dim_prodi(oltp)
    dim_demografi = transform_dim_demografi(ump_data)
    dim_matkul   = transform_dim_matkul(oltp, dim_prodi)
    dim_dosen    = transform_dim_dosen_scd1(oltp, dim_prodi)

    # Load dim_demografi dulu supaya sk_demografi tersedia
    bulk_insert(conn, 'dim_waktu',             dim_waktu)
    bulk_insert(conn, 'dim_prodi',             dim_prodi)
    bulk_insert(conn, 'dim_demografi_ekonomi', dim_demografi)

    # Map kode_provinsi → sk_demografi
    cur = conn.cursor()
    cur.execute("SELECT kode_provinsi, sk_demografi FROM dim_demografi_ekonomi")
    sk_demo_map = {r[0]: r[1] for r in cur.fetchall()}

    # SCD2 untuk mahasiswa
    mhs_new_rows, sk_mhs_map = transform_dim_mahasiswa_scd2(oltp, dim_prodi, dim_demografi, conn)

    # Load sisa dimensi
    load_dimensions(conn, {
        'dim_mata_kuliah': dim_matkul,
        'dim_dosen'      : dim_dosen,
        'dim_mahasiswa'  : mhs_new_rows,
    })

    # Build prov map untuk fact
    mhs_prov_map = {int(m['id_mhs']): m['kode_provinsi_asal'] for m in oltp['mahasiswa']}

    # Transform fakta
    f_pendaftaran = transform_fact_pendaftaran(
        oltp, dim_waktu, dim_prodi, sk_mhs_map, sk_demo_map, mhs_prov_map)
    f_krs = transform_fact_krs(
        oltp, dim_waktu, dim_prodi, sk_mhs_map, dim_matkul, dim_dosen)
    f_kelulusan = transform_fact_kelulusan(
        oltp, dim_waktu, dim_prodi, sk_mhs_map, dim_matkul, dim_dosen, sk_demo_map)

    # ── LOAD ─────────────────────────────────────────────────
    print("\n[3/3] LOAD (Facts)")
    load_facts(conn, {
        'fact_pendaftaran': f_pendaftaran,
        'fact_krs'        : f_krs,
        'fact_kelulusan'  : f_kelulusan,
    })

    # ── SUMMARY ──────────────────────────────────────────────
    cur = conn.cursor()
    summary = {}
    for tbl in ['dim_waktu','dim_prodi','dim_demografi_ekonomi',
                'dim_mahasiswa','dim_dosen','dim_mata_kuliah',
                'fact_pendaftaran','fact_krs','fact_kelulusan']:
        n = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        summary[tbl] = n

    print("\n" + "=" * 60)
    print("  ETL SUMMARY")
    print("=" * 60)
    for tbl, n in summary.items():
        print(f"  {tbl:<30} {n:>8} baris")
    print("=" * 60)
    print(f"\nDatabase DW: {DB_PATH}")
    conn.close()
    return summary


if __name__ == "__main__":
    result = siakad_etl_flow()
