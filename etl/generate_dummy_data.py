"""
generate_dummy_data.py
Menghasilkan data OLTP dummy untuk SIAKAD (Sistem Informasi Akademik Universitas)
Target: minimal 10.000 baris untuk tabel fakta (nilai_mahasiswa)
"""
import random
import csv
import json
from datetime import date, timedelta
from faker import Faker

fake = Faker('id_ID')
random.seed(42)

# ─────────────────────────────────────────────
# Konstanta
# ─────────────────────────────────────────────
PROVINSI = [
    ('11','Aceh'),('12','Sumatera Utara'),('13','Sumatera Barat'),
    ('14','Riau'),('15','Jambi'),('16','Sumatera Selatan'),
    ('17','Bengkulu'),('18','Lampung'),('19','Kepulauan Bangka Belitung'),
    ('21','Kepulauan Riau'),('31','DKI Jakarta'),('32','Jawa Barat'),
    ('33','Jawa Tengah'),('34','DI Yogyakarta'),('35','Jawa Timur'),
    ('36','Banten'),('51','Bali'),('52','Nusa Tenggara Barat'),
    ('53','Nusa Tenggara Timur'),('61','Kalimantan Barat'),
    ('62','Kalimantan Tengah'),('63','Kalimantan Selatan'),
    ('64','Kalimantan Timur'),('65','Kalimantan Utara'),
    ('71','Sulawesi Utara'),('72','Sulawesi Tengah'),
    ('73','Sulawesi Selatan'),('74','Sulawesi Tenggara'),
    ('75','Gorontalo'),('76','Sulawesi Barat'),
    ('81','Maluku'),('82','Maluku Utara'),
    ('91','Papua Barat'),('94','Papua'),
]

FAKULTAS = [
    ('FT','Fakultas Teknik'),
    ('FKIP','Fakultas Keguruan dan Ilmu Pendidikan'),
    ('FEB','Fakultas Ekonomi dan Bisnis'),
    ('FMIPA','Fakultas MIPA'),
    ('FH','Fakultas Hukum'),
    ('FK','Fakultas Kedokteran'),
]

PRODI = [
    # (kode, nama, jenjang, kode_fak, akreditasi)
    ('TIF','Teknik Informatika','S1','FT','A'),
    ('TSI','Sistem Informasi','S1','FT','B'),
    ('TE','Teknik Elektro','S1','FT','B'),
    ('PBI','Pendidikan Bahasa Inggris','S1','FKIP','A'),
    ('PMAT','Pendidikan Matematika','S1','FKIP','B'),
    ('MAN','Manajemen','S1','FEB','A'),
    ('AKT','Akuntansi','S1','FEB','A'),
    ('MTK','Matematika','S1','FMIPA','B'),
    ('KIM','Kimia','S1','FMIPA','B'),
    ('HKM','Ilmu Hukum','S1','FH','B'),
    ('PDK','Pendidikan Dokter','S1','FK','A'),
    ('TIF2','Teknik Informatika','S2','FT','B'),
]

JALUR_MASUK = ['SNBP','SNBT','Mandiri','Beasiswa']
JALUR_BOBOT = [0.30, 0.40, 0.25, 0.05]

AKREDITASI_SMA = ['A','A','A','B','B','B','C','TT']

JABATAN = ['Asisten Ahli','Lektor','Lektor Kepala','Guru Besar']
PENDIDIKAN = ['S2','S2','S2','S3','S3']

MATA_KULIAH_TEMPLATE = [
    ('Kalkulus','4',1), ('Fisika Dasar','3',1), ('Bahasa Inggris','2',1),
    ('Pemrograman Dasar','3',1), ('Matematika Diskrit','3',2),
    ('Struktur Data','3',2), ('Basis Data','3',3), ('Jaringan Komputer','3',3),
    ('Algoritma & Pemrograman','3',2), ('Sistem Operasi','3',4),
    ('Rekayasa Perangkat Lunak','3',4), ('Kecerdasan Buatan','3',5),
    ('Data Mining','3',5), ('Machine Learning','3',6),
    ('Manajemen Proyek TI','2',6), ('Etika Profesi','2',7),
    ('Skripsi','6',8), ('Kerja Praktek','3',7),
    ('Statistika','3',3), ('Probabilitas','3',3),
    ('Akuntansi Dasar','3',1), ('Ekonomi Mikro','3',1),
    ('Manajemen Keuangan','3',3), ('Pemasaran','3',4),
    ('Hukum Bisnis','2',5), ('Audit','3',5),
    ('Anatomi','3',1), ('Biokimia','3',2), ('Fisiologi','3',2),
]

NILAI_DISTRIBUSI = {
    'A' : (85, 100, 4.0, 'A'),
    'A-': (80, 84,  3.7, 'A-'),
    'B+': (75, 79,  3.3, 'B+'),
    'B' : (70, 74,  3.0, 'B'),
    'B-': (65, 69,  2.7, 'B-'),
    'C+': (60, 64,  2.3, 'C+'),
    'C' : (55, 59,  2.0, 'C'),
    'D' : (45, 54,  1.0, 'D'),
    'E' : (0,  44,  0.0, 'E'),
}
# Bobot probabilitas nilai
NILAI_KEYS = list(NILAI_DISTRIBUSI.keys())
NILAI_PROBS = [0.15, 0.15, 0.18, 0.18, 0.12, 0.08, 0.07, 0.05, 0.02]

TAHUN_AKADEMIK = [
    (1,'2020/2021-1',2020,1),
    (2,'2020/2021-2',2020,2),
    (3,'2021/2022-1',2021,1),
    (4,'2021/2022-2',2021,2),
    (5,'2022/2023-1',2022,1),
    (6,'2022/2023-2',2022,2),
    (7,'2023/2024-1',2023,1),
    (8,'2023/2024-2',2023,2),
    (9,'2024/2025-1',2024,1),
    (10,'2024/2025-2',2024,2),
]

# ─────────────────────────────────────────────
# Generate Data
# ─────────────────────────────────────────────
print("Generating dummy OLTP data...")

# 1. Provinsi
provinsi_rows = [(k, v) for k, v in PROVINSI]

# 2. Fakultas
fakultas_rows = [(i+1, k, v) for i,(k,v) in enumerate(FAKULTAS)]
fak_id_map = {k: i+1 for i,(k,v) in enumerate(FAKULTAS)}

# 3. Program Studi
prodi_rows = []
for i, (kode, nama, jenjang, kode_fak, akr) in enumerate(PRODI):
    prodi_rows.append((i+1, kode, nama, jenjang, fak_id_map[kode_fak], akr))
prodi_id_map = {p[1]: p[0] for p in prodi_rows}

# 4. Tahun Akademik
ta_rows = [(r[0], r[1], r[2], r[3]) for r in TAHUN_AKADEMIK]
ta_id_list = [r[0] for r in ta_rows]

# 5. Dosen  (100 dosen)
dosen_rows = []
for i in range(1, 101):
    prodi_rand = random.choice(prodi_rows)
    dosen_rows.append((
        i,
        f"{1000000000 + i:010d}",       # NIDN
        fake.name(),
        prodi_rand[0],                   # id_prodi
        random.choice(JABATAN),
        random.choice(PENDIDIKAN),
        fake.date_between(start_date=date(2000,1,1), end_date=date(2020,1,1)),
        True
    ))

# 6. Mata Kuliah (per prodi, ambil template)
mk_rows = []
mk_id = 1
for prodi in prodi_rows:
    # Ambil 15 MK per prodi
    sample_mk = random.sample(MATA_KULIAH_TEMPLATE, min(15, len(MATA_KULIAH_TEMPLATE)))
    for mk_tpl in sample_mk:
        nama_mk, sks, smt = mk_tpl
        kode_mk = f"{prodi[1][:3]}{mk_id:03d}"
        jenis = 'Wajib' if random.random() < 0.8 else 'Pilihan'
        mk_rows.append((mk_id, kode_mk, f"{nama_mk} ({prodi[1]})", int(sks), int(smt), prodi[0], jenis))
        mk_id += 1

mk_by_prodi = {}
for mk in mk_rows:
    mk_by_prodi.setdefault(mk[5], []).append(mk)

# 7. Mahasiswa (1500 mahasiswa)
mahasiswa_rows = []
nim_set = set()
for i in range(1, 1501):
    prodi = random.choice(prodi_rows)
    prov  = random.choice(PROVINSI)
    tahun_masuk = random.randint(2018, 2023)
    nim = f"A{tahun_masuk % 100:02d}{prodi[0]:02d}{i:04d}"
    nim_set.add(nim)
    akr_sma = random.choice(AKREDITASI_SMA)
    jk = random.choice(['L','L','P'])
    status = 'Aktif' if tahun_masuk >= 2022 else random.choices(
        ['Aktif','Lulus','DO','Cuti'],
        weights=[0.5,0.4,0.05,0.05])[0]
    mahasiswa_rows.append((
        i, nim, fake.name(), jk,
        fake.date_of_birth(minimum_age=18, maximum_age=28),
        prov[0],
        f"SMA Negeri {random.randint(1,50)} {prov[1]}",
        akr_sma,
        prodi[0],
        tahun_masuk,
        status
    ))

# 8. Pendaftaran
pendaftaran_rows = []
p_id = 1
for mhs in mahasiswa_rows:
    id_mhs, nim, nama, jk, tgl_lahir, kode_prov, nama_sma, akr_sma, id_prodi, tahun_masuk, status = mhs
    # Cari TA pertama (semester 1 tahun masuk atau paling dekat)
    ta_match = None
    for ta in ta_rows:
        if ta[2] == tahun_masuk and ta[3] == 1:
            ta_match = ta
            break
    if ta_match is None:
        ta_match = random.choice(ta_rows)
    jalur = random.choices(JALUR_MASUK, weights=JALUR_BOBOT)[0]
    tgl_daftar = date(tahun_masuk, random.randint(7,8), random.randint(1,28))
    pendaftaran_rows.append((p_id, id_mhs, ta_match[0], jalur, tgl_daftar, True))
    p_id += 1

# 9. KRS & Nilai (target >10.000 baris)
krs_rows = []
nilai_rows = []
krs_id = 1
nilai_id = 1

dosen_by_prodi = {}
for d in dosen_rows:
    dosen_by_prodi.setdefault(d[3], []).append(d)

for mhs in mahasiswa_rows:
    id_mhs, nim, nama, jk, tgl_lahir, kode_prov, nama_sma, akr_sma, id_prodi, tahun_masuk, status = mhs
    
    # Mahasiswa aktif ambil 6-10 semester
    if status == 'Aktif' and tahun_masuk >= 2022:
        n_smt = random.randint(2, 4)
    elif status == 'Lulus':
        n_smt = random.randint(7, 9)
    elif status == 'DO':
        n_smt = random.randint(1, 4)
    else:
        n_smt = random.randint(4, 6)
    
    # Ambil TA yang tersedia setelah tahun masuk
    available_ta = [ta for ta in ta_rows if ta[2] >= tahun_masuk][:n_smt]
    
    mks_prodi = mk_by_prodi.get(id_prodi, mk_rows[:15])
    dosens_prodi = dosen_by_prodi.get(id_prodi, dosen_rows[:5])
    if not dosens_prodi:
        dosens_prodi = dosen_rows[:5]
    
    ipk_running = 0.0
    sks_total = 0
    bobot_total = 0.0
    
    for smt_idx, ta in enumerate(available_ta):
        # Ambil 5-7 MK per semester
        n_mk = random.randint(5, 7)
        smt_mks = random.sample(mks_prodi, min(n_mk, len(mks_prodi)))
        
        ip_smt_bobots = []
        ip_smt_sks = []
        
        for mk in smt_mks:
            id_mk, kode_mk, nama_mk, sks_mk, smt_ke, prodi_mk, jenis_mk = mk
            dosen = random.choice(dosens_prodi)
            
            tgl_krs = date(ta[2], 8 if ta[3]==1 else 2, random.randint(1,20))
            krs_rows.append((
                krs_id, id_mhs, id_mk, ta[0], dosen[0], tgl_krs, 'Disetujui'
            ))
            
            # Nilai
            # Mahasiswa dari SMA A cenderung lebih baik
            if akr_sma == 'A':
                nilai_probs_adj = [0.20,0.18,0.18,0.16,0.10,0.07,0.05,0.04,0.02]
            elif akr_sma == 'B':
                nilai_probs_adj = NILAI_PROBS
            else:
                nilai_probs_adj = [0.08,0.10,0.14,0.18,0.16,0.12,0.10,0.08,0.04]
            
            grade_key = random.choices(NILAI_KEYS, weights=nilai_probs_adj)[0]
            g = NILAI_DISTRIBUSI[grade_key]
            nilai_angka = random.uniform(g[0], g[1])
            bobot = g[2]
            
            ip_smt_bobots.append(bobot * sks_mk)
            ip_smt_sks.append(sks_mk)
            bobot_total += bobot * sks_mk
            sks_total += sks_mk
            
            ip_smt = round(sum(ip_smt_bobots) / max(sum(ip_smt_sks), 1), 2)
            ipk_kum = round(bobot_total / max(sks_total, 1), 2)
            
            nilai_rows.append((
                nilai_id, krs_id, round(nilai_angka, 2), grade_key,
                bobot, ip_smt, ipk_kum, date.today()
            ))
            nilai_id += 1
            krs_id += 1
        
print(f"  Provinsi       : {len(provinsi_rows)}")
print(f"  Fakultas       : {len(fakultas_rows)}")
print(f"  Program Studi  : {len(prodi_rows)}")
print(f"  Tahun Akademik : {len(ta_rows)}")
print(f"  Dosen          : {len(dosen_rows)}")
print(f"  Mata Kuliah    : {len(mk_rows)}")
print(f"  Mahasiswa      : {len(mahasiswa_rows)}")
print(f"  Pendaftaran    : {len(pendaftaran_rows)}")
print(f"  KRS            : {len(krs_rows)}")
print(f"  Nilai          : {len(nilai_rows)}")

# ─────────────────────────────────────────────
# Save to CSV
# ─────────────────────────────────────────────
OUTPUT_DIR = '/home/claude/siakad_dw/data'

def write_csv(filename, headers, rows):
    path = f"{OUTPUT_DIR}/{filename}"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"  Saved: {filename} ({len(rows)} rows)")

write_csv('provinsi.csv',
    ['kode_provinsi','nama_provinsi'], provinsi_rows)

write_csv('fakultas.csv',
    ['id_fakultas','kode_fakultas','nama_fakultas'], fakultas_rows)

write_csv('program_studi.csv',
    ['id_prodi','kode_prodi','nama_prodi','jenjang','id_fakultas','akreditasi_prodi'], prodi_rows)

write_csv('tahun_akademik.csv',
    ['id_ta','kode_ta','tahun_mulai','semester'], ta_rows)

write_csv('dosen.csv',
    ['id_dosen','nidn','nama_dosen','id_prodi','jabatan_akademik','pendidikan_terakhir','tgl_masuk','status_aktif'],
    dosen_rows)

write_csv('mata_kuliah.csv',
    ['id_mk','kode_mk','nama_mk','sks','semester_ke','id_prodi','jenis_mk'], mk_rows)

write_csv('mahasiswa.csv',
    ['id_mhs','nim','nama_mhs','jenis_kelamin','tgl_lahir','kode_provinsi_asal',
     'nama_sma_asal','akreditasi_sma_asal','id_prodi','tahun_masuk','status_mhs'],
    mahasiswa_rows)

write_csv('pendaftaran.csv',
    ['id_pendaftaran','id_mhs','id_ta','jalur_masuk','tanggal_daftar','status_diterima'],
    pendaftaran_rows)

write_csv('krs.csv',
    ['id_krs','id_mhs','id_mk','id_ta','id_dosen','tanggal_krs','status_krs'], krs_rows)

write_csv('nilai_mahasiswa.csv',
    ['id_nilai','id_krs','nilai_angka','nilai_huruf','bobot_nilai','ip_semester',
     'ipk_kumulatif','tgl_input'], nilai_rows)

print(f"\nSemua file CSV berhasil dibuat di {OUTPUT_DIR}/")
print(f"Total baris tabel fakta (nilai_mahasiswa): {len(nilai_rows)}")
