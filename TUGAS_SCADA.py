# ETL: BMKG API -> MySQL (Star Schema Ready)
# Ditujukan untuk memuat data cuaca ke dalam model Star Schema (OLAP ready)

import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import time

# --- 1. Konfigurasi Awal ---
load_dotenv()

# --- TAMBAHKAN DUA BARIS INI UNTUK MENGUJI ---
print(f"DEBUG: DB_URL value is: {os.getenv('DB_URL')}")
print(f"DEBUG: BMKG_API value is: {os.getenv('BMKG_API')}")
# ---------------------------------------------



# Pastikan variabel ini ada di file .env Anda
DB_URL = os.getenv("DB_URL")
API_BASE = os.getenv("BMKG_API")

# Wilayah yang ditargetkan (contoh untuk Surabaya)
WILAYAH = {
    "Keputih": "35.78.09.1001",
    "GebangPutih": "35.78.09.1002",
    "KlampisNgasem": "35.78.09.1003"
}

# Inisialisasi Engine Koneksi Database
try:
    engine = create_engine(DB_URL, pool_pre_ping=True)
except Exception as e:
    print(f"FATAL: Gagal membuat koneksi database. Cek DB_URL di .env. Error: {e}")
    exit()

# --- 2. Fungsi Extract (E) ---

def fetch_one(kode):
    """Mengambil data JSON dari endpoint BMKG untuk satu kode wilayah."""
    url = f"{API_BASE}{kode}"
    print(f"  -> Fetching data from: {url}")
    # Gunakan User-Agent yang ramah
    headers = {
        'User-Agent': 'BMKG-ETL-Script/1.0 (Contact: your@email.com)'
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status() # Akan melempar HTTPError jika status bukan 2xx
    return r.json()

# --- 3. Fungsi Transform (T) ---

def normalize_and_flatten(json_obj, lokasi_name, kode):
    """
    Menormalisasi JSON response BMKG menjadi Pandas DataFrame datar.
    Ini adalah bagian krusial yang harus disesuaikan dengan struktur API BMKG yang sebenarnya.
    """
    rows = []
    
    # 1. Tentukan kunci data utama (asumsi list of forecast items)
    payload_list = []
    # Coba mencari list data di berbagai kunci umum
    if isinstance(json_obj, dict):
        for key in ['data', 'prakiraan', 'list', 'forecast']:
            if key in json_obj and isinstance(json_obj[key], list):
                payload_list = json_obj[key]
                break
        if not payload_list and isinstance(json_obj.get('data'), dict):
             # Beberapa API BMKG mungkin memiliki 'data' sebagai dict yang berisi list
             for key in json_obj['data']:
                 if isinstance(json_obj['data'][key], list):
                     payload_list = json_obj['data'][key]
                     break

    if not payload_list and isinstance(json_obj, list):
         payload_list = json_obj

    if not payload_list:
        print(f"  -> WARNING: Payload list not found for {lokasi_name}. Inspect JSON structure.")
        return pd.DataFrame(rows)

    # 2. Iterasi dan Ekstraksi Parameter
    for item in payload_list:
        if not isinstance(item, dict):
             continue
             
        # Ekstraksi Timestamp (ts)
        ts = None
        for k in ['time', 'jam', 'datetime', 'tgl', '@datetime', '@time']:
             if k in item:
                 ts = item[k]
                 break
        
        # Ekstraksi Parameter Cuaca
        t = item.get('t') or item.get('temperature') or item.get('temp')
        hu = item.get('hu') or item.get('humidity')
        weather_desc = item.get('weather_desc') or item.get('keterangan') or item.get('cuaca') or item.get('desc')

        try:
            ts_parsed = pd.to_datetime(ts, utc=True) if ts else pd.Timestamp.utcnow().floor('min')
        except Exception:
            # Fallback jika timestamp tidak valid
            ts_parsed = pd.Timestamp.utcnow().floor('min')

        rows.append({
            "lokasi": lokasi_name,
            "kode_lokasi": kode,
            "ts": ts_parsed,
            # Konversi aman ke float/int
            "temperature": float(t) if (t is not None and str(t).replace('.','',1).isdigit()) else None,
            "humidity": int(hu) if (hu is not None and str(hu).isdigit()) else None,
            "weather_desc": weather_desc
        })
    return pd.DataFrame(rows)

# --- 4. Fungsi Load (L) ke Star Schema ---

def upsert_dim_station(df_station):
    """Memperbarui atau menyisipkan (UPSERT) data Stasiun ke dim_station."""
    print("  -> Loading dim_station...")
    # Menggunakan pandas to_sql untuk bulk staging, lalu menggunakan SQL untuk UPSERT yang efisien
    
    # Tambahkan kolom dummy untuk memastikan format data
    df_station['latitude'] = df_station['latitude'].fillna(0)
    df_station['longitude'] = df_station['longitude'].fillna(0)

    # Note: MySQL 'REPLACE INTO' adalah cara paling mudah untuk UPSERT sederhana
    # Tapi kita gunakan ON DUPLICATE KEY UPDATE untuk fleksibilitas
    with engine.begin() as conn:
        for _, r in df_station.iterrows():
            sql = text("""
            INSERT INTO dim_station (station_code, station_name, latitude, longitude)
            VALUES (:code, :name, :lat, :lon)
            ON DUPLICATE KEY UPDATE 
                station_name=VALUES(station_name), 
                latitude=VALUES(latitude), 
                longitude=VALUES(longitude);
            """)
            conn.execute(sql, {"code": r.station_code, "name": r.station_name, "lat": r.latitude, "lon": r.longitude})
    print("  -> dim_station loaded.")


def upsert_dim_time(ts_series):
    """Memperbarui atau menyisipkan (UPSERT) data waktu ke dim_time."""
    print("  -> Preparing dim_time...")
    uniq_ts = pd.to_datetime(ts_series).dt.floor('min').drop_duplicates().sort_values().reset_index(drop=True)
    if uniq_ts.empty:
        return

    # Membuat DataFrame Dimensi Waktu yang lengkap
    df = pd.DataFrame({'ts': uniq_ts})
    df['date'] = df['ts'].dt.date
    df['hour'] = df['ts'].dt.hour
    df['minute'] = df['ts'].dt.minute
    df['second'] = df['ts'].dt.second
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.isocalendar().week.astype(int)
    df['month'] = df['ts'].dt.month
    df['quarter'] = (df['ts'].dt.month - 1) // 3 + 1
    df['year'] = df['ts'].dt.year

    # Bulk INSERT IGNORE ke dim_time (Mengatasi duplikasi berdasarkan Primary Key 'ts')
    print(f"  -> Inserting {len(df)} unique timestamps into dim_time...")
    
    # Membuat tabel staging sementara
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS temp_dim_time"))
        conn.execute(text("""
            CREATE TEMPORARY TABLE temp_dim_time (
                ts DATETIME PRIMARY KEY,
                date DATE, hour INT, minute INT, second INT, day INT, week INT, month INT, quarter INT, year INT
            ) ENGINE=Memory;
        """))
        # Bulk insert ke tabel staging
        df.to_sql('temp_dim_time', con=conn, if_exists='append', index=False)
        
        # INSERT IGNORE dari staging ke dim_time
        insert_ignore_sql = text("""
            INSERT IGNORE INTO dim_time (ts, date, hour, minute, second, day, week, month, quarter, year)
            SELECT ts, date, hour, minute, second, day, week, month, quarter, year FROM temp_dim_time;
        """)
        conn.execute(insert_ignore_sql)
        print("  -> dim_time load complete.")

def load_fact_weather(df):
    """Memuat data fact ke fact_weather menggunakan staging table dan JOIN."""
    print("  -> Loading fact_weather...")
    
    # 1. Persiapan Data untuk Staging
    df_st = df[['ts','kode_lokasi','temperature','humidity','weather_desc']].copy()
    df_st['ts'] = df_st['ts'].dt.to_pydatetime() # konversi untuk kompatibilitas SQL

    # 2. Bulk Insert Staging
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS staging_weather"))
        conn.execute(text("""
            CREATE TEMPORARY TABLE staging_weather (
                ts DATETIME,
                kode_lokasi VARCHAR(50),
                temperature DOUBLE,
                humidity INT,
                weather_desc TEXT
            ) ENGINE=Memory
        """))
        df_st.to_sql('staging_weather', con=conn, if_exists='append', index=False)
        print(f"  -> Staging {len(df_st)} rows...")
        
        # 3. Insert into Fact Table dengan JOIN
        # Mencegah duplikasi data (Primary Key/Unique Key pada fact_weather harus berupa (time_id, station_id))
        insert_sql = text("""
            INSERT INTO fact_weather (time_id, station_id, temperature, humidity, weather_desc, created_at)
            SELECT t.time_id, s.station_id, st.temperature, st.humidity, st.weather_desc, NOW()
            FROM staging_weather st
            LEFT JOIN dim_station s ON s.station_code = st.kode_lokasi
            LEFT JOIN dim_time t ON t.ts = st.ts
            -- Pencegahan Duplikasi (hanya insert jika kombinasi time_id dan station_id belum ada)
            ON DUPLICATE KEY UPDATE 
                temperature = VALUES(temperature), humidity = VALUES(humidity), weather_desc = VALUES(weather_desc);
            
            -- Jika Anda tidak menggunakan Unique/Primary Key pada (time_id, station_id), gunakan subquery NOT EXISTS:
            /*
            WHERE NOT EXISTS (
                 SELECT 1 FROM fact_weather f
                 WHERE f.time_id = t.time_id AND f.station_id = s.station_id
             )
            */
        """)
        
        # Eksekusi Insert
        result = conn.execute(insert_sql)
        print(f"  -> {result.rowcount} rows inserted/updated in fact_weather.")
    print("  -> Load fact complete.")

# --- 5. Fungsi Utama (Orchestration) ---

def main():
    start_time = time.time()
    print(f"--- ETL Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    all_rows = []
    # E: Extract
    for nama, kode in WILAYAH.items():
        try:
            js = fetch_one(kode)
            df_loc = normalize_and_flatten(js, nama, kode)
            if df_loc.empty:
                print(f"  -> WARNING: No data or invalid structure for {nama}")
                continue
            all_rows.append(df_loc)
            print(f"  -> Extracted {len(df_loc)} rows for {nama}")
            # Jeda sebentar agar tidak membebani API
            time.sleep(1) 
        except requests.exceptions.HTTPError as he:
            print(f"  -> ERROR: HTTP Error fetching {nama}: {he}")
        except Exception as e:
            print(f"  -> CRITICAL ERROR fetching {nama}: {e}")
            continue

    if not all_rows:
        print("No data fetched successfully. ETL aborted.")
        return

    df_all = pd.concat(all_rows, ignore_index=True)
    
    # L: Load (Dimensi Stasiun)
    print("\n[STEP 1/3] Loading Dimension Tables (dim_station)...")
    df_stations = pd.DataFrame([
        {"station_code": v, "station_name": k, "latitude": None, "longitude": None} 
        for k,v in WILAYAH.items()
    ])
    upsert_dim_station(df_stations)

    # L: Load (Dimensi Waktu)
    print("\n[STEP 2/3] Loading Dimension Tables (dim_time)...")
    upsert_dim_time(df_all['ts']) # Hanya perlu menyisipkan, tidak perlu mengambil ID di Python
    
    # L: Load (Fact Table)
    print("\n[STEP 3/3] Loading Fact Table (fact_weather)...")
    load_fact_weather(df_all)
    
    end_time = time.time()
    print(f"\n--- ETL Complete. Total time: {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()