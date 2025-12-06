# ETL: BMKG API -> MySQL (star schema ready)
import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()

DB_URL = os.getenv("DB_URL")
API_BASE = os.getenv("BMKG_API")

# 3 wilayah yang kamu sebutkan
WILAYAH = {
    "Keputih": "35.78.09.1001",
    "GebangPutih": "35.78.09.1002",
    "KlampisNgasem": "35.78.09.1003"
}

engine = create_engine(DB_URL, pool_pre_ping=True)

def fetch_one(kode):
    url = f"{API_BASE}{kode}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def normalize_and_flatten(json_obj, lokasi_name, kode):
    # Adjust depending on exact BMKG response structure.
    # Example uses fields: data[...] with time+parameters.
    rows = []
    # If response contains "data" or "prakiraan" adjust path as necessary
    # Common pattern: json['data'] is a list; inspect sample.
    # We'll try to be defensive:
    if isinstance(json_obj, dict):
        # Many BMKG endpoints return key 'data' or 'prakiraan'; inspect givent sample:
        payload_list = None
        if 'data' in json_obj and isinstance(json_obj['data'], list):
            payload_list = json_obj['data']
        elif 'prakiraan' in json_obj and isinstance(json_obj['prakiraan'], list):
            payload_list = json_obj['prakiraan']
        elif 'list' in json_obj and isinstance(json_obj['list'], list):
            payload_list = json_obj['list']
        else:
            # try root as list
            if isinstance(json_obj, list):
                payload_list = json_obj
        if not payload_list:
            # fallback: convert whole object
            payload_list = [json_obj]

        for item in payload_list:
            # common keys: 'time', 't','hu','weather_desc' — adapt to actual response
            # try multiple common key names
            ts = None
            if 'time' in item:
                ts = item['time']
            elif 'jam' in item:
                ts = item['jam']
            elif 'datetime' in item:
                ts = item['datetime']
            else:
                # maybe there is attribute '@datetime' inside nested
                if isinstance(item, dict):
                    for k in ['@datetime','@time']:
                        if k in item:
                            ts = item[k]
                            break
            # temperature and humidity
            t = item.get('t') or item.get('temperature') or item.get('temp')
            hu = item.get('hu') or item.get('humidity')
            weather_desc = item.get('weather_desc') or item.get('keterangan') or item.get('cuaca') or item.get('desc')

            try:
                ts_parsed = pd.to_datetime(ts)
            except Exception:
                ts_parsed = pd.Timestamp.utcnow()

            rows.append({
                "lokasi": lokasi_name,
                "kode_lokasi": kode,
                "ts": ts_parsed,
                "temperature": float(t) if t is not None else None,
                "humidity": int(hu) if hu is not None else None,
                "weather_desc": weather_desc
            })
    return pd.DataFrame(rows)

def upsert_dim_station(df_station):
    # df_station: columns station_code, station_name, latitude, longitude (latitude optional)
    # upsert using INSERT ... ON DUPLICATE KEY UPDATE
    with engine.begin() as conn:
        for _, r in df_station.iterrows():
            sql = text("""
            INSERT INTO dim_station (station_code, station_name, latitude, longitude)
            VALUES (:code, :name, :lat, :lon)
            ON DUPLICATE KEY UPDATE station_name=VALUES(station_name), latitude=VALUES(latitude), longitude=VALUES(longitude)
            """)
            conn.execute(sql, {"code": r.station_code, "name": r.station_name, "lat": r.latitude, "lon": r.longitude})

def upsert_dim_time_and_get_ids(ts_series):
    # ts_series: pandas Series of Timestamps
    # Insert unique timestamps into dim_time (with ON DUPLICATE KEY IGNORE)
    uniq_ts = pd.to_datetime(ts_series).drop_duplicates().sort_values()
    # prepare dataframe
    rows = []
    for ts in uniq_ts:
        rows.append({
            "ts": ts,
            "date": ts.date(),
            "hour": ts.hour,
            "minute": ts.minute,
            "second": ts.second,
            "day": ts.day,
            "week": int(ts.isocalendar()[1]),
            "month": ts.month,
            "quarter": (ts.month-1)//3 + 1,
            "year": ts.year
        })
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    # Insert using sqlalchemy to allow ON DUPLICATE KEY IGNORE: MySQL doesn't have IGNORE with SQLAlchemy easily,
    # so we use raw insertion with INSERT IGNORE
    with engine.begin() as conn:
        for _, r in df.iterrows():
            sql = text("""
            INSERT IGNORE INTO dim_time (ts, date, hour, minute, second, day, week, month, quarter, year)
            VALUES (:ts, :date, :hour, :minute, :second, :day, :week, :month, :quarter, :year)
            """)
            conn.execute(sql, {
                "ts": r['ts'].to_pydatetime(),
                "date": r['date'],
                "hour": int(r['hour']),
                "minute": int(r['minute']),
                "second": int(r['second']),
                "day": int(r['day']),
                "week": int(r['week']),
                "month": int(r['month']),
                "quarter": int(r['quarter']),
                "year": int(r['year'])
            })
    # now query IDs
    mapping = {}
    q = text("SELECT time_id, ts FROM dim_time WHERE ts IN :ts_list")
    # fetch mapping
    # sqlalchemy requires tuple, so:
    ts_tuple = tuple(pd.to_datetime(uniq_ts).astype(str).tolist())
    sel = text("SELECT time_id, ts FROM dim_time WHERE ts IN (" + ",".join(["'{}'".format(str(ts)) for ts in uniq_ts]) + ")")
    with engine.connect() as conn:
        res = conn.execute(sel).fetchall()
        for time_id, ts in res:
            mapping[pd.to_datetime(ts)] = time_id
    return mapping

def load_fact_weather(df):
    # expects df columns: ts, temperature, humidity, weather_desc, lokasi, kode_lokasi
    # We'll resolve time_id and station_id via SQL joins (faster): use temporary staging table
    with engine.begin() as conn:
        # create staging temp table
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
        # bulk insert staging via pandas
        df_st = df[['ts','kode_lokasi','temperature','humidity','weather_desc']].copy()
        df_st['ts'] = df_st['ts'].dt.to_pydatetime()
        df_st.to_sql('staging_weather', con=engine, if_exists='append', index=False)
        # insert into fact by joining dim tables
        insert_sql = text("""
            INSERT INTO fact_weather (time_id, station_id, temperature, humidity, weather_desc, created_at)
            SELECT t.time_id, s.station_id, st.temperature, st.humidity, st.weather_desc, NOW()
            FROM staging_weather st
            LEFT JOIN dim_station s ON s.station_code = st.kode_lokasi
            LEFT JOIN dim_time t ON t.ts = st.ts
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_weather f
                WHERE f.time_id = t.time_id AND f.station_id = s.station_id
            )
        """)
        conn.execute(insert_sql)

def main():
    all_rows = []
    for nama, kode in WILAYAH.items():
        try:
            js = fetch_one(kode)
            df_loc = normalize_and_flatten(js, nama, kode)
            if df_loc.empty:
                continue
            all_rows.append(df_loc)
            print(f"Fetched {len(df_loc)} rows for {nama}")
        except Exception as e:
            print("Error fetching", nama, e)
            continue

    if not all_rows:
        print("No data fetched.")
        return

    df_all = pd.concat(all_rows, ignore_index=True)
    # upsert station info minimal
    df_stations = pd.DataFrame([{"station_code": v, "station_name": k, "latitude": None, "longitude": None} for k,v in WILAYAH.items()])
    upsert_dim_station(df_stations)

    # upsert time dimension
    upsert_dim_time_and_get_ids(df_all['ts'])

    # load fact table using staging join
    load_fact_weather(df_all)
    print("Load complete.")

if __name__ == "__main__":
    main()
