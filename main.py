from fastapi import FastAPI, Depends
import requests
from sqlalchemy.orm import Session
from database import get_db, engine
from models import Base, Weather
from datetime import datetime

Base.metadata.create_all(bind=engine)

app = FastAPI()

BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"

@app.get("/")
def index():
    return {"message": "BMKG Weather API is running!"}


@app.get("/weather/{kode}")
def get_weather(kode: str, db: Session = Depends(get_db)):

    url = f"{BMKG_URL}?adm4={kode}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.bmkg.go.id/",
        "Origin": "https://www.bmkg.go.id"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return {"error": "Gagal mengambil data BMKG", "status": response.status_code, "url": url}

    data = response.json()

    if "data" not in data or len(data["data"]) == 0:
        return {"error": "Struktur JSON tidak sesuai", "contoh": data}

    # Ambil data cuaca pertama
    cuaca_data = data["data"][0]["cuaca"]
    first_item = cuaca_data[0][0]  # data jam terdekat

    kondisi = first_item.get("weather_desc", "-")
    suhu = first_item.get("t", "-")
    kelembaban = first_item.get("hu", "-")

    # Simpan DB
    weather = Weather(
        kode_wilayah=kode,
        cuaca=str(kondisi),
        suhu=str(suhu),
        kelembaban=str(kelembaban),
        timestamp=datetime.now()
    )
    db.add(weather)
    db.commit()
    db.refresh(weather)

    return {
        "status": "Data berhasil disimpan ke database!",
        "kode": kode,
        "cuaca": kondisi,
        "suhu": suhu,
        "kelembaban": kelembaban
    }
