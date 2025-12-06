from fastapi import FastAPI, Depends
import requests
from sqlalchemy.orm import Session
from database import get_db, engine
from models import Base, Weather
from datetime import datetime

Base.metadata.create_all(bind=engine)

app = FastAPI()

# URL BMKG yang benar
BMKG_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"

@app.get("/")
def index():
    return {"message": "BMKG Weather Proxy API is running!"}

@app.get("/weather/{kode}")
def get_weather(kode: str, db: Session = Depends(get_db)):
    """
    Mengambil data cuaca dari API BMKG berdasarkan kode ADM4 (?adm4=xxx)
    lalu menyimpannya ke database.
    """

    # Contoh: ?adm4=31.71.03.1001
    url = f"{BMKG_URL}?adm4={kode}"

    response = requests.get(url)

    if response.status_code != 200:
        return {
            "error": "Gagal mengambil data BMKG",
            "status": response.status_code,
            "url": url
        }

    data = response.json()

    # Pastikan format data sesuai
    # BMKG biasanya return: { "data": { "parameter": [ ... ] } }
    if "data" not in data:
        return {"error": "Format data BMKG tidak sesuai", "raw": data}

    # DATA CUACA BIASANYA ADA DI DALAM:
    # data["data"]["cuaca"]["value"]
    cuaca_data = data["data"]

    # Extract contoh parameter (disesuaikan dengan struktur yang BMKG kirim)
    kondisi = cuaca_data.get("cuaca", {}).get("value", "-")
    suhu = cuaca_data.get("t", {}).get("value", "-")
    kelembaban = cuaca_data.get("hu", {}).get("value", "-")

    # Simpan ke database
    save_data = Weather(
        kode_wilayah=kode,
        cuaca=str(kondisi),
        suhu=str(suhu),
        kelembaban=str(kelembaban),
        timestamp=datetime.now()
    )

    db.add(save_data)
    db.commit()

    return {
        "kode_wilayah": kode,
        "cuaca": kondisi,
        "suhu": suhu,
        "kelembaban": kelembaban,
        "sumber": url
    }
