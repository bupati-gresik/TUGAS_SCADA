from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Weather(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, index=True)
    kode_wilayah = Column(String(50))
    cuaca = Column(String(200))
    suhu = Column(String(50))
    kelembaban = Column(String(50))
    timestamp = Column(DateTime)
