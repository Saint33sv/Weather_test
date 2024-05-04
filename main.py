import asyncio
import openpyxl
import openmeteo_requests
import requests_cache
import argparse

from retry_requests import retry
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime


Base = declarative_base()


# Класс для хранения данных погоды в базе данных
class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    temperature = Column(Float)
    precipitation_amount = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(Float)
    
    

# Инициализация базы данных
engine = create_engine('sqlite:///weather.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Функция для запроса данных о погоде через API и добавления их в базу данных


async def fetch_weather_data():
    while True:
        # Запрос данных о погоде через API
        cache_session = requests_cache.CachedSession(
            '.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 55.7007,
            "longitude": 37.36185,
            "current": ["temperature_2m", "precipitation", "pressure_msl", "wind_speed_10m", "wind_direction_10m"],
            "timezone": "Europe/Moscow",
            "forecast_days": 1
        }
        responses = openmeteo.weather_api(url, params=params)

        response = responses[0]
        current = response.Current()

        # Извлечение данных о погоде из ответа API и добавление их в базу данных
        weather_data = WeatherData(
            timestamp=datetime.now(),
            temperature=current.Variables(0).Value(),
            precipitation_amount=current.Variables(1).Value(),
            pressure=current.Variables(2).Value(),
            wind_speed=current.Variables(3).Value(),
            wind_direction=current.Variables(4).Value(),
            
        )
        session.add(weather_data)
        session.commit()

        await asyncio.sleep(5)  # Пауза в 3 минуты


# Функция для экспорта данных из базы данных в Excel файл
async def export_to_excel():
    while True:
        # Запрос последних 10 записей из базы данных
        weather_records = session.query(WeatherData).order_by(
            WeatherData.timestamp.desc()).limit(10).all()

        # Создание Excel файла и запись данных
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['Timestamp', 'Temperature', 'Precipitation Amount', 'Pressure', 'Wind Speed', 'Wind Direction'])
        for record in weather_records:
            ws.append([record.timestamp, record.temperature, record.precipitation_amount, record.pressure, 
                       record.wind_speed, record.wind_direction])

        wb.save('weather_data.xlsx')

        await asyncio.sleep(60)  # Пауза в 10 минут

# Запуск асинхронных функций
async def main():
    await asyncio.gather(fetch_weather_data(), export_to_excel())

# Парсинг аргументов
parser = argparse.ArgumentParser()
parser.add_argument('-a', '--action', help="Write file", default="False")

args = parser.parse_args()

if __name__ == "__main__":
    if args.action == "True":
        asyncio.run(main())
    else:
        asyncio.run(fetch_weather_data())
