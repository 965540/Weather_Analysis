import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

API_KEY = "9e9845701d917392cf7c0a17b508b970"

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
AQI_URL = "https://api.openweathermap.org/data/2.5/air_pollution"

TAMIL_NADU_DISTRICTS = [
    "Chennai", "Coimbatore", "Madurai", "Tiruchirappalli", "Salem",
    "Tirunelveli", "Thoothukudi", "Thanjavur", "Erode", "Vellore",
    "Dindigul", "Karur", "Namakkal", "Cuddalore", "Nagapattinam",
    "Tiruppur", "Kanchipuram", "Chengalpattu", "Villupuram",
    "Arcot", "Tiruvannamalai", "Krishnagiri",
    "Dharmapuri", "Perambalur", "Ariyalur", "Mayiladuthurai",
    "Tenkasi", "Virudhunagar", "Sivaganga", "Ramanathapuram",
    "Pudukkottai", "Ooty", "Theni"
]

def get_region_type(city):
    coastal = ["Chennai", "Cuddalore", "Nagapattinam", "Thoothukudi", "Ramanathapuram"]
    hill = ["Ooty", "Kodaikanal"]

    if city in coastal:
        return "Coastal"
    elif city in hill:
        return "Hill"
    else:
        return "Inland"

all_data = []

for city in TAMIL_NADU_DISTRICTS:
    weather_params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }

    weather_response = requests.get(WEATHER_URL, params=weather_params)
    weather_data = weather_response.json()

    if weather_response.status_code != 200:
        print(f"Failed for {city}")
        continue

    lat = weather_data["coord"]["lat"]
    lon = weather_data["coord"]["lon"]

    aqi_response = requests.get(
        AQI_URL,
        params={"lat": lat, "lon": lon, "appid": API_KEY}
    )
    aqi_data = aqi_response.json()

    aqi_value = None
    if "list" in aqi_data and len(aqi_data["list"]) > 0:
        aqi_value = aqi_data["list"][0]["main"]["aqi"]

    temperature = weather_data["main"]["temp"]
    humidity = weather_data["main"]["humidity"]
    wind_speed = weather_data["wind"]["speed"]

    clean_data = {
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "region_type": get_region_type(city),
        "datetime": datetime.fromtimestamp(weather_data["dt"]),
        "fetch_datetime": datetime.now(),
        "temperature": temperature,
        "humidity": humidity,
        "pressure": weather_data["main"]["pressure"],
        "wind_speed": wind_speed,
        "rainfall_1h": weather_data.get("rain", {}).get("1h", 0),
        "air_quality_index": aqi_value,
        "extreme_heat_alert": temperature > 32,
        "high_wind_alert": wind_speed > 6,
        "high_humidity_alert": humidity > 70,
        "weather_description": weather_data["weather"][0]["description"]
    }

    all_data.append(clean_data)
    print(f"Data collected for {city}")

df = pd.DataFrame(all_data)

engine = create_engine("mysql+mysqlconnector://root:root@localhost:3306/weather_db")

df.to_sql("weather_data", con=engine, if_exists="append", index=False)

print("weather data inserted successfully.")
