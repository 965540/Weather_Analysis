import pandas as pd
from sqlalchemy import create_engine

engine=create_engine("mysql+mysqlconnector://root:root@localhost:3306/weather_db")

query="SELECT * FROM weather_data"
df=pd.read_sql(query, con=engine)

df.to_csv("weather_data.csv", index=False)

print("Weather data exported to weather_data.csv successfully.")