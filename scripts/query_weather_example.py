# %% Imports
from datetime import date

from demeter.db import getConnection
from dotenv import load_dotenv
from shapely.geometry import Point

from demeter_utils.query import query_daily_weather

# %% Connect to database
c = load_dotenv()

conn = getConnection(env_name="DEMETER-DEV_LOCAL")
cursor = conn.connection.cursor()

# %% Set parameters and make query
parameters = [
    "t_min_2m_24h:C",
    "t_max_2m_24h:C",
    "t_mean_2m_24h:C",
    "precip_24h:mm",
    "wind_speed_mean_2m_24h:ms",
    "relative_humidity_mean_2m_24h:p",
    "global_rad_24h:J",
]
coordinate_list = [
    Point(-90.612269, 44.723885),
    Point(-90.612249, 44.723825),
    Point(-90.596003, 44.684680),
    Point(-90.636626, 44.690766),
]
startdate = date(2013, 1, 1)
enddate = date(2023, 3, 31)

gdf_sql = query_daily_weather(
    conn,
    coordinate_list,
    startdate,
    enddate,
    parameters,
    wide=False,
)

# %%
