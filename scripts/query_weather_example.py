# %% Imports
from contextlib import contextmanager
from datetime import date
from time import perf_counter as press_button

from demeter.db import getConnection
from demeter.weather.initialize.weather_types import DAILY_WEATHER_TYPES
from dotenv import load_dotenv
from shapely.geometry import Point

from demeter_utils.query import query_daily_weather

# %% Connect to database
c = load_dotenv()

conn = getConnection(env_name="DEMETER-DEV_LOCAL")
cursor = conn.connection.cursor()

# %% Time context manager


@contextmanager
def catchtime() -> float:
    t1 = t2 = press_button()
    yield lambda: t2 - t1
    t2 = press_button()


# %% Set parameters and make query
PARAMETERS_ALL = [t["weather_type"] for t in DAILY_WEATHER_TYPES]
parameters = [
    "t_mean_2m_24h:C",
    "precip_24h:mm",
    "wind_speed_mean_2m_24h:ms",
]

coordinate_list = [
    Point(-90.612269, 44.723885),
    Point(-90.612249, 44.723825),
    Point(-90.596003, 44.684680),
    Point(-90.636620, 44.690766),
    Point(-90.636621, 44.690766),
    Point(-90.636622, 44.690766),
    Point(-90.636623, 44.690766),
    Point(-90.636624, 44.690766),
    Point(-90.636625, 44.690766),
    Point(-90.636626, 44.690766),
    Point(-90.636627, 44.690766),
    Point(-90.636628, 44.690766),
    Point(-90.636629, 44.690766),
    Point(-90.636620, 44.690767),
    Point(-90.636621, 44.690767),
    Point(-90.636622, 44.690767),
    Point(-90.636623, 44.690767),
    Point(-90.636624, 44.690767),
    Point(-90.636625, 44.690767),
    Point(-90.636626, 44.690767),
    Point(-90.636627, 44.690767),
    Point(-90.636628, 44.690767),
    Point(-90.636629, 44.690767),
    Point(-90.636620, 44.690768),
    Point(-90.636621, 44.690768),
    Point(-90.636622, 44.690768),
    Point(-90.636623, 44.690768),
    Point(-90.636624, 44.690768),
    Point(-90.636625, 44.690768),
    Point(-90.636626, 44.690768),
    Point(-90.636627, 44.690768),
    Point(-90.636628, 44.690768),
    Point(-90.636629, 44.690768),
    Point(-90.636620, 44.690769),
    Point(-90.636621, 44.690769),
    Point(-90.636622, 44.690769),
    Point(-90.636623, 44.690769),
    Point(-90.636624, 44.690769),
    Point(-90.636625, 44.690769),
    Point(-90.636626, 44.690769),
    Point(-90.636627, 44.690769),
    Point(-90.636628, 44.690769),
    Point(-90.636629, 44.690769),
    Point(-90.63662, 44.690766),
    Point(-90.636626, 44.690766),
    Point(-90.636626, 44.690766),
    Point(-90.636626, 44.690766),
]
startdate = date(2013, 1, 1)
enddate = date(2023, 3, 31)


# %% Run query

with catchtime() as t:
    gdf_sql = query_daily_weather(
        conn=conn,
        # cursor=cursor,
        coordinate_list=coordinate_list,
        startdate=startdate,
        enddate=enddate,
        parameters=PARAMETERS_ALL,
        wide=False,
    )
print(f"query_daily_weather() time: {t():.1f} seconds")
