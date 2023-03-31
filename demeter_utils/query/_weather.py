"""Python wrappers around demeter.weather SQL queries."""
from datetime import date
from typing import Any, List

from demeter.weather.query import get_daily_weather_types
from geo_utils.vector import pivot_geodataframe
from geopandas import GeoDataFrame, read_postgis
from psycopg2.extensions import AsIs
from pyproj import CRS
from shapely.geometry import Point


def query_daily_weather(
    conn: Any,
    coordinate_list: List[Point],
    startdate: date,
    enddate: date,
    parameters: List,
    wide: bool = False,
) -> GeoDataFrame:
    """
    Queries the `daily` weather table for based on lat/lngs, startdate, enddate, and parameter list.

    Args:
        conn (Connection): Active connection to the database to query.
        coordinate_list (List[Point]): Spatial coordinates to query (must be lat/lng/WGS-84).
        startdate (datetime): Start date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        startdate (datetime): End date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        parameters (List): Weather parameters to retrieve weather data for. Must be present in `weather_types` table.
        wide (bool): If `True`, return data in the wide format; if `False`, return data in the long/tidy format.

    Returns:
        GeoDataFrame: With weather data for all coordinates and parameters between startdate and enddate (inclusive).
    """
    # get cell_id using demeter. The query below shouldn't ever take points, it should take
    # cursor = conn
    # for point in coordinate_list:
    #     get_cell_id(cursor, geometry, geometry_crs=CRS.from_epsg(4326))
    # The SQL should only take cell_id
    stmt = """
    WITH pairs(x,y) AS (
    VALUES %(coordinate_list)s
    ), q1 AS (
        SELECT ST_Point(x, y, 4326) as query_point, (ST_Value(raster_5km.rast_cell_id,ST_Transform(ST_Point(x, y, 4326), world_utm.raster_epsg)))::INTEGER AS cell_id
        FROM world_utm, raster_5km
            CROSS JOIN pairs
            WHERE ST_intersects(ST_Point(x, y, 4326), world_utm.geom) AND
            world_utm.world_utm_id=raster_5km.world_utm_id
    ), q2 AS (
        select q1.cell_id, d.date_requested, d.daily_id, d.weather_type_id, d.date, d.value,
            ROW_NUMBER() OVER(PARTITION BY d.cell_id, d.weather_type_id, d.date ORDER BY d.date_requested desc) as rn
        FROM daily AS d
        INNER JOIN q1
        ON q1.cell_id = d.cell_id
        where d.date >= %(startdate)s and
        d.date <= %(enddate)s and
        weather_type_id in %(weather_type_ids)s
        GROUP BY q1.cell_id, d.date_requested, d.daily_id
    ), q3 AS (
        SELECT q2.cell_id, q2.date_requested, weather_type.weather_type, q2.date, q2.value, q2.weather_type_id
        FROM q2
        LEFT JOIN weather_type
        ON q2.weather_type_id = weather_type.weather_type_id
    )
    SELECT q3.cell_id, q3.date_requested, q3.weather_type, q3.date, q3.value, q1.query_point AS geometry
    FROM q1
    LEFT JOIN q3
    ON q1.cell_id = q3.cell_id
    ORDER BY q1.query_point, q3.weather_type_id, q3.date;
    """
    assert all(
        isinstance(p, Point) for p in coordinate_list
    ), "`point` must be passed as a `Point`"
    df_params = get_daily_weather_types(conn.connection.cursor())[
        ["weather_type_id", "weather_type"]
    ]
    for p in parameters:
        assert (
            p in df_params["weather_type"].to_list()
        ), f'Weather Type "{p}" is not present in weather_type table.'
    param_dict = (
        df_params[df_params["weather_type"].isin(parameters)]
        .set_index("weather_type_id")
        .to_dict()["weather_type"]
    )
    args = {
        "coordinate_list": AsIs(
            ", ".join([str(tuple((p.x, p.y))) for p in coordinate_list])
        ),
        "startdate": startdate.strftime("%Y-%m-%d"),
        "enddate": enddate.strftime("%Y-%m-%d"),
        "weather_type_ids": tuple([x for x in param_dict.keys()]),
    }

    gdf_sql = read_postgis(
        sql=stmt,
        con=conn,
        params=args,
        geom_col="geometry",
        crs=CRS.from_epsg(4326),
        parse_dates=["date", "date_requested"],
    )

    # After, join the query response to the input coordinate list.

    if wide is True:
        return pivot_geodataframe(
            gdf_sql,
            index=[gdf_sql.geometry.name, "date"],
            columns="weather_type",
            values="value",
        )
    else:
        return gdf_sql
