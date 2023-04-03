"""Python wrappers around demeter.weather SQL queries."""
import warnings
from datetime import date
from typing import Any, List, Union

from demeter.weather.query import get_daily_weather_types
from demeter.weather.utils.grid import get_cell_id

# from geo_utils.vector import pivot_geodataframe
from geopandas import GeoDataFrame, GeoSeries, read_postgis
from pandas import merge as pd_merge
from pandas import pivot_table, read_sql
from psycopg2.extensions import AsIs
from pyproj import CRS
from shapely.geometry import Point


def pivot_geodataframe(
    gdf: GeoDataFrame,
    index: Union[str, List[str]],
    columns: Union[str, List[str]],
    values: Union[str, List[str]] = None,
) -> GeoDataFrame:
    """
    Return reshaped GeoDataFrame organized by given index / column values.

    See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.pivot.html for more information.
    GeoDataFrame.pivot() raises a TypeError unless geometry colummn is converted to WKT. This function converts geometry
    column to WKT, performs the pivot, then returns the result as a GeoDataFrame, setting the geometry and CRS.

    Args:
        gdf (GeoDataFrame): Input GeoDataFrame (in the long/tidy format).
        index (Union[str, List[str]]): Column to use to make new frame's index. If None, uses existing index.
        columns (Union[str, List[str]]): Column to use to make new frame's columns.
        values (Union[str, List[str]], optional): Column(s) to use for populating new frame's values. If not specified,
        all remaining columns will be used and the result will have hierarchically indexed columns. Defaults to None.

    Returns:
        GeoDataFrame: Reshaped GeoDataFrame (in the "wide" format).
    """
    geom_name = gdf.geometry.name
    gdf_wkt = gdf.copy()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action="ignore",
            category=UserWarning,
            message="Geometry column does not contain geometry.",
        )
        gdf_wkt[geom_name] = gdf_wkt[geom_name].to_wkt()  # warning is filtered
    # df_pivot = (
    #     gdf_wkt.pivot(index=index, columns=columns, values=values)
    #     .rename_axis(None, axis=1)
    #     .reset_index()
    # )

    df_pivot = (
        pivot_table(gdf_wkt, index=index, columns=columns, values=values)
        .rename_axis(None, axis=1)
        .reset_index()
    )
    return GeoDataFrame(
        df_pivot, geometry=GeoSeries.from_wkt(df_pivot[geom_name]), crs=gdf.crs
    )


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
        conn (Connection): Active cursor to the database to query.
        coordinate_list (List[Point]): Spatial coordinates to query (must be lat/lng/WGS-84).
        startdate (datetime): Start date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        startdate (datetime): End date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        parameters (List): Weather parameters to retrieve weather data for. Must be present in `weather_types` table.
        wide (bool): If `True`, return data in the wide format; if `False`, return data in the long/tidy format.

    Returns:
        GeoDataFrame: With weather data for all coordinates and parameters between startdate and enddate (inclusive).
    """
    # get cell_id using demeter. The query below shouldn't ever take points, it should take cell_id
    cell_ids = [
        (
            get_cell_id(
                conn.connection.cursor(),
                geometry=point,
                geometry_crs=CRS.from_epsg(4326),
            ),
            point,
        )
        for point in coordinate_list
    ]
    df_coords = GeoDataFrame(
        cell_ids,
        columns=["cell_id", "geometry"],
        geometry="geometry",
        crs=CRS.from_epsg(4326),
    )
    cell_id_list = df_coords["cell_id"].unique().tolist()
    # The SQL should only take cell_id_list
    stmt = """
    WITH q2 AS (
        select d.cell_id, d.date_requested, d.daily_id, d.weather_type_id, d.date, d.value,
            ROW_NUMBER() OVER(PARTITION BY d.cell_id, d.weather_type_id, d.date ORDER BY d.date_requested desc) as rn
        FROM daily AS d
        WHERE cell_id in (%(cell_id_list)s) and
        d.date >= %(startdate)s and
        d.date <= %(enddate)s and
        weather_type_id in %(weather_type_ids)s
        GROUP BY d.cell_id, d.date_requested, d.daily_id
    )
    SELECT q2.cell_id, q2.date_requested, weather_type.weather_type, q2.date, q2.value, q2.weather_type_id
    FROM q2
    LEFT JOIN weather_type
    ON q2.weather_type_id = weather_type.weather_type_id
    WHERE rn = 1;
    """
    assert all(
        isinstance(c, int) for c in cell_id_list
    ), "`cell_ids` must be passed as a `integer`"
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
        "cell_id_list": AsIs(", ".join([str(c) for c in cell_id_list])),
        "startdate": startdate.strftime("%Y-%m-%d"),
        "enddate": enddate.strftime("%Y-%m-%d"),
        "weather_type_ids": tuple([x for x in param_dict.keys()]),
    }
    df_sql = read_sql(
        sql=stmt, con=conn, params=args, parse_dates=["date", "date_requested"]
    )

    # Now that we have data for each cell_id, join back to the input coordinate list and sort.
    gdf_sql = GeoDataFrame(
        pd_merge(left=df_coords, right=df_sql, how="left", on="cell_id"),
        geometry="geometry",
        crs=df_coords.crs,
    )
    gdf_sql.insert(
        len(gdf_sql.columns) - 1, "geometry", gdf_sql.pop("geometry")
    )  # Put geom column at end
    gdf_sql.insert(
        len(gdf_sql.columns), column="geometry_wkt", value=gdf_sql.geometry.to_wkt()
    )
    gdf_sql = (
        gdf_sql.sort_values(
            by=[
                "geometry_wkt",
                "weather_type_id",
                "date",
            ]  # Inheriting order of df_coords (from coordinate_list)
        )
        .drop(columns=["geometry_wkt", "weather_type_id"])
        .reset_index(drop=True)
    )

    if wide is True:
        gdf_sql.groupby(["date"]).count()

        return pivot_geodataframe(
            gdf_sql,
            index=[gdf_sql.geometry.name, "date"],
            columns="weather_type",
            values="value",
        )
    else:
        return gdf_sql


def query_daily_weather_sql(
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
        WHERE rn = 1
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

    if wide is True:
        return pivot_geodataframe(
            gdf_sql,
            index=[gdf_sql.geometry.name, "date"],
            columns="weather_type",
            values="value",
        )
    else:
        return gdf_sql
