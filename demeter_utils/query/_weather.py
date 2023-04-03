"""Python wrappers around demeter.weather SQL queries."""
import logging
from datetime import date
from typing import Any, List

from demeter.weather.query import get_cell_id, get_daily_weather_types
from geo_utils.vector import pivot_geodataframe
from geopandas import GeoDataFrame, read_postgis
from pandas import merge as pd_merge
from pandas import read_sql
from psycopg2.extensions import AsIs
from pyproj import CRS
from shapely import wkt
from shapely.geometry import Point


def _join_coordinates_to_unique_cell_ids(
    cursor: Any, coordinate_list: List[Point]
) -> GeoDataFrame:
    """
    Queries weather grid for all Points in coordinate list.

    Args:
        cursor (Any): Active cursor to the database.
        coordinate_list (List[Point]): Spatial coordinates to query (must be lat/lng/WGS-84). Note that if duplicate
        coordinates are present, all but one of the duplicate points are dropped by this function (warning is issued).

    Returns:
        GeoDataFrame: With "cell_id" and "geometry" (Point) columns.
    """
    coordinate_list_no_dups = list(set([c.wkt for c in coordinate_list]))
    if len(coordinate_list) != len(coordinate_list_no_dups):
        n_dups = len(coordinate_list) - len(coordinate_list_no_dups)

        def _find_duplicate_points(coordinate_list):
            seen = set()
            return set(
                [
                    x
                    for x in [c.wkt for c in coordinate_list]
                    if x in seen or seen.add(x)
                ]
            )

        msg = "".join(
            [
                "Duplicate Points in `coordinate_list` were detected; dropping %s Points from the query.\n",
                "List of duplicate points:\n    %s",
            ]
        )
        logging.warning(
            msg,
            n_dups,
            list(_find_duplicate_points(coordinate_list)),
        )
        coordinate_list = [wkt.loads(coords) for coords in coordinate_list_no_dups]
    cell_ids = [
        (
            get_cell_id(
                cursor,
                geometry=point,
                geometry_crs=CRS.from_epsg(4326),
            ),
            point,
        )
        for point in coordinate_list
    ]
    return GeoDataFrame(
        cell_ids,
        columns=["cell_id", "geometry"],
        geometry="geometry",
        crs=CRS.from_epsg(4326),
    )


def query_daily_weather(
    conn: Any,
    coordinate_list: List[Point],
    startdate: date,
    enddate: date,
    parameters: List,
    wide: bool = False,
    include_metadata: bool = False,
) -> GeoDataFrame:
    """
    Queries the `daily` weather table for based on lat/lngs, startdate, enddate, and parameter list.

    Args:
        conn (Connection): Active connection to the database.
        coordinate_list (List[Point]): Spatial coordinates to query (must be lat/lng/WGS-84).
        startdate (datetime): Start date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        startdate (datetime): End date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        parameters (List): Weather parameters to retrieve weather data for. Must be present in `weather_types` table.
        wide (bool): If `True`, return data in the wide format; if `False`, return data in the long/tidy format (default
        is False).
        include_metadata (bool): If `True`, return `cell_id` and `date_requested` as extra columns to the output
        GeoDataFrame (default is False).

    Note:
        Any duplicate Points in `coordinate_list` are removed (warning is issued).

    Returns:
        GeoDataFrame: With weather data for all coordinates and parameters between startdate and enddate (inclusive).
    """
    msg = (
        "`wide` and `include_metadata` are both be set to `True`, which is not supported; set `wide` to `False` if you "
        + "want metadata returned."
    )
    assert not all((include_metadata is True, wide is True)), msg

    df_coords = _join_coordinates_to_unique_cell_ids(
        conn.connection.cursor(), coordinate_list
    )  # uses get_cell_id()
    cell_id_list = (
        df_coords["cell_id"].unique().tolist()
    )  # The SQL should only take cell_id_list
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
    args = {
        "cell_id_list": AsIs(", ".join([str(c) for c in cell_id_list])),
        "startdate": startdate.strftime("%Y-%m-%d"),
        "enddate": enddate.strftime("%Y-%m-%d"),
        "weather_type_ids": tuple([x for x in param_dict.keys()]),
    }
    # TODO: Should we raise a special error if user tries to get daily weather for cell_id that isn't populated?
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

    if include_metadata is False:  # wide must be `False` because of the assert above
        gdf_sql.drop(columns=["cell_id", "date_requested"], inplace=True)
    if wide is False:
        return gdf_sql
    else:
        return pivot_geodataframe(
            gdf_sql,
            index=[gdf_sql.geometry.name, "date"],
            columns="weather_type",
            values="value",
        )


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
