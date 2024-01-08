"""Python wrappers around demeter.weather SQL queries."""
import logging
from datetime import date
from typing import Any, List

from demeter.weather.query import get_cell_id, get_daily_weather_types
from geo_utils.vector import pivot_geodataframe
from geopandas import GeoDataFrame
from pandas import merge as pd_merge
from pandas import read_sql
from pyproj import CRS
from shapely import wkt
from shapely.geometry import Point


def find_duplicate_points(coordinate_list: List[Point]) -> List:
    """
    Identifies duplicate Points in `coordinate_list`.

    Args:
        coordinate_list (List[Point]): Spatial coordinates (must be lat/lng/WGS-84).

    Returns:
        List: WTK points that appear two or more times in `coordinate_list`.
    """
    seen = set()
    return list(
        set([x for x in [c.wkt for c in coordinate_list] if x in seen or seen.add(x)])
    )


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
        GeoDataFrame: "geometry" that is a copy of the input Points from `coordinate_list`, with a "cell_id" column
        joined representing each coordinate's `cell_id` in the demeter weather grid.
    """
    coordinate_list_no_dups = list(set([c.wkt for c in coordinate_list]))
    if len(coordinate_list) != len(coordinate_list_no_dups):
        n_dups = len(coordinate_list) - len(coordinate_list_no_dups)

        msg = "".join(
            [
                "Duplicate Points in `coordinate_list` were detected; dropping %s Points from the query.\n",
                "List of duplicate points:\n    %s",
            ]
        )
        logging.warning(
            msg,
            n_dups,
            find_duplicate_points(coordinate_list),
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
    # conn: Any,
    cursor: Any,
    coordinate_list: List[Point],
    startdate: date,
    enddate: date,
    parameters: List,
    wide: bool = False,
    include_metadata: bool = False,
) -> GeoDataFrame:
    """
    Queries the `daily` weather table based on lat/lngs, startdate, enddate, and parameter list.

    Args:
        conn (Connection): Active connection to the database.
        coordinate_list (List[Point]): Spatial coordinates to query (must be lat/lng/WGS-84).
        startdate (datetime): Start date of the query (inclusive); e.g., `date(2023, 3, 28)`.
        enddate (datetime): End date of the query (inclusive); e.g., `date(2023, 4, 4)`.
        parameters (List): Weather parameters to retrieve weather data for. Must be present in `weather_types` table.
        wide (bool): If `True`, return data in the wide format; if `False`, return data in the long/tidy format (default
        is False).
        include_metadata (bool): If `True`, return `cell_id` and `date_requested` as extra columns to the output
        GeoDataFrame; ignored if `wide == True` (default is False).

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
        # conn.connection.cursor(),
        cursor,
        coordinate_list,
    )  # uses get_cell_id()
    cell_id_list = (
        df_coords["cell_id"].unique().tolist()
    )  # The SQL should only take cell_id_list
    assert all(
        isinstance(c, int) for c in cell_id_list
    ), "`cell_ids` must be passed as a `integer`"
    df_params = get_daily_weather_types(
        # conn.connection.cursor(),
        cursor
    )[["weather_type_id", "weather_type"]]
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
        WHERE cell_id in %(cell_id_tuple)s and
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
        "cell_id_tuple": tuple([str(c) for c in cell_id_list]),
        "startdate": startdate.strftime("%Y-%m-%d"),
        "enddate": enddate.strftime("%Y-%m-%d"),
        "weather_type_ids": tuple([x for x in param_dict.keys()]),
    }

    # TODO: Should we raise a special error if user tries to get daily weather for cell_id that isn't populated?
    df_sql = read_sql(
        sql=stmt,
        # con=conn,
        con=cursor.conneciton,
        params=args,
        parse_dates=["date", "date_requested"],
    )

    # Now that we have data for each cell_id, join back to the input coordinate list and sort.
    gdf_sql = GeoDataFrame(
        pd_merge(left=df_coords.reset_index(), right=df_sql, how="left", on="cell_id"),
        geometry="geometry",
        crs=df_coords.crs,
    )

    gdf_sql.insert(
        len(gdf_sql.columns) - 1, "geometry", gdf_sql.pop("geometry")
    )  # Put geom column at end

    gdf_sql = (
        gdf_sql.sort_values(
            by=[
                "index",
                "weather_type_id",
                "date",
            ]  # Inheriting order of df_coords (from coordinate_list)
        )
        .drop(columns=["weather_type_id"])
        .reset_index(drop=True)
    )

    if include_metadata is False:  # wide must be `False` because of the assert above
        gdf_sql.drop(columns=["cell_id", "date_requested"], inplace=True)

    if wide is False:
        gdf_sql.drop(columns="index", inplace=True)
        return gdf_sql
    else:
        return pivot_geodataframe(
            gdf_sql,
            index=[gdf_sql.geometry.name, "date"],
            columns="weather_type",
            values="value",
            spatial_index="index",
        ).drop(columns="index")
