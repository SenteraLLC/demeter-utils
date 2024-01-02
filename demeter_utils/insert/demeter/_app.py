import logging
from dataclasses import asdict
from typing import Union

from demeter.data import App, insertOrGetApp
from geopandas import GeoDataFrame
from pandas import DataFrame, isna
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm

from demeter_utils.insert.demeter._core import DEMETER_IDS
from demeter_utils.insert.demeter._crop_type import insert_or_get_crop_type
from demeter_utils.insert.demeter._nutrient_source import insert_or_get_nutrient_source
from demeter_utils.query.demeter import basic_demeter_query
from demeter_utils.update import update_details

APP_TYPE_ENUM = [
    "BIOLOGICAL",
    "FERTILIZER",
    "FUNGICIDE",
    "HERBICIDE",
    "INHIBITOR",
    "INSECTICIDE",
    "IRRIGATION",
    "LIME",
    "MANURE",
    "NEMATICIDE",
    "STABILIZER",
]


def insert_or_get_app(
    cursor: NamedTupleCursor,
    organization_id: int,
    df_application: Union[DataFrame, GeoDataFrame],
    df_demeter_object: Union[DataFrame, GeoDataFrame],
    demeter_object_join_cols: list[str] = ["field_trial_name"],
    app_type_col: str = "APP_TYPE",
    app_method_col: str = "APP_METHOD",
    app_date_col: str = "DATE_APPLIED",
    app_product_col: str = "PRODUCT",
    app_rate_col: str = "RATE",
    app_rate_unit_col: str = "RATE_UNIT",
    crop_col: str = None,
    product_name_col: str = None,
    df_nutrient_sources: DataFrame = None,
    nutrient_source_col: str = None,
    app_details_col_list: list = [],
) -> tuple[DataFrame, DataFrame]:
    """
    Insert Field Applications and [optional] CropType or NutrientSource for multiple demeter objects (i.e., fields,
    field_trials, or plots).
    """
    # Passing a crop_type to Application table is optional
    df_crop_types = (
        insert_or_get_crop_type(cursor, df_application, crop_col, product_name_col)
        if any([crop_col, product_name_col])
        else None
    )

    # Passing a nutrient_source to Application table is optional
    df_nutrient_sources_ = (
        insert_or_get_nutrient_source(
            cursor, organization_id, df_nutrient_sources, nutrient_source_col
        )
        if all([len(df_nutrient_sources.columns), nutrient_source_col])
        else None
    )

    logging.info("  Creating Applications dataframe from management data")
    df_app = _build_application_dataframe(
        df_application,
        df_demeter_object,
        demeter_object_join_cols,
        app_type_col,
        app_method_col,
        app_date_col,
        app_product_col,
        app_rate_col,
        app_rate_unit_col,
        app_details_col_list,
        df_crop_types,
        crop_col,
        product_name_col,
        df_nutrient_sources_,
        nutrient_source_col,  # Should this be a dict for all nutrient_source columns?
    )

    logging.info("  Inserting %s Applications", list(df_app[app_type_col].unique()))
    df_app = _insert_or_update_app(
        cursor,
        df_app,
        app_type_col,
        app_method_col,
        app_date_col,
        app_rate_col,
        app_rate_unit_col,
        app_details_col_list,
    )
    return df_crop_types, df_nutrient_sources, df_app


def _build_application_dataframe(
    df_application: DataFrame,
    df_demeter_object: GeoDataFrame,
    demeter_object_join_cols: list[str] = ["field_trial_name"],
    app_type_col: str = "APP_TYPE",
    app_method_col: str = "APP_METHOD",
    app_date_col: str = "DATE_APPLIED",
    app_product_col: str = "PRODUCT",
    app_rate_col: str = "RATE",
    app_rate_unit_col: str = "RATE_UNIT",
    app_details_col_list: list = [],
    df_crop_types: DataFrame = None,
    crop_col: str = None,
    product_name_col: str = None,
    df_nutrient_sources: DataFrame = None,
    nutrient_source_col: str = None,
) -> DataFrame:
    # Get all column in df_demeter_object.columns that begin with  any of the strings in DEMETER_IDS
    cols_demeter_ids = [
        c
        for c in df_demeter_object.columns
        if any(c.startswith(s) for s in DEMETER_IDS)
    ]
    # Separate out crop_types in case they are not provided (df_crop_types is optional).
    cols_crop_types = (
        [
            crop_col,
            product_name_col,
        ]
        if df_crop_types is not None
        else []
    )
    # Separate out nutrient_sources in case they are not provided (df_nutrient_sources is optional).
    cols_nutrient_sources = (
        [
            nutrient_source_col,
        ]
        if df_nutrient_sources is not None
        else []
    )
    df_app_ = (
        df_application[
            [
                app_type_col,
                app_method_col,
                app_date_col,
                app_product_col,
                app_rate_col,
                app_rate_unit_col,
            ]
            + demeter_object_join_cols
            + cols_crop_types
            # + cols_nutrient_sources
            + app_details_col_list
        ]
        .drop_duplicates()
        .reset_index(drop=True)
        .merge(
            df_demeter_object[demeter_object_join_cols + cols_demeter_ids],
            on=demeter_object_join_cols,
        )
    )
    df_app2 = (
        df_app_.merge(df_crop_types, on=cols_crop_types)
        if df_crop_types is not None
        else df_app_
    )

    # Have to use left_on/right_on because app_product_col is not necessarily similar to cols_nutrient_sources
    df_app = (
        # df_app2.merge(df_nutrient_sources, on=cols_nutrient_sources)
        df_app2.merge(
            df_nutrient_sources, left_on=app_product_col, right_on=cols_nutrient_sources
        )
        if df_nutrient_sources is not None
        else df_app2
    )
    return df_app


def _insert_or_update_app(
    cursor: NamedTupleCursor,
    df_app: DataFrame,
    app_type_col: str = "APP_TYPE",
    app_method_col: str = "APP_METHOD",
    app_date_col: str = "DATE_APPLIED",
    app_rate_col: str = "RATE",
    app_rate_unit_col: str = "RATE_UNIT",
    app_details_col_list: list = [],
) -> DataFrame:
    """
    Insert or update Applications from `df_app`. If the app already exists, updates the app if "details" have changed.
    """
    app_types = df_app[app_type_col].unique()
    app_ids = []
    for ind in tqdm(
        range(len(df_app)), desc=f"Inserting {list(app_types)} Applications"
    ):
        row = df_app.iloc[ind]
        crop_type_id = int(row.crop_type_id) if "crop_type_id" in row.index else None
        nutrient_source_id = (
            int(row.nutrient_source_id) if "nutrient_source_id" in row.index else None
        )
        # TODO: Support for geom_id (should only be used if more specific than field, field_trial, or plot_ids)
        # Choose one of field_id, field_trial_id, or plot_id to pass to App
        # plot_id is most specific
        plot_id = int(row.plot_id) if "plot_id" in row.index else None

        # field_trial_id is 2nd most specific
        field_trial_id = (
            int(row.field_trial_id)
            if ("field_trial_id" in row.index and isna(plot_id))
            else None
        )
        # field_id is least specific
        field_id = (
            int(row.field_id)
            if ("field_id" in row.index and isna(field_trial_id) and isna(plot_id))
            else None
        )
        if all([isna(i) for i in [plot_id, field_trial_id, field_id]]):
            # Is warning enough?
            logging.warning("plot_id, field_trial_id, and field_id are all NULL.")

        app_type = row[app_type_col] if app_type_col in row.index else None
        app_type = app_type if not isna(app_type) else None
        if app_type not in APP_TYPE_ENUM:
            raise ValueError(f"app_type must be one of {APP_TYPE_ENUM}, not {app_type}")

        app_method = row[app_method_col] if app_method_col in row.index else None
        app_method = app_method if not isna(app_method) else None
        date_applied = row[app_date_col] if app_date_col in row.index else None
        date_applied = date_applied if not isna(date_applied) else None
        rate = row[app_rate_col] if app_rate_col in row.index else None
        rate = float(rate) if not isna(rate) else None
        rate_unit = row[app_rate_unit_col] if app_rate_unit_col in row.index else None
        rate_unit = rate_unit if not isna(rate_unit) else None

        app = App(
            app_type=app_type,
            app_method=app_method,
            date_applied=date_applied,
            rate=rate,
            rate_unit=rate_unit,
            crop_type_id=crop_type_id,
            nutrient_source_id=nutrient_source_id,
            field_id=field_id,
            field_trial_id=field_trial_id,
            plot_id=plot_id,
            geom_id=None,
            details={
                k: None if isna(v) else v
                for k, v in row[row.index.isin(app_details_col_list)].to_dict().items()
            },
        )
        app_id = insertOrGetApp(cursor, app)
        # UPDATE DETAILS
        # TODO: How can the following be refactored into insertOrUpdateOrGetApp(cursor, app) function?
        # Now that we have app_id, we can check for differences in "details" column between app and app_id
        table_cols = [
            "app_type",
            "app_method" "date_applied",
            "rate",
            "rate_unit",
            "crop_type_id",
            "nutrient_source_id",
            "field_id",
            "field_trial_id",
            "plot_id",
            "geom_id",
            "details",
        ]
        # Get record corresponding to passed app_id
        app_record = basic_demeter_query(
            cursor,
            table="app",
            cols=["app_id"] + table_cols,
            conditions={"app_id": app_id},
        ).to_records()
        # Create App object from record
        app_db = App(
            app_type=app_record.app_type[0],
            app_method=app_record.app_method[0],
            date_applied=app_record.date_applied[0],
            rate=app_record.rate[0],
            rate_unit=app_record.rate_unit[0],
            crop_type_id=app_record.crop_type_id[0],
            nutrient_source_id=app_record.nutrient_source_id[0],
            field_id=app_record.field_id[0],
            field_trial_id=app_record.field_trial_id[0],
            plot_id=app_record.plot_id[0],
            geom_id=app_record.geom_id[0],
            details=app_record.details[0],
        )
        # Convert dataclass to dict for comparison
        app_db_dict = {k: asdict(app_db)[k] for k in table_cols}
        app_dict = {k: asdict(app)[k] for k in table_cols}

        # If dicts aren't the same, update details column
        if app_dict != app_db_dict:
            logging.info(
                "    Updating details column in app table for app_id %s", app_id
            )
            update_details(
                cursor,
                demeter_table=App,
                table_id=app_record.app_id[0],
                details=app_dict["details"],
            )

        app_ids.append(app_id)
    df_app["app_id_" + app_type.lower()] = app_ids
    return df_app
