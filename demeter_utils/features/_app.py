from datetime import datetime
from typing import Any, Union

from numpy import nan


def get_applied_fertilizer_until_date_for_field_id(
    cursor: Any,
    field_id: int,
    date_limit: datetime,
    nutrient: str,
    method: Union[str, None] = None,
) -> float:
    """For a given field ID, gets sum of fertilizer applied for a specified nutrient and, if given, method up until a specified date.

    Args:
        cursor: Connection to Demeter database.
        field_id (int): Demeter field ID to find application data for.
        date_limit (datetime): Reference date to sum application rate information up until to.
        nutrient (str): Applied nutrient to consider.
        method (str): Application method to consider; if not given, all methods are considered.
    """
    pass
    # # get all fertilizer activity for this `field_id`
    # df_applied_full = get_as_applied(
    #     cursor, field_id=field_id, colname_date="date_performed"
    # )

    # # reduce based on end date
    # df_apps = df_applied_full.loc[df_applied_full["date_performed"] < date_limit]

    # # if no rows left, then return 0
    # if len(df_apps) == 0:
    #     return 0

    # # filter on nutrient and method (if available)
    # df_apps = df_apps.loc[df_apps["nutrient"] == nutrient]
    # if method is not None:
    #     df_apps = df_apps.loc[df_apps["method"] == method]

    # return float(df_apps["rate_lbs_acre"].sum())


def get_yield_response_to_application(
    control_yield: float,
    treatment_yield: float,
    treatment_application_rate: float,
    control_application_rate: float = 0,
) -> float:
    """Calculates yield response to an application rate relative to a `control_yield`.

    If difference in application rate between treatment and control is 0, then NA is returned.

    Args:
        control_yield (float): Reference value of yield to which `treatment_yield` should be compared (often the mean control yield for a site);
            should be given in units of mass or volume per acre (e.g., bu/acre).

        treatment_yield (float): Value of yield measured for treatment area, where response should be measured; should be given in units of mass
            or volume per acre (e.g., bu/acre).

        treatment_application_rate (float): Application rate of product applied to treatment area; should be given in units normalized by acre

        control_application_rate (float): Application rate of product applied to control area; should be given in the same units as `treatment_application_rate`.
            Value defaults to 0.
    """
    rate_diff = treatment_application_rate - control_application_rate
    if rate_diff > 0:
        return (treatment_yield - control_yield) / rate_diff
    else:
        return nan
