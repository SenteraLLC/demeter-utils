from datetime import datetime, timedelta
from io import StringIO

import requests
from pandas import read_csv
from scipy.interpolate import Akima1DInterpolator

from demeter_utils.interpolate._interpolate import generate_fill_in_values

# from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# load the standard/reference data
# TODO: Use the function by Marissa
GIMMS_COLS = {
    "START DATE": "date_start",
    "END DATE": "date_end",
    "SAMPLE VALUE": "sample_value",
    "SAMPLE COUNT": "n_pixels_sample",
    "MEAN VALUE": "mean_hist_value",
    "MIN VALUE": "min_hist_value",
    "MAX VALUE": "max_hist_value",
}
req = requests.get(
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14).rename(columns=GIMMS_COLS)[
    GIMMS_COLS.values()
]


# using `generate_fill_in_values` to generate df_interp
df_reference_interp = df_interp = generate_fill_in_values(
    df_reference_ndvi=df_gimms_ndvi,
    datetime_start=datetime(2019, 5, 6),
    datetime_end=datetime(2019, 10, 1),
    temporal_resolution=timedelta(days=1),
    interp_function=Akima1DInterpolator,
)
