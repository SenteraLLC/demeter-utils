from datetime import datetime
from io import StringIO

import matplotlib.pyplot as plt
import numpy as np
import requests
from pandas import read_csv
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

from demeter_utils.interpolate._interpolate import (
    fill_missing_values,
    find_fill_in_dates,
    generate_fill_in_values,
)

# load the true data
# TODO: read data from cloud
# data available at sharepoint:https://sentera.sharepoint.com/:x:/s/demeter/ESR0PKnkjQBIkYDkT9NBVS8B1h5kJHTbJE2tCLgM7QWP7A?e=ioUoFB
df_true_data = read_csv(
    "/root/git/demeter-utils/df_drone_imagery1.csv",
    parse_dates=["date_observed", "last_updated"],
)

# using `find_fill_in_dates function` to generate df_observed
df_observed = find_fill_in_dates(df_true_data, 0, 120, 10, datetime(2022, 5, 6))

# load the standard/reference data
# TODO: Use the function by Marissa
req = requests.get(
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14)

# using `generate_fill_in_values` to generate df_interp
df_reference_interp = generate_fill_in_values(df_gimms_ndvi, Akima1DInterpolator)

# using `fill_missing_values` to generate df_final
df_complete_data = fill_missing_values(df_observed, df_reference_interp)


# %% Generate the interpolated values for each DOY (defined dates) using different methods and plot them

df = df_complete_data
df = df.rename(columns={"value_observed": "ndvi_obs"})
df = df.sort_values(by=["doy_obs"])
doy_obs = df["doy_obs"]
ndvi_obs = df["ndvi_obs"]
doy_interp = np.arange(120, 250, 1, dtype=int)

ndvi_cubic = CubicSpline(doy_obs, ndvi_obs)(doy_interp)
ndvi_akima = Akima1DInterpolator(doy_obs, ndvi_obs)(doy_interp)
ndvi_pchip1 = PchipInterpolator(doy_obs, ndvi_obs)(doy_interp)

plt.plot(doy_interp, ndvi_cubic, "--", label="spline")
plt.plot(doy_interp, ndvi_akima, "-", label="Akima1D")
plt.plot(doy_interp, ndvi_pchip1, "-", label="pchip1")
colors = {"True": "green", "False": "red"}
plt.scatter(doy_obs, ndvi_obs, c=df["available"].map(colors))
plt.legend()
plt.ylim(0, 1)
plt.show()


# %% to store the interpolated values
# data = {
#     "ndvi_cubic": ndvi_cubic,
#     "ndvi_akima": ndvi_akima,
#     "ndvi_pchip1": ndvi_pchip1,
# }
# df_temp = pd.DataFrame(data=data)
