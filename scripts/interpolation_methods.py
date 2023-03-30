# %% Import libraries and data
import numpy as np
import pandas as pd
from pandas import read_csv
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# %% Load data
# TODO: Put data on cloud drive

df_allyears_in = read_csv("/root/git/demeter-utils/ndvi_gimms_allyears.csv")
df_allyears_in["year"] = pd.DatetimeIndex(df_allyears_in["START DATE"]).year

# %% Clean data
# create a new column for year
df_allyears = df_allyears_in[df_allyears_in["SAMPLE_VALUE"].notna()]
# df_2000_NAdropped = df_allyears_NAdropped[df_allyears_NAdropped['year']==2000]

# %% create a list of years
years = np.unique(df_allyears["year"].values)

# %%create x-axis values for interpolation
x_new = np.linspace(1, 365, num=365, dtype=int)

# %% Loop through each year and set two new columns: interp_type and interp_val
models_by_year = {}

for year in df_allyears["year"].unique():
    # filter df_allyears by year
    df = df_allyears[df_allyears["year"] == year]
    for Model, name in [
        (CubicSpline, "cubic_spline"),
        (Akima1DInterpolator, "akima_1d_interpolator"),
        (PchipInterpolator, "pchip_interpolator"),
    ]:
        model = Model(x=df["DOY"], y=df["SAMPLE_VALUE"])
        schema = {"model_type": name, "year": year, "model": model}
        models_by_year[f"{name}-{year}"] = schema

# Example usage:
# model_type = "cubic_spline"
# year = 2013
# models_by_year[f"{model_type}-{year}"]["model"](100)

# %% Create a dataframe that estimates the NDVI for each DOY (365x) and each model/year (72x)

# df = df_allyears[df_allyears["year"] == year]
df_interp = pd.DataFrame(columns=["year", "model_type", "x_new", "y_model"])

for year in df_allyears["year"].unique():
    df_year = df_allyears.loc[df_allyears["year"] == year]
    for model_type in [CubicSpline, Akima1DInterpolator, PchipInterpolator]:
        y_model = model_type(x=df_year["DOY"], y=df_year["SAMPLE_VALUE"])(x_new)

        # data = {
        #     "year": [year] * len(y_model),
        #     "model_type":...
        #     "y_model": y_model
        # }
        # df_temp = pd.DataFrame(data = data)
        # df_interp = pd.concat([df_interp, df_temp], axis = 0)


# df_interp = pd.DataFrame(x_new, y_model)

# %% Plotting new dataframe
# cubic_spline = CubicSpline(x, y)
# cubic_spline(100)
# cubic_spline(150)


# Plot the new interpolated values obtained using different methods
# plt.plot(x_new, cubic_spline(x_new), "--", label="spline")
# plt.plot(x_new, Akima1DInterpolator(x, y)(x_new), "-", label="Akima1D")
# plt.plot(x_new, PchipInterpolator(x, y)(x_new), "-", label="pchip")
# # plt.plot(x_new, pchip_interpolate(x, y)(x_new), '-', label='pchip2')
# plt.plot(x, y, "o")
# plt.legend()
# plt.ylim(0, 1)
# plt.show()
