# %%
import matplotlib.pyplot as plt
from pandas import DataFrame
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

from demeter_utils.interpolate._interpolate import interpolation

df_skeleton_final = DataFrame
df_skeleton_new = DataFrame

# %% Step 2: generate an interpolated dataframe from dem-360

df_interp = interpolation(
    df_complete=df_skeleton_final,
    df_skeleton=df_skeleton_new,
    interp_function=PchipInterpolator,
)

# %% Generate the interpolated values using different interp_function and plot them
df_interp_pchip1 = df_interp

df_interp_cubic_spline = interpolation(
    df_complete=df_skeleton_final,
    df_skeleton=df_skeleton_new,
    interp_function=CubicSpline,
)

df_interp_akima = interpolation(
    df_complete=df_skeleton_final,
    df_skeleton=df_skeleton_new,
    interp_function=Akima1DInterpolator,
)

datetime_interp = df_interp_akima["datetime_skeleton"]

ndvi_cubic = df_interp_cubic_spline["sample_value_interp"]
ndvi_akima = df_interp_akima["sample_value_interp"]
ndvi_pchip1 = df_interp_pchip1["sample_value_interp"]

plt.plot(datetime_interp, ndvi_cubic, "--", label="spline")
plt.plot(datetime_interp, ndvi_akima, "-", label="Akima1D")
plt.plot(datetime_interp, ndvi_pchip1, "-", label="pchip1")

# colors = {"True": "green", "False": "red"}
# plt.scatter(doy_obs, ndvi_obs, c=df["available"].map(colors))

plt.legend()
plt.ylim(0, 1)
plt.xticks(rotation=60)
plt.show()
