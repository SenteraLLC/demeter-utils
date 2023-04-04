# %% Import libraries and data
import numpy as np
import pandas as pd
import seaborn as sns
from pandas import read_csv
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# %% Load data
# TODO: Put data on cloud drive
# url_onedrive = "https://sentera.sharepoint.com/sites/demeter/Shared%20Documents/projects_deepak"
# file_path = url_onedrive + "/interpolation/data/ndvi_gimms_allyears.csv"
# df_allyears_in = read_csv("ndvi_gimms_allyears.csv")

df_allyears_in = read_csv("/root/git/demeter-utils/ndvi_gimms_allyears.csv")

# %% Clean data
# create a new column for doy, year and month
df_allyears_in["doy"] = (
    df_allyears_in["ORDINAL_DATE"].str.split("-").str[-1].astype(int)
)
df_allyears_in["year"] = pd.DatetimeIndex(df_allyears_in["START DATE"]).year
df_allyears_in["month"] = pd.DatetimeIndex(df_allyears_in["START DATE"]).month

# create new columns for metadata
df_allyears_in["shape_id"] = "110631"  # Enter shape_id of the data extracted
df_allyears_in["crop_mask"] = "corn"  # Enter crop_mask of the data extracted

# drop NA
df_allyears = df_allyears_in[df_allyears_in["SAMPLE_VALUE"].notna()]

# TODO: Create new dataframe for select months only, say March 1st to October 31
# df_allyears_selectmonths = df_allyears[]

df_forinterp = df_allyears

# %%create x-axis values for interpolation
doy_new = np.linspace(1, 365, num=365, dtype=int)

# %% Create a dataframe that estimates the NDVI for each DOY (365x) and each model/year (72x)
df_interp = pd.DataFrame(columns=["year", "model_type", "DOY", "ndvi_interp"])
for year in df_forinterp["year"].unique():
    df_year = df_forinterp.loc[df_forinterp["year"] == year]
    for model_type, name in [
        (CubicSpline, "cubic_spline"),
        (Akima1DInterpolator, "akima_1d_interpolator"),
        (PchipInterpolator, "pchip_interpolator"),
    ]:
        ndvi_interp = model_type(x=df_year["DOY"], y=df_year["SAMPLE_VALUE"])(doy_new)

        data = {
            "year": [year] * len(ndvi_interp),
            "model_type": [name] * len(ndvi_interp),
            "DOY": doy_new,
            "ndvi_interp": ndvi_interp,
        }
        df_temp = pd.DataFrame(data=data)
        df_interp = pd.concat([df_interp, df_temp], axis=0)

# %%Cleaning new data
df_interp_clean = df_interp[
    (df_interp.year != 2000)
    & (df_interp.year != 2001)
    & (df_interp.year != 2005)
    & (df_interp.year != 2010)
    & (df_interp.year != 2023)
]
df_interp_clean = df_interp_clean[df_interp_clean["ndvi_interp"].notna()]

# Exporting data to a csv
# import os
# os.makedirs('exported', exist_ok=True)
# df_interp_clean.to_csv('exported/ndvi_interpolated.csv')

# Merge the oberseved data and interpolated dataset based on DOY
df_allyears_new = pd.merge(
    df_interp_clean, df_forinterp, left_on=["year", "DOY"], right_on=["year", "DOY"]
)


# %% Plotting line graph for new dataframe: DOY vs interpolated NDVI for each year by each method
sns.set_theme(style="ticks")
graph = sns.relplot(
    data=df_interp_clean,
    x="DOY",
    y="ndvi_interp",
    hue="year",
    col="model_type",
    kind="line",
    palette=sns.color_palette(),
    height=5,
    aspect=0.75,
    facet_kws=dict(sharex=True),
)
graph.add_legend()

# %%Scatter plot for observed NDVI vs interpolated NDVI
# Method1
graph_obsvspred = sns.FacetGrid(df_allyears_new, col="model_type", hue="year")
graph_obsvspred.map(sns.scatterplot, "ndvi_interp", "SAMPLE_VALUE")
graph_obsvspred.add_legend()

# Method2:
# graph_obsvspred = sns.relplot(
#     data=df_allyears_new,
#     x='ndvi_interp', y='SAMPLE_VALUE',
#     kind='scatter',
#     hue="year_y", col= 'model_type'
#     )
# graph_obsvspred.add_legend()

# %%Linear regresssion plot for observed NDVI vs interpolated NDVI

# sns.set_theme(style="ticks")
# sns.lmplot(
#     data=df_allyears_new,
#     x="ndvi_interp", y="SAMPLE_VALUE",
#     col="model_type", #hue="year_y",
#     col_wrap=2, palette="muted", ci=None,
#     height=4, scatter_kws={"s": 50, "alpha": 1}
# )

# from scipy import stats
# # get coeffs of linear fit
# slope, intercept, r_value, p_value, std_err = stats.linregress(df_allyears_new['ndvi_interp'],df_allyears_new['SAMPLE_VALUE'])

# # use line_kws to set line label for legend
# ax = sns.regplot(x="total_bill", y="tip", data=tips, color='b',
#  line_kws={'label':"y={0:.1f}x+{1:.1f}".format(slope,intercept)})

# # plot legend
# ax.legend()

# %%New
# from scipy import stats
# import seaborn as sns
# import matplotlib.pyplot as plt

# def corrfunc(x, y, **kws):
#     r, _ = stats.pearsonr(x, y)
#     ax = plt.gca()
#     ax.annotate("r = {:.2f}".format(r),
#                 xy=(.1, .9), xycoords=ax.transAxes)

# g = sns.FacetGrid(df_allyears_new, col=year, hue=model_type,col_wrap=5, height=5)
# g.map(plt.scatter, ndvi_interp, SAMPLE_VALUE, alpha=.7, s=3)
# g.map(corrfunc, ndvi_interp, SAMPLE_VALUE)


# %% Loop through each year and set two new columns: interp_type and interp_val
# This code will return the interpolated value for a specified DOY with a specified model type.

# models_by_year = {}
# for year in df_allyears["year"].unique():
#     # filter df_allyears by year
#     df = df_allyears[df_allyears["year"] == year]
#     for Model, name in [
#         (CubicSpline, "cubic_spline"),
#         (Akima1DInterpolator, "akima_1d_interpolator"),
#         (PchipInterpolator, "pchip_interpolator"),
#     ]:
#         model = Model(x=df["DOY"], y=df["SAMPLE_VALUE"])
#         schema = {"model_type": name, "year": year, "model": model}
#         models_by_year[f"{name}-{year}"] = schema

# Example usage:
# model_type = "cubic_spline"
# year = 2013
# models_by_year[f"{model_type}-{year}"]["model"](100)

# %% Generate the interpolated values using different methods and plot them

# from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator, pchip_interpolate
# ndvi_cubic = cubic_spline(doy_interp)
# ndvi_akima = Akima1DInterpolator(doy_obs, ndvi_obs))(doy_interp)
# ndvi_pchip1 = PchipInterpolator(doy_obs, ndvi_obs)(doy_interp)
# ndvi_pchip2 = pchip_interpolate(doy_obs, ndvi_obs)(doy_interp)

# plt.plot(doy_interp, ndvi_cubic, "--", label="spline")
# plt.plot(doy_interp, ndvi_akima, "-", label="Akima1D")
# plt.plot(doy_interp, ndvi_pchip1, "-", label="pchip1")
# # plt.plot(doy_interp, ndvi_pchip2, '-', label='pchip2')
# plt.plot(doy_obs, ndvi_obs, "o")
# plt.legend()
# plt.ylim(0, 1)
# plt.show()
