from datetime import datetime

from pandas import read_csv

from demeter_utils.interpolate._interpolate import find_fill_in_dates

# load the true data
# TODO: read data from cloud
# data available at sharepoint:https://sentera.sharepoint.com/:x:/s/demeter/ESR0PKnkjQBIkYDkT9NBVS8B1h5kJHTbJE2tCLgM7QWP7A?e=ioUoFB
df_true_data = read_csv(
    "/root/git/demeter-utils/df_drone_imagery1.csv",
    parse_dates=["date_observed", "last_updated"],
)

# using `find_fill_in_dates function` to generate df_observed
df_observed = find_fill_in_dates(df_true_data, 0, 120, 10, datetime(2022, 5, 6))
