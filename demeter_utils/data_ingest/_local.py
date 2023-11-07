import logging
from os import getenv
from os.path import join
from pathlib import Path
from typing import Union

from pandas import DataFrame, read_csv

PLOT_ID_COL = "plot_id"
TREATMENT_COL = "treatment"


def load_exp_design(
    project_name: str,
    fname_trt_info: Union[str, Path],
    col_name_plot_id: str = None,
    col_name_treatment: str = None,
) -> DataFrame:
    """
    Load experimental design/treatment information from local CSV.

    Be sure to set `DEMETER_DIR` in your .env.

    Args:
        project_name (str): Name of project.
        fname_trt_info (str): Name of CSV file containing treatment information.

        col_name_plot_id (str, Optional): Name of column containing plot IDs. If not provided, assumes the "plot_id"
            column already exists in `fname_trt_info`.

        col_name_treatment (str, Optional): Name of column containing treatment IDs. If not provided, assumes the
            "treatment_id" column already exists in `fname_trt_info`.

    Returns:
        DataFrame: Experimental trial information with "plot_id" and "treatment_id" columns present.
    """
    logging.info('    Loading experimental design data for "%s"', fname_trt_info)
    demeter_dir = str(getenv("DEMETER_DIR"))
    if not demeter_dir:
        raise RuntimeError('"DEMETER_DIR" environment variable is not properly set.')

    data_dir = join(demeter_dir, "projects", project_name, "data")
    filepath_trt_info = Path(join(data_dir, "raw", fname_trt_info))
    if filepath_trt_info.is_file() is False:
        raise RuntimeError(f'File "{filepath_trt_info}" does not exist.')
    df_trt_info = read_csv(filepath_trt_info)

    # Ensure "plot_id" and "treatment_id" are present
    df_trt_info.rename(
        columns={col_name_plot_id: PLOT_ID_COL}
    ) if col_name_plot_id else df_trt_info
    df_trt_info.rename(
        columns={col_name_treatment: TREATMENT_COL}
    ) if col_name_treatment else df_trt_info
    for col in [PLOT_ID_COL, TREATMENT_COL]:
        if col not in df_trt_info.columns:
            raise RuntimeError(
                f'Column "{col}" is required, but is not present in "{fname_trt_info}".'
            )
    return df_trt_info
