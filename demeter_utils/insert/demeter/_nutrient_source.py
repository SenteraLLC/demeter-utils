from demeter.data import NutrientSource, insertOrGetNutrientSource
from pandas import DataFrame, isna
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm


def insert_or_get_nutrient_source(
    cursor: NamedTupleCursor,
    organization_id: int,
    df_nutrient_sources: DataFrame,
    nutrient_source_col: str,
    n_col: str = "N",
    p2o5_col: str = "P2O5",
    k2o_col: str = "K2O",
    s_col: str = "S",
    ca_col: str = "Ca",
    mg_col: str = "Mg",
    b_col: str = "B",
    cu_col: str = "Cu",
    fe_col: str = "Fe",
    mn_col: str = "Mn",
    mo_col: str = "Mo",
    zn_col: str = "Zn",
    ch_col: str = "Ch",
    nutrient_details_col_list: list = [],
) -> DataFrame:
    """Insert NutrientSource."""
    # df_nutrient_sources = df_nutrient_sources[[nutrient_source_col]].drop_duplicates()
    df_nutrient_sources_ = df_nutrient_sources.drop_duplicates(
        subset=nutrient_source_col
    )
    nutrient_source_ids = []
    for ind in tqdm(
        range(len(df_nutrient_sources_)), desc="Inserting Nutrient Sources:"
    ):
        row = df_nutrient_sources_.iloc[ind]
        nutrient = (
            row[nutrient_source_col] if nutrient_source_col in row.index else None
        )
        if isna(nutrient):
            raise ValueError("Nutrient Source cannot be null.")
        n = float(row[n_col]) if n_col in row.index else 0.0
        p2o5 = float(row[p2o5_col]) if p2o5_col in row.index else 0.0
        k2o = float(row[k2o_col]) if k2o_col in row.index else 0.0
        s = float(row[s_col]) if s_col in row.index else 0.0
        ca = float(row[ca_col]) if ca_col in row.index else 0.0
        mg = float(row[mg_col]) if mg_col in row.index else 0.0
        b = float(row[b_col]) if b_col in row.index else 0.0
        cu = float(row[cu_col]) if cu_col in row.index else 0.0
        fe = float(row[fe_col]) if fe_col in row.index else 0.0
        mn = float(row[mn_col]) if mn_col in row.index else 0.0
        mo = float(row[mo_col]) if mo_col in row.index else 0.0
        zn = float(row[zn_col]) if zn_col in row.index else 0.0
        ch = float(row[ch_col]) if ch_col in row.index else 0.0
        nutrient_source = NutrientSource(
            nutrient=nutrient,
            organization_id=int(organization_id),
            n=n,
            p2o5=p2o5,
            k2o=k2o,
            s=s,
            ca=ca,
            mg=mg,
            b=b,
            cu=cu,
            fe=fe,
            mn=mn,
            mo=mo,
            zn=zn,
            ch=ch,
            details={
                k: None if isna(v) else v
                for k, v in row[row.index.isin(nutrient_details_col_list)]
                .to_dict()
                .items()
            },
        )
        nutrient_source_id = insertOrGetNutrientSource(cursor, nutrient_source)
        nutrient_source_ids.append(nutrient_source_id)
    df_nutrient_sources_["nutrient_source_id"] = nutrient_source_ids
    return df_nutrient_sources_
