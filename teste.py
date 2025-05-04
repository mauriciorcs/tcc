from pyarrow import dataset as ds, Table
from pysus.ftp.databases.sinan import SINAN
from pysus.preprocessing.decoders import add_dv
from pysus.online_data import IBGE
from typing import List, Optional, Union, Dict
import os
import pandas as pd

def add_dv_safe(geocode):
    try:
        return add_dv(geocode)
    except Exception as e:
        print(f"Error processing geocode {geocode}: {e}")
        return 0

def first_day_of_month(date):
    try:
        date = pd.to_datetime(date)
        return pd.Timestamp(year=date.year, month=date.month, day=1)
    except Exception as e:
        print(f"Erro ao processar data: {date} -> {e}")
        return None

def download_data(
    dis_code,
    year: Optional[Union[str, int, list]] = None,
    data_dir: str = 'data/pysus'
) -> str:
    sinan = SINAN().load()
    files = sinan.get_files(dis_code=[dis_code], year=year)
    dataset_dir = os.path.join(data_dir, dis_code)
    parquets = sinan.download(files, local_dir=dataset_dir)
    return [parquet.path for parquet in parquets]

def extract_data(parquet_path, columns=None, filter=None) -> Table:
    return ds.dataset(parquet_path, format='parquet').to_table(columns, filter)

def table_count_by(tbl: Table, group_cols: List[str]) -> pd.DataFrame:
    data_agg = (
        tbl
        .group_by(group_cols)
        .aggregate([([], "count_all")])
        .to_pandas()
    )
    return data_agg

def transform_to_visao(
    parquet_paths: List[str],
    name: str = None,
    year_col: str = "DT_NOTIFIC",
    geocode_col: str = "ID_MUNICIP",
    other_group_cols: Dict[str, str] = {}
) -> pd.DataFrame:
    df_list = []
    pop = IBGE.get_population(year=2021)[["MUNIC_RES", "POPULACAO"]].rename(columns={"MUNIC_RES": "geocode", "POPULACAO": "populacao"})

    for parquet in parquet_paths:
        print(parquet)
        group_cols = [year_col, geocode_col]
        data_agg = extract_data(parquet, columns=group_cols)
        data_agg = table_count_by(data_agg, group_cols)

        data_agg.rename(columns={year_col: "data", geocode_col: "geocode", "count_all": "valor"}, inplace=True)
        data_agg["geocode"] = data_agg["geocode"].str.strip().apply(add_dv_safe).astype(str)

        data_agg = data_agg[data_agg["geocode"].isin(pop["geocode"].astype(str))]

        # 🗓️ Agrupamento por mês
        data_agg["data"] = pd.to_datetime(data_agg["data"], errors='coerce')
        data_agg["data"] = data_agg["data"].apply(first_day_of_month)

        data_agg.dropna(inplace=True)
        data_agg = data_agg.merge(pop, on="geocode", how="left")

        df_list.append(data_agg)

    df = pd.concat(df_list, ignore_index=True)

    # ✅ Aqui a gente remove duplicações na coluna de população
    df["populacao"] = df.groupby("geocode")["populacao"].transform("first")

    df = df.groupby(["data", "geocode"]).sum().reset_index().sort_values(["data", "geocode"])

    return df[['geocode', 'valor', 'populacao', 'data']]


def transform_geocode_to_uf(df: pd.DataFrame, geocode_col: str = "geocode"):
    df_uf = df.copy()
    df_uf[geocode_col] = df_uf[geocode_col].astype(str).str[:2]
    return df_uf

def aggregate_visao_count(df: pd.DataFrame, value_col: str = "valor"):
    group_cols = [col for col in df.columns if col != value_col]
    df_agg = df.groupby(group_cols)[value_col].sum().reset_index()
    return df_agg[['geocode', 'valor', 'data']]

def generate_visao_data(
        dis_codes: List[str] = ['DENG', 'ESQU', 'LEIV', 'MALA', 'RAIV'],
        data_dir: str = "dados/"
):
    csv_files = []
    for dis_code in dis_codes:
        parquets = download_data(dis_code)
        csv_municipio = os.path.join(data_dir, f"{dis_code.lower()}-municipio.csv")
        df_mun = transform_to_visao(parquets, year_col="DT_NOTIFIC")
        df_mun["data"] = df_mun["data"].apply(lambda x: x.strftime("%d/%m/%Y"))
        df_mun.to_csv(csv_municipio, index=False)
        df_uf = transform_geocode_to_uf(df_mun)
        df_uf = aggregate_visao_count(df_uf)
        csv_uf = os.path.join(data_dir, f"{dis_code.lower()}-uf.csv")
        df_uf.to_csv(csv_uf, index=False, date_format="%d/%m/%Y")
        csv_files.append([csv_municipio, csv_uf])
    return csv_files


if __name__ == "__main__":
    dis_codes = ['DENG']
    csv_files = generate_visao_data(dis_codes, "dados/teste")
    print(f"Generated files: {csv_files}")
