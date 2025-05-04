import geopandas as gpd

shape_path = r"C:\Users\Mauricio\tcc-mauricio\precipitacao-shp-media\preci_muni_mensal.shp"


gdf = gpd.read_file(shape_path)

csv_path = r"C:\Users\Mauricio\tcc-mauricio\precipitacao-shp-media\preci_muni_mensal.csv"

gdf.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8")


print(f"Arquivos csv salvo em: {csv_path}")