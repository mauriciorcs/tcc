import pandas as pd

csv_precipitacao = r"C:\Users\Mauricio\tcc-mauricio\unir-dados\dados_transformados_precipitacao.csv"
csv_temperatura = r"C:\Users\Mauricio\tcc-mauricio\unir-dados\dados_transformados_Tmax.csv"

df_precipitacao = pd.read_csv(csv_precipitacao, encoding="utf-8")
df_temperatura = pd.read_csv(csv_temperatura, encoding="utf-8")

print("Colunas Precipitação:", df_precipitacao.columns)
print("Colunas Temperatura:", df_temperatura.columns)

# Renomear colunas
df_precipitacao = df_precipitacao.rename(columns={"Valor": "precipitacao_mm"})
df_temperatura = df_temperatura.rename(columns={"Valor": "temperatura_max_C"})

# união usando 'CD_MUN' e 'Data' como chave
df_merged = pd.merge(df_precipitacao, df_temperatura, on=['CD_MUN', 'Data'], how='inner')

# Remover colunas duplicadas
colunas_para_remover = [col for col in df_merged.columns if col.endswith("_y") and col not in ["temperatura_max_C"]]
df_merged = df_merged.drop(columns=colunas_para_remover)

# Ajustar nomes das colunas removendo sufixos desnecessários
df_merged = df_merged.rename(columns=lambda x: x.replace("_x", ""))

print("Colunas finais:", df_merged.columns)

csv_final = r"C:\Users\Mauricio\tcc-mauricio\unir-dados\dados_climaticos.csv"
df_merged.to_csv(csv_final, index=False, encoding="utf-8")

print(f"Arquivo consolidado salvo em: {csv_final}")
