import pandas as pd

df = pd.read_csv(r"C:\Users\Mauricio\tcc-mauricio\precipitacao-shp-media\preci_muni_mensal.csv")

colunas_fixas = ['CD_MUN', 'NM_MUN', 'CD_RGI', 'NM_RGI', 'CD_RGINT', 'NM_RGINT', 
                 'CD_UF', 'NM_UF', 'CD_REGIAO', 'NM_REGIAO', 'CD_CONCURB', 
                 'NM_CONCURB', 'AREA_KM2']

colunas_validas = []

for col in df.columns:
    try:
        data_coluna = pd.to_datetime(col, format='%Y-%m-%d')
        if '1961-01-31' <= col <= '2024-03-31':  # Intervalo desejado
            colunas_validas.append(col)
    except Exception:
        continue  # Ignora colunas que não são datas

# Transpoe
df_transformado = df.melt(id_vars=colunas_fixas, 
                           value_vars=colunas_validas, 
                           var_name='Data', 
                           value_name='Valor')

print(df_transformado)

df_transformado.to_csv('dados_transformadosPreci.csv', index=False, encoding='utf-8')

print("Arquivo 'dados_transformadosPreci.csv' salvo com sucesso!")
