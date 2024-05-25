# importar o necessario
import pandas as pd
import sqlite3
from datetime import datetime

#definir o caminho para o arquivo JSONL
df  = pd.read_json('../data/data.jsonl', lines=True)
#print(df)

# adicionar a coluna _source com um valor fixo
df['_source'] = "https://lista.mercadolivre.com.br/tenis-masculinos#D[A:tenis%20masculinos]"

#adicionar a coluna _data_coleta com a datae hora atuais
df['_data_coleta'] = datetime.now()


#tratar os valores nulos para colunas numéricas e de texto
df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_rating_number'] = df['reviews_rating_number'].fillna(0).astype(float)

#remover os parenteses do reviews_amount
df['reviews_amount'] = df['reviews_amount'].str.replace('[\(\)]', '', regex=True)
df['reviews_amount'] = df['reviews_amount'].fillna(0).astype(int)

#tratar os preços como float e calcular os valores totais
df['old_price'] = df['old_price_reais'] + (df['old_price_centavos']/100)
df['new_price'] = df['new_price_reais'] + (df['new_price_centavos']/100)
print(df)

#remover as colunas antigas de preços
df.drop(columns=['old_price_reais', 'old_price_centavos', 'new_price_reais', 'new_price_centavos'], inplace=True)

#Connectar ao banco de dados SQlite (ou criar um novo)
conn = sqlite3.connect('../data/quotes.db')

#Salvar o DataFrame no banco de dados SQLite
df.to_sql('mercadolivre_items', conn, if_exists='replace', index=False)

#fechar conexão com o banco de dados
conn.close()

#print(df.head())