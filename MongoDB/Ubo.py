import pandas as pd
import pymongo
import random

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["UBO"]
collection_name_100 = 'UBO 100%'
collection_name_75 = 'UBO 75%'
collection_name_50 = 'UBO 50%'
collection_name_25 = 'UBO 25%'

csv_filename = 'Dataset/ubo.csv'
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

total_documents = df.shape[0]

n_100 = int(total_documents)
n_75 = int(0.75 * total_documents)
n_50 = int(0.50 * total_documents)
n_25 = int(0.25 * total_documents)

indices = list(range(total_documents))
random.shuffle(indices)
indices_100 = indices[:n_100]
indices_75 = indices[:n_75]
indices_50 = indices[:n_50]
indices_25 = indices[:n_25]

df_100 = df.iloc[indices_100]
df_75 = df.iloc[indices_75]
df_50 = df.iloc[indices_50]
df_25 = df.iloc[indices_25]

data_100 = df_100.to_dict(orient='records')
data_75 = df_75.to_dict(orient='records')
data_50 = df_50.to_dict(orient='records')
data_25 = df_25.to_dict(orient='records')

db[collection_name_100].insert_many(data_100)
db[collection_name_75].insert_many(data_75)
db[collection_name_50].insert_many(data_50)
db[collection_name_25].insert_many(data_25)

print("Data successfully loaded into MongoDB.")
