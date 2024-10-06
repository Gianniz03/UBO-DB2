import pandas as pd
import pymongo
import random

# Connessione al server MongoDB locale
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["UBO"]  # Seleziona il database 'UBO'

# Definizione dei nomi delle collezioni per i diversi dataset UBO
collection_name_100 = 'UBO 100%'
collection_name_75 = 'UBO 75%'
collection_name_50 = 'UBO 50%'
collection_name_25 = 'UBO 25%'

# Nome del file CSV da cui leggere i dati
csv_filename = 'Dataset/File/ubo.csv'

# Leggi il file CSV in un DataFrame pandas
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

# Converti il campo della data in oggetti datetime
if 'birthdate' in df.columns:
    df['birthdate'] = pd.to_datetime(df['birthdate'])
    
# Calcola il numero totale di documenti nel DataFrame
total_documents = df.shape[0]

# Calcola il numero di documenti per ciascun dataset
n_100 = int(total_documents)
n_75 = int(0.75 * total_documents)
n_50 = int(0.50 * total_documents)
n_25 = int(0.25 * total_documents)

# Crea una lista di indici e mescola casualmente
indices = list(range(total_documents))
random.shuffle(indices)

# Seleziona gli indici per ciascun dataset in base alla dimensione calcolata
indices_100 = indices[:n_100]
indices_75 = indices[:n_75]
indices_50 = indices[:n_50]
indices_25 = indices[:n_25]

# Crea DataFrame per ciascun dataset in base agli indici selezionati
df_100 = df.iloc[indices_100]
df_75 = df.iloc[indices_75]
df_50 = df.iloc[indices_50]
df_25 = df.iloc[indices_25]

# Definisci il documento speciale come DataFrame
special_document = pd.DataFrame([{
    'id': 999999999,  # Assicurati che l'ID sia unico e non presente nei dati reali
    'name': 'Special Name',  # Sostituisci con valori appropriati per i tuoi campi
    'address': 'Special Address',
    'birthdate':  pd.to_datetime('2024-01-01'),
    'nationality': 'Special Nationality',
    'ownership_percentage': 55.5,
    'type': 'Company',

}])

# Aggiungi il documento speciale a ciascun DataFrame
df_100 = pd.concat([df_100, special_document], ignore_index=True)
df_75 = pd.concat([df_75, special_document], ignore_index=True)
df_50 = pd.concat([df_50, special_document], ignore_index=True)
df_25 = pd.concat([df_25, special_document], ignore_index=True)

# Converti i DataFrame in liste di dizionari per l'inserimento in MongoDB
data_100 = df_100.to_dict(orient='records')
data_75 = df_75.to_dict(orient='records')
data_50 = df_50.to_dict(orient='records')
data_25 = df_25.to_dict(orient='records')

# Inserisci i dati nelle rispettive collezioni MongoDB
db[collection_name_100].insert_many(data_100)
db[collection_name_75].insert_many(data_75)
db[collection_name_50].insert_many(data_50)
db[collection_name_25].insert_many(data_25)

# Stampa un messaggio di conferma
print("Data successfully loaded into MongoDB with special document included.")
