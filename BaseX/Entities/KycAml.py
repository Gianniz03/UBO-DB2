import pandas as pd
import pymongo
import random

# Crea una connessione al server MongoDB locale
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Seleziona il database 'UBO'
db = client["UBO"]

# Definisci i nomi delle collezioni nel database per i diversi set di dati
collection_name_100 = 'KYC_AML_Checks 100%'
collection_name_75 = 'KYC_AML_Checks 75%'
collection_name_50 = 'KYC_AML_Checks 50%'
collection_name_25 = 'KYC_AML_Checks 25%'

# Specifica il percorso del file CSV da leggere
csv_filename = 'Dataset/File/kyc_aml_checks.csv'

# Leggi il file CSV in un DataFrame di pandas, utilizzando la codifica 'ISO-8859-1'
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

# Converti il campo della data in oggetti datetime
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])
    
# Calcola il numero totale di documenti nel DataFrame
total_documents = df.shape[0]

# Calcola il numero di documenti per ciascun subset di dati
n_100 = int(total_documents)        # 100% dei documenti
n_75 = int(0.75 * total_documents)  # 75% dei documenti
n_50 = int(0.50 * total_documents)  # 50% dei documenti
n_25 = int(0.25 * total_documents)  # 25% dei documenti

# Crea una lista di indici e mescola gli indici in modo casuale
indices = list(range(total_documents))
random.shuffle(indices)

# Seleziona indici casuali per ciascun subset di dati
indices_100 = indices[:n_100]  # Indici per 100%
indices_75 = indices[:n_75]    # Indici per 75%
indices_50 = indices[:n_50]    # Indici per 50%
indices_25 = indices[:n_25]    # Indici per 25%

# Crea nuovi DataFrame contenenti i dati selezionati per ciascun subset
df_100 = df.iloc[indices_100]
df_75 = df.iloc[indices_75]
df_50 = df.iloc[indices_50]
df_25 = df.iloc[indices_25]

# Definisci il documento speciale come DataFrame
special_document = pd.DataFrame([{
    'id': 999999999,  # Assicurati che l'ID sia unico e non presente nei dati reali
    'ubo_id': 999999999,  # Assicurati che l'ID sia valido e presente nei dati reali
    'type': 'Special Check',
    'result': 'Passed',
    'date': pd.to_datetime('2024-01-01'),
    'notes': 'This is a special KYC/AML check document included in all subsets.'
}])

# Aggiungi il documento speciale a ciascun DataFrame
df_100 = pd.concat([df_100, special_document], ignore_index=True)
df_75 = pd.concat([df_75, special_document], ignore_index=True)
df_50 = pd.concat([df_50, special_document], ignore_index=True)
df_25 = pd.concat([df_25, special_document], ignore_index=True)

# Converte ciascun DataFrame in un formato adatto per l'inserimento in MongoDB
data_100 = df_100.to_dict(orient='records')
data_75 = df_75.to_dict(orient='records')
data_50 = df_50.to_dict(orient='records')
data_25 = df_25.to_dict(orient='records')

# Inserisce i dati nelle collezioni MongoDB corrispondenti
db[collection_name_100].insert_many(data_100)
db[collection_name_75].insert_many(data_75)
db[collection_name_50].insert_many(data_50)
db[collection_name_25].insert_many(data_25)

# Stampa un messaggio di conferma a schermo
print("Data successfully loaded into MongoDB with special document included.")
