import pandas as pd
import random

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from BaseXClient import BaseXClient

# Specifica il percorso del file CSV da leggere
csv_filename = 'Dataset/File/administrators.csv'

# Leggi il file CSV in un DataFrame di pandas, utilizzando la codifica 'ISO-8859-1'
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

# Converti il campo della data in oggetti datetime, se esiste
if 'birthdate' in df.columns:
    df['birthdate'] = pd.to_datetime(df['birthdate'])

# Calcola il numero totale di documenti nel DataFrame
total_documents = df.shape[0]

# Definisci il documento speciale come DataFrame
special_document = pd.DataFrame([{
    'id': 999999999,  # Assicurati che l'ID sia unico e non presente nei dati reali
    'name': 'Special Administrator',
    'address': '123 Special Lane, Special City, SP',
    'birthdate': '1970-01-01',
    'nationality': 'Special Country'
}])

# Funzione per convertire un DataFrame in XML con escaping corretto dei caratteri speciali
def escape_xml_chars(text):
    """
    Funzione che sostituisce i caratteri speciali XML con le rispettive entità.
    """
    if isinstance(text, str):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")
    return text

def dataframe_to_xml(df):
    xml = ['<administrators>']
    for _, row in df.iterrows():
        xml.append('  <administrator>')
        for field in df.columns:
            value = escape_xml_chars(str(row[field]))  # Escapa i caratteri speciali nel testo
            xml.append(f'    <{field}>{value}</{field}>')
        xml.append('  </administrator>')
    xml.append('</administrators>')
    return '\n'.join(xml)

# Funzione per connettersi a BaseX e inserire i dati
def insert_into_basex(db_name, xml_data):
    # Connettersi al database BaseX locale
    try:
        session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')
        try:
            # Crea un nuovo database o sovrascrivi quello esistente
            print(f"Creating database: {db_name}")
            session.execute(f"CREATE DB {db_name}")
            
            # Verifica la lunghezza del file XML
            print(f"Length of XML data for {db_name}: {len(xml_data)} characters")
            
            # Inserisce i dati XML nel database
            session.add(f"{db_name}.xml", xml_data)
            print(f"Data successfully loaded into {db_name} in BaseX.")
        except Exception as e:
            print(f"An error occurred during data insertion: {e}")
        finally:
            # Chiude la sessione
            session.close()
    except Exception as e:
        print(f"Connection error: {e}")

# Crea il database 100%
def create_db_100(df):
    # Crea il DataFrame per il 100% dei dati
    df_100 = df.copy()
    # Aggiungi il documento speciale
    df_100 = pd.concat([df_100, special_document], ignore_index=True)
    # Converti in XML
    data_100_xml = dataframe_to_xml(df_100)
    # Inserisci nel database
    insert_into_basex('Administrators_100', data_100_xml)
    return df_100

# Crea il database 75% dal 100%
def create_db_75(df_100):
    # Prendi il 75% dei dati dal DataFrame 100%
    df_75 = df_100.sample(frac=0.75, random_state=1)
    # Aggiungi il documento speciale
    df_75 = pd.concat([df_75, special_document], ignore_index=True)
    # Converti in XML
    data_75_xml = dataframe_to_xml(df_75)
    # Inserisci nel database
    insert_into_basex('Administrators_75', data_75_xml)
    return df_75

# Crea il database 50% dal 75%
def create_db_50(df_75):
    # Prendi il 50% dei dati dal DataFrame 75%
    df_50 = df_75.sample(frac=0.50, random_state=1)
    # Aggiungi il documento speciale
    df_50 = pd.concat([df_50, special_document], ignore_index=True)
    # Converti in XML
    data_50_xml = dataframe_to_xml(df_50)
    # Inserisci nel database
    insert_into_basex('Administrators_50', data_50_xml)
    return df_50

# Crea il database 25% dal 50%
def create_db_25(df_50):
    # Prendi il 25% dei dati dal DataFrame 50%
    df_25 = df_50.sample(frac=0.25, random_state=1)
    # Aggiungi il documento speciale
    df_25 = pd.concat([df_25, special_document], ignore_index=True)
    # Converti in XML
    data_25_xml = dataframe_to_xml(df_25)
    # Inserisci nel database
    insert_into_basex('Administrators_25', data_25_xml)
    return df_25

# Avvia il processo di creazione sequenziale dei database
df_100 = create_db_100(df)
df_75 = create_db_75(df_100)
df_50 = create_db_50(df_75)
df_25 = create_db_25(df_50)

