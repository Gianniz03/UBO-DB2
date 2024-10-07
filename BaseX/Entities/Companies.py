import pandas as pd
import random

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from BaseXClient import BaseXClient

# Specifica il percorso del file CSV da leggere
csv_filename = 'Dataset/File/companies.csv'

# Leggi il file CSV in un DataFrame di pandas, utilizzando la codifica 'ISO-8859-1'
df = pd.read_csv(csv_filename, encoding='ISO-8859-1')

# Calcola il numero totale di documenti nel DataFrame
total_documents = df.shape[0]

# Definisci il documento speciale come DataFrame
special_document = pd.DataFrame([{
    'id': 999999999,  # Assicurati che l'ID sia unico e non presente nei dati reali
    'name': 'Special Company',
    'address': '123 Special Ave, Special City, SC',
    'legal_form': 'S.p.A.',
    'registration_details': 'SPECIAL-REG-001',
    'financial_data': '[{"year":2024,"revenue":100000,"profit":50000}]',
    'administrators': '[999999999]',  # Assicurati che gli ID siano validi e presenti nei dati reali
    'shareholders': '[999999999]',
    'ubo': '[999999999]',
    'transactions': '[999999999]',
    'kyc_aml_checks': '[999999999]'
}])

# Funzione per convertire un DataFrame in XML con escaping corretto dei caratteri speciali
def escape_xml_chars(text):
    if isinstance(text, str):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")
    return text

def dataframe_to_xml(df):
    xml = ['<companies>']
    for _, row in df.iterrows():
        xml.append('  <company>')
        for field in df.columns:
            value = escape_xml_chars(str(row[field]))  # Escapa i caratteri speciali nel testo
            xml.append(f'    <{field}>{value}</{field}>')
        xml.append('  </company>')
    xml.append('</companies>')
    return '\n'.join(xml)

# Funzione per connettersi a BaseX e inserire i dati
def insert_into_basex(db_name, xml_data):
    try:
        session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')
        try:
            print(f"Creating database: {db_name}")
            session.execute(f"CREATE DB {db_name}")
            print(f"Length of XML data for {db_name}: {len(xml_data)} characters")
            session.add(f"{db_name}.xml", xml_data)
            print(f"Data successfully loaded into {db_name} in BaseX.")
        except Exception as e:
            print(f"An error occurred during data insertion: {e}")
        finally:
            session.close()
    except Exception as e:
        print(f"Connection error: {e}")

# Crea il database 100%
def create_db_100(df):
    df_100 = df.copy()
    df_100 = pd.concat([df_100, special_document], ignore_index=True)
    data_100_xml = dataframe_to_xml(df_100)
    insert_into_basex('Companies_100', data_100_xml)
    return df_100

# Crea il database 75% dal 100%
def create_db_75(df_100):
    df_75 = df_100.sample(frac=0.75, random_state=1)
    df_75 = pd.concat([df_75, special_document], ignore_index=True)
    data_75_xml = dataframe_to_xml(df_75)
    insert_into_basex('Companies_75', data_75_xml)
    return df_75

# Crea il database 50% dal 75%
def create_db_50(df_75):
    df_50 = df_75.sample(frac=0.50, random_state=1)
    df_50 = pd.concat([df_50, special_document], ignore_index=True)
    data_50_xml = dataframe_to_xml(df_50)
    insert_into_basex('Companies_50', data_50_xml)
    return df_50

# Crea il database 25% dal 50%
def create_db_25(df_50):
    df_25 = df_50.sample(frac=0.25, random_state=1)
    df_25 = pd.concat([df_25, special_document], ignore_index=True)
    data_25_xml = dataframe_to_xml(df_25)
    insert_into_basex('Companies_25', data_25_xml)
    return df_25

# Avvia il processo di creazione sequenziale dei database
df_100 = create_db_100(df)
df_75 = create_db_75(df_100)
df_50 = create_db_50(df_75)
df_25 = create_db_25(df_50)

