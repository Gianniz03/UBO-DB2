import pandas as pd

from BaseXClient import BaseXClient

# Nome dei file CSV da cui leggere i dati delle entità
csv_files = {
    'administrators': 'Dataset/File/administrators.csv',
    'companies': 'Dataset/File/companies.csv',
    'kyc_aml_checks': 'Dataset/File/kyc_aml_checks.csv',
    'shareholders': 'Dataset/File/shareholders.csv',
    'transactions': 'Dataset/File/transactions.csv',
    'ubo': 'Dataset/File/ubo.csv'
}

# Leggi tutti i file CSV in un unico DataFrame pandas, aggiungendo una colonna 'entity_type' per indicare l'entità di origine
dfs = []
for entity_type, file_path in csv_files.items():
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    df['entity_type'] = entity_type  # Aggiungi una colonna per l'entità
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)  # Combina tutti i DataFrame in uno solo

# Funzione per convertire un DataFrame in XML con escaping corretto dei caratteri speciali
def escape_xml_chars(text):
    if isinstance(text, str):
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")
    return text

# Funzione per convertire un DataFrame in XML, omettendo i campi non presenti o NaN e aggiungendo l'attributo entity_type
def dataframe_to_xml(df):
    xml = ['<ubo_records>']
    for _, row in df.iterrows():
        entity_type = row['entity_type']
        xml.append(f'  <ubo_record entity_type="{entity_type}">')  # Aggiungi attributo entity_type
        for field in df.columns:
            if field != 'entity_type':  # Non aggiungere il campo 'entity_type' come tag
                value = row[field]
                if pd.notna(value):  # Aggiungi solo i campi non NaN
                    escaped_value = escape_xml_chars(str(value))  # Escapa i caratteri speciali
                    xml.append(f'    <{field}>{escaped_value}</{field}>')
        xml.append('  </ubo_record>')
    xml.append('</ubo_records>')
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
    data_100_xml = dataframe_to_xml(df)
    insert_into_basex('UBO_100', data_100_xml)

# Crea il database 75% dal 100%
def create_db_75(df):
    df_75 = df.sample(frac=0.75, random_state=1)
    data_75_xml = dataframe_to_xml(df_75)
    insert_into_basex('UBO_75', data_75_xml)

# Crea il database 50% dal 100%
def create_db_50(df):
    df_50 = df.sample(frac=0.50, random_state=1)
    data_50_xml = dataframe_to_xml(df_50)
    insert_into_basex('UBO_50', data_50_xml)

# Crea il database 25% dal 100%
def create_db_25(df):
    df_25 = df.sample(frac=0.25, random_state=1)
    data_25_xml = dataframe_to_xml(df_25)
    insert_into_basex('UBO_25', data_25_xml)

# Avvia il processo di creazione sequenziale dei database
create_db_100(df)
create_db_75(df)
create_db_50(df)
create_db_25(df)