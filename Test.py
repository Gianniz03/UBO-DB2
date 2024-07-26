from neo4j import GraphDatabase
import pandas as pd
import random
import json

# Connessione al database Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

def clear_database(tx):
    # Rimuovere tutti i nodi e le relazioni dal database
    tx.run("MATCH (n) DETACH DELETE n")

def create_nodes(tx, data, node_label, properties):
    # Creare nodi nel database Neo4j
    for index, row in data.iterrows():
        query = f"CREATE (n:{node_label} {{"
        query += ", ".join([f"{prop}: ${prop}" for prop in properties])
        query += "})"  # Corretto: usa una sola parentesi graffa chiusa
        tx.run(query, **row.to_dict())


def create_relationships(tx, relationships):
    # Creare relazioni tra nodi
    for relationship in relationships:
        tx.run(
            f"MATCH (a:{relationship['start_label']} {{id: $start_id}}), (b:{relationship['end_label']} {{id: $end_id}}) "
            f"CREATE (a)-[:{relationship['relation_type']}]->(b)", 
            start_id=relationship['start_id'], end_id=relationship['end_id']
        )

def load_data(file_path):
    return pd.read_csv(file_path)

def sample_data(df, percentage):
    return df.sample(frac=percentage, random_state=1).reset_index(drop=True)

def main():
    # Pulizia iniziale del database
    with driver.session() as session:
        session.execute_write(clear_database)

    # Caricamento dei dati
    admins = load_data('Dataset/administrators.csv')
    shareholders = load_data('Dataset/shareholders.csv')
    companies = load_data('Dataset/companies.csv')
    ubos = load_data('Dataset/ubo.csv')
    transactions = load_data('Dataset/transactions.csv')
    kyc_aml_checks = load_data('Dataset/kyc_aml_checks.csv')

    # Percentuali di campionamento
    percentages = [1.0, 0.75, 0.5, 0.25]
    
    # Creazione di dataset campionati
    for percentage in percentages:
        sampled_admins = sample_data(admins, percentage)
        sampled_shareholders = sample_data(shareholders, percentage)
        sampled_companies = sample_data(companies, percentage)
        sampled_ubos = sample_data(ubos, percentage)
        sampled_transactions = sample_data(transactions, percentage)
        sampled_kyc_aml_checks = sample_data(kyc_aml_checks, percentage)
        
        dataset_name = f"dataset_{int(percentage * 100)}"
        
        with driver.session() as session:
            session.execute_write(create_nodes, sampled_admins, 'Administrator', ['id', 'name', 'address', 'birthdate', 'nationality'])
            session.execute_write(create_nodes, sampled_shareholders, 'Shareholder', ['id', 'name', 'type', 'ownership_percentage', 'address', 'date_of_birth', 'nationality'])
            session.execute_write(create_nodes, sampled_companies, 'Company', ['id', 'name', 'address', 'legal_form', 'registration_details', 'financial_data'])
            session.execute_write(create_nodes, sampled_ubos, 'UBO', ['id', 'name', 'address', 'birthdate', 'nationality', 'ownership_percentage', 'type'])
            session.execute_write(create_nodes, sampled_transactions, 'Transaction', ['id', 'type', 'amount', 'date', 'currency'])
            session.execute_write(create_nodes, sampled_kyc_aml_checks, 'KYCAMLCheck', ['id', 'type', 'result', 'date', 'notes'])
            
            # Creazione delle relazioni
            # Nota: Gli ID degli esempi di relazione sono fissi. Dovresti aggiungere logica per collegare effettivamente i nodi.
            relationships = [
                {'start_label': 'Company', 'end_label': 'Shareholder', 'relation_type': 'HAS_SHAREHOLDER', 'start_id': 1, 'end_id': 1},
                {'start_label': 'Company', 'end_label': 'UBO', 'relation_type': 'HAS_UBO', 'start_id': 1, 'end_id': 1},
                {'start_label': 'Transaction', 'end_label': 'Company', 'relation_type': 'INVOLVES', 'start_id': 1, 'end_id': 1},
                {'start_label': 'KYCAMLCheck', 'end_label': 'Company', 'relation_type': 'CHECKED', 'start_id': 1, 'end_id': 1},
            ]
            
            session.execute_write(create_relationships, relationships)

    print("Dati importati e dataset creati con successo.")

if __name__ == "__main__":
    main()
