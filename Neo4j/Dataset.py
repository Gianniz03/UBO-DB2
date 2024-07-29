import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Directory in cui si trovano i file CSV
csv_directory = 'Dataset'

# Connessioni ai database Neo4j
graph100 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset100")
graph75 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset75")
graph50 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset50")
graph25 = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"), name="dataset25")

# Dizionario per mappare le percentuali ai grafi
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# Funzione per leggere e restituire un numero di righe specifico da un file CSV
def read_csv_partial(file_path, percentage):
    data = pd.read_csv(file_path)
    rows_to_insert = int(len(data) * (percentage / 100))
    return data.head(rows_to_insert)

# Funzione per creare nodi e relazioni nel database Neo4j
def create_nodes_and_relationships(graph, data, node_label, relationships):
    for _, row in data.iterrows():
        node = Node(node_label, **row.to_dict())
        graph.create(node)
        for rel in relationships:
            if rel['type'] in row and row[rel['type']] is not None:
                target_node = graph.nodes.match(rel['target_label'], id=row[rel['type']]).first()
                if target_node:
                    relationship = Relationship(node, rel['rel_type'], target_node)
                    graph.create(relationship)

# Importa i dati dai file CSV nei grafi Neo4j e crea le relazioni
datasets = {
    "administrators": {
        "file": "administrators.csv",
        "label": "Amministratore",
        "relationships": []
    },
    "shareholders": {
        "file": "shareholders.csv",
        "label": "Azionista",
        "relationships": []
    },
    "ubo": {
        "file": "ubo.csv",
        "label": "UBO",
        "relationships": []
    },
    "transactions": {
        "file": "transactions.csv",
        "label": "Transazione",
        "relationships": []
    },
    "kyc_aml_checks": {
        "file": "kyc_aml_checks.csv",
        "label": "ControlloKYCAML",
        "relationships": []
    },
    "companies": {
        "file": "companies.csv",
        "label": "Azienda",
        "relationships": [
            {'type': 'administrators', 'target_label': 'Amministratore', 'rel_type': 'AZIENDA_HA_AMMINISTRATORE'},
            {'type': 'shareholders', 'target_label': 'Azionista', 'rel_type': 'AZIENDA_HA_AZIONISTA'},
            {'type': 'ubo', 'target_label': 'UBO', 'rel_type': 'AZIENDA_HA_UBO'},
            {'type': 'transactions', 'target_label': 'Transazione', 'rel_type': 'AZIENDA_HA_TRANSAZIONE'},
            {'type': 'kyc_aml_checks', 'target_label': 'ControlloKYCAML', 'rel_type': 'UBO_HA_CONTROLLI'}
        ]
    }
}

for dataset_name, dataset_info in datasets.items():
    csv_path = os.path.join(csv_directory, dataset_info['file'])
    for percentage, graph in graphs_by_percentage.items():
        data = read_csv_partial(csv_path, percentage)
        create_nodes_and_relationships(graph, data, dataset_info['label'], dataset_info['relationships'])
        print(f"{percentage}% dei dati del dataset {dataset_name} inseriti in Neo4j con successo nel dataset {percentage}%.")

print("Inserimento dati e creazione relazioni completato per tutti i dataset.")
