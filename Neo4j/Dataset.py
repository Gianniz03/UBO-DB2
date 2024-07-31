import pandas as pd
from py2neo import Graph, Node, Relationship
from faker import Faker
import random

# Creare un'istanza di Faker
fake = Faker()

def create_graph(graph, admins, shareholders, ubos, transactions, companies, kyc_aml_checks):
    # Creazione dei nodi per ogni tipo di entità
    admin_nodes = {}
    for _, row in admins.iterrows():
        admin_node = Node("Amministratore", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'])
        graph.create(admin_node)
        admin_nodes[row['id']] = admin_node

    shareholder_nodes = {}
    for _, row in shareholders.iterrows():
        shareholder_node = Node("Azionista", id=row['id'], name=row['name'], type=row['type'], ownership_percentage=row['ownership_percentage'], address=row['address'], date_of_birth=row['date_of_birth'], nationality=row['nationality'])
        graph.create(shareholder_node)
        shareholder_nodes[row['id']] = shareholder_node

    ubo_nodes = {}
    for _, row in ubos.iterrows():
        ubo_node = Node("UBO", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'], ownership_percentage=row['ownership_percentage'], type=row['type'])
        graph.create(ubo_node)
        ubo_nodes[row['id']] = ubo_node

    transaction_nodes = {}
    for _, row in transactions.iterrows():
        transaction_node = Node("Transazione", id=row['id'], type=row['type'], amount=row['amount'], date=row['date'], currency=row['currency'])
        graph.create(transaction_node)
        transaction_nodes[row['id']] = transaction_node

    # Creazione dei nodi per le aziende e delle relazioni
    for _, row in companies.iterrows():
        company_node = Node("Azienda", id=row['id'], name=row['name'], address=row['address'], legal_form=row['legal_form'], registration_details=row['registration_details'], financial_data=row['financial_data'])
        graph.create(company_node)

        # Relazione con Amministratori
        for admin_id in eval(row['administrators']):
            rel = Relationship(company_node, "AZIENDA_HA_AMMINISTRATORE", admin_nodes[admin_id], ruolo="Amministratore", data_inizio=fake.date_between(start_date='-5y', end_date='-1y'), data_fine=None)
            graph.create(rel)

        # Relazione con Azionisti
        for shareholder_id in eval(row['shareholders']):
            rel = Relationship(company_node, "AZIENDA_HA_AZIONISTA", shareholder_nodes[shareholder_id], percentuale_partecipazione=random.uniform(0.1, 100), data_acquisto=fake.date_between(start_date='-10y', end_date='-1y'))
            graph.create(rel)

        # Relazione con UBO
        for ubo_id in eval(row['ubo']):
            rel = Relationship(company_node, "AZIENDA_HA_UBO", ubo_nodes[ubo_id], percentuale_partecipazione=random.uniform(0.1, 100), data_acquisto=fake.date_between(start_date='-10y', end_date='-1y'))
            graph.create(rel)

        # Relazione con Transazioni
        for transaction_id in eval(row['transactions']):
            rel = Relationship(company_node, "AZIENDA_HA_TRANSAZIONE", transaction_nodes[transaction_id], tipo=random.choice(['Purchase', 'Sale', 'Payment', 'Refund']), importo=random.uniform(10.0, 10000.0), data=fake.date_between(start_date='-5y', end_date='today'), valuta=random.choice(['EUR', 'USD', 'GBP', 'JPY', 'AUD']))
            graph.create(rel)

    # Relazione tra UBO e Controlli KYC/AML
    for _, row in kyc_aml_checks.iterrows():
        rel = Relationship(ubo_nodes[row['ubo_id']], "UBO_HA_CONTROLLI", Node("Controllo_KYC_AML", id=row['id'], type=row['type'], result=row['result'], date=row['date'], notes=row['notes']), tipo=row['type'], esito=row['result'], data=row['date'], note=row['notes'])
        graph.create(rel)

# Lettura dei dati dai CSV
admins = pd.read_csv('Dataset/administrators.csv', encoding='ISO-8859-1')
shareholders = pd.read_csv('Dataset/shareholders.csv', encoding='ISO-8859-1')
ubos = pd.read_csv('Dataset/ubo.csv', encoding='ISO-8859-1')
transactions = pd.read_csv('Dataset/transactions.csv', encoding='ISO-8859-1')
companies = pd.read_csv('Dataset/companies.csv', encoding='ISO-8859-1')
kyc_aml_checks = pd.read_csv('Dataset/kyc_aml_checks.csv', encoding='ISO-8859-1')

# Connessione ai vari database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Creazione dei grafi
create_graph(graph100, admins, shareholders, ubos, transactions, companies, kyc_aml_checks)
create_graph(graph75, admins.sample(frac=0.75), shareholders.sample(frac=0.75), ubos.sample(frac=0.75), transactions.sample(frac=0.75), companies.sample(frac=0.75), kyc_aml_checks.sample(frac=0.75))
create_graph(graph50, admins.sample(frac=0.50), shareholders.sample(frac=0.50), ubos.sample(frac=0.50), transactions.sample(frac=0.50), companies.sample(frac=0.50), kyc_aml_checks.sample(frac=0.50))
create_graph(graph25, admins.sample(frac=0.25), shareholders.sample(frac=0.25), ubos.sample(frac=0.25), transactions.sample(frac=0.25), companies.sample(frac=0.25), kyc_aml_checks.sample(frac=0.25))

print("Dati caricati in Neo4j con successo.")
