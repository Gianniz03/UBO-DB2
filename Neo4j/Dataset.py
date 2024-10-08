import pandas as pd
from py2neo import Graph, Node, Relationship
from faker import Faker
import random

# Inizializza l'oggetto Faker per generare dati fittizi
fake = Faker()

# Funzione per creare nodi e relazioni nel grafo Neo4j a partire dai dataset forniti.
def create_graph(graph, admins, shareholders, ubos, transactions, companies, kyc_aml_checks):
    # Crea nodi per gli amministratori
    admin_nodes = {}
    for _, row in admins.iterrows():
        admin_node = Node("Administrators", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'])
        graph.create(admin_node)
        admin_nodes[row['id']] = admin_node

    # Crea nodi per gli azionisti
    shareholder_nodes = {}
    for _, row in shareholders.iterrows():
        shareholder_node = Node("Shareholders", id=row['id'], name=row['name'], type=row['type'], ownership_percentage=row['ownership_percentage'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'])
        graph.create(shareholder_node)
        shareholder_nodes[row['id']] = shareholder_node

    # Crea nodi per gli UBO (Ultimate Beneficial Owners)
    ubo_nodes = {}
    for _, row in ubos.iterrows():
        ubo_node = Node("Ubo", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'], ownership_percentage=row['ownership_percentage'], type=row['type'])
        graph.create(ubo_node)
        ubo_nodes[row['id']] = ubo_node

    # Crea nodi per le transazioni
    transaction_nodes = {}
    for _, row in transactions.iterrows():
        transaction_node = Node("Transactions", id=row['id'], type=row['type'], amount=row['amount'], date=row['date'], currency=row['currency'])
        graph.create(transaction_node)
        transaction_nodes[row['id']] = transaction_node

    # Crea nodi per le aziende e le relazioni con altri nodi
    for _, row in companies.iterrows():
        company_node = Node("Companies", id=row['id'], name=row['name'], address=row['address'], legal_form=row['legal_form'], registration_details=row['registration_details'], financial_data=row['financial_data'])
        graph.create(company_node)

        # Crea relazioni tra aziende e amministratori
        for admin_id in eval(row['administrators']):
            if admin_id in admin_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_ADMINISTRATOR", admin_nodes[admin_id], role="Administrator", start_date=fake.date_between(start_date='-5y', end_date='-1y'), end_date=None)
                graph.create(rel)

        # Crea relazioni tra aziende e azionisti
        for shareholder_id in eval(row['shareholders']):
            if shareholder_id in shareholder_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_SHAREHOLDER", shareholder_nodes[shareholder_id], ownership_percentage=random.uniform(0.1, 100), purchase_date=fake.date_between(start_date='-10y', end_date='-1y'))
                graph.create(rel)

        # Crea relazioni tra aziende e UBO
        for ubo_id in eval(row['ubo']):
            if ubo_id in ubo_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_UBO", ubo_nodes[ubo_id], ownership_percentage=random.uniform(0.1, 100), purchase_date=fake.date_between(start_date='-10y', end_date='-1y'))
                graph.create(rel)

        # Crea relazioni tra aziende e transazioni
        for transaction_id in eval(row['transactions']):
            if transaction_id in transaction_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_TRANSACTION", transaction_nodes[transaction_id], transaction_type=random.choice(['Purchase', 'Sale', 'Payment', 'Refund']), amount=random.uniform(10.0, 10000.0), date=fake.date_between(start_date='-5y', end_date='today'), currency=random.choice(['EUR', 'USD', 'GBP', 'JPY', 'AUD']))
                graph.create(rel)

    # Crea relazioni per i controlli KYC/AML
    for _, row in kyc_aml_checks.iterrows():
        if row['ubo_id'] in ubo_nodes:
            rel = Relationship(ubo_nodes[row['ubo_id']], "UBO_HAS_CHECKS", Node("KYC_AML_Check", id=row['id'], check_type=row['type'], result=row['result'], date=row['date'], notes=row['notes']), check_type=row['type'], result=row['result'], date=row['date'], notes=row['notes'])
            graph.create(rel)

    # Aggiungi nodi speciali
    special_admin = Node("Administrators", id=999999999, name='Special Administrator', address='123 Special Lane, Special City, SP', birthdate='1970-01-01', nationality='Special Country')
    graph.create(special_admin)

    special_company = Node("Companies", id=999999999, name='Special Company', address='123 Special Ave, Special City, SC', legal_form='S.p.A.', registration_details='SPECIAL-REG-001', financial_data='[{"year":2024,"revenue":100000,"profit":50000}]', administrators='[999999999]', shareholders='[999999999]', ubo='[999999999]', transactions='[999999999]', kyc_aml_checks='[999999999]')
    graph.create(special_company)

    special_ubo = Node("Ubo", id=999999999, name='Special Name', address='Special Address', birthdate='2024-01-01', nationality='Special Nationality', ownership_percentage=55.5, type='Company')
    graph.create(special_ubo)

    special_shareholder = Node("Shareholders", id=999999999, name='Special Shareholder', type='Person', ownership_percentage=100.0, address='123 Special Lane', date_of_birth='1980-01-01', nationality='Special Country')
    graph.create(special_shareholder)

    special_transaction = Node("Transactions", id=999999999, type='Payment', date='2024-01-01', amount=1000.0, currency='USD')
    graph.create(special_transaction)

    special_check = Node("KYC_AML_Check", id=999999999, check_type='Special Check', result='Passed', date='2024-01-01', notes='This is a special KYC/AML check document included in all subsets.')
    graph.create(special_check)

    # Crea relazioni tra nodi speciali
    graph.create(Relationship(special_company, "COMPANY_HAS_ADMINISTRATOR", special_admin, role="Special Administrator", start_date=fake.date_between(start_date='-5y', end_date='-1y'), end_date=None))
    graph.create(Relationship(special_company, "COMPANY_HAS_SHAREHOLDER", special_shareholder, ownership_percentage=100.0, purchase_date=fake.date_between(start_date='-10y', end_date='-1y')))
    graph.create(Relationship(special_company, "COMPANY_HAS_UBO", special_ubo, ownership_percentage=55.5, purchase_date=fake.date_between(start_date='-10y', end_date='-1y')))
    graph.create(Relationship(special_company, "COMPANY_HAS_TRANSACTION", special_transaction, transaction_type='Payment', amount=1000.0, date='2024-01-01', currency='USD'))
    graph.create(Relationship(special_ubo, "UBO_HAS_CHECKS", special_check, check_type='Special Check', result='Passed', date='2024-01-01', notes='This is a special KYC/AML check document included in all subsets.'))

# Carica i dataset dai file CSV
admins = pd.read_csv('Dataset/File/administrators.csv', encoding='ISO-8859-1')
shareholders = pd.read_csv('Dataset/File/shareholders.csv', encoding='ISO-8859-1')
ubos = pd.read_csv('Dataset/File/ubo.csv', encoding='ISO-8859-1')
transactions = pd.read_csv('Dataset/File/transactions.csv', encoding='ISO-8859-1')
companies = pd.read_csv('Dataset/File/companies.csv', encoding='ISO-8859-1')
kyc_aml_checks = pd.read_csv('Dataset/File/kyc_aml_checks.csv', encoding='ISO-8859-1')

# Connessione ai database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Crea i grafi per i diversi dataset

# Il 100% del dataset completo
create_graph(graph100, admins, shareholders, ubos, transactions, companies, kyc_aml_checks)

# Prendi il 75% dal 100%
admins_75 = admins.sample(frac=0.75)
shareholders_75 = shareholders.sample(frac=0.75)
ubos_75 = ubos.sample(frac=0.75)
transactions_75 = transactions.sample(frac=0.75)
companies_75 = companies.sample(frac=0.75)
kyc_aml_checks_75 = kyc_aml_checks.sample(frac=0.75)
create_graph(graph75, admins_75, shareholders_75, ubos_75, transactions_75, companies_75, kyc_aml_checks_75)

# Prendi il 50% dal 75%
admins_50 = admins_75.sample(frac=0.6667)  # 50% del totale = 66.67% del 75%
shareholders_50 = shareholders_75.sample(frac=0.6667)
ubos_50 = ubos_75.sample(frac=0.6667)
transactions_50 = transactions_75.sample(frac=0.6667)
companies_50 = companies_75.sample(frac=0.6667)
kyc_aml_checks_50 = kyc_aml_checks_75.sample(frac=0.6667)
create_graph(graph50, admins_50, shareholders_50, ubos_50, transactions_50, companies_50, kyc_aml_checks_50)

# Prendi il 25% dal 50%
admins_25 = admins_50.sample(frac=0.5)  # 25% del totale = 50% del 50%
shareholders_25 = shareholders_50.sample(frac=0.5)
ubos_25 = ubos_50.sample(frac=0.5)
transactions_25 = transactions_50.sample(frac=0.5)
companies_25 = companies_50.sample(frac=0.5)
kyc_aml_checks_25 = kyc_aml_checks_50.sample(frac=0.5)
create_graph(graph25, admins_25, shareholders_25, ubos_25, transactions_25, companies_25, kyc_aml_checks_25)

# Stampa un messaggio di conferma
print("Data successfully loaded into Neo4j.")
