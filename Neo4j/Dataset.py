import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Directory containing the CSV files
csv_directory = r'C:\Users\gznan\Documents\GitHub\UBO-DB2\Dataset'

# Neo4j graph connections
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Dictionary to map percentages to graphs
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# CSV file names
csv_files = [
    "companies.csv",
    "administrators.csv",
    "shareholders.csv",
    "ubo.csv",
    "transactions.csv",
    "kyc_aml_checks.csv"
]

# Function to clean ID values
def clean_id(value):
    if isinstance(value, str):
        # Remove any unwanted characters like brackets and spaces
        return value.strip('[]').strip()
    return str(value)  # Convert integers to string for further processing

# Function to create nodes and relationships in Neo4j
def create_nodes_and_relationships(csv_file, data, graph):
    for index, row in data.iterrows():
        node_label = csv_file.split(".")[0].capitalize()
        node = Node(node_label, **row.to_dict())
        graph.create(node)

        if "companies" in csv_file:
            company_id = clean_id(row['id'])

            # Relationships with administrators
            admin_ids = clean_id(row.get('administrators', '')).split(',')
            for admin_id in admin_ids:
                try:
                    admin_node = graph.nodes.match("Administrator", id=int(admin_id)).first()
                    if admin_node:
                        company_to_admin = Relationship(node, 'HAS_ADMINISTRATOR', admin_node)
                        graph.create(company_to_admin)
                except ValueError:
                    print(f"Invalid administrator ID: {admin_id}")

            # Relationships with shareholders
            shareholder_ids = clean_id(row.get('shareholders', '')).split(',')
            for shareholder_id in shareholder_ids:
                try:
                    shareholder_node = graph.nodes.match("Shareholder", id=int(shareholder_id)).first()
                    if shareholder_node:
                        company_to_shareholder = Relationship(node, 'HAS_SHAREHOLDER', shareholder_node)
                        graph.create(company_to_shareholder)
                except ValueError:
                    print(f"Invalid shareholder ID: {shareholder_id}")

            # Relationships with UBOs
            ubo_ids = clean_id(row.get('ubo', '')).split(',')
            for ubo_id in ubo_ids:
                try:
                    ubo_node = graph.nodes.match("UBO", id=int(ubo_id)).first()
                    if ubo_node:
                        company_to_ubo = Relationship(node, 'HAS_UBO', ubo_node)
                        graph.create(company_to_ubo)
                except ValueError:
                    print(f"Invalid UBO ID: {ubo_id}")

            # Relationships with transactions
            transaction_ids = clean_id(row.get('transactions', '')).split(',')
            for transaction_id in transaction_ids:
                try:
                    transaction_node = graph.nodes.match("Transaction", id=int(transaction_id)).first()
                    if transaction_node:
                        company_to_transaction = Relationship(node, 'HAS_TRANSACTION', transaction_node)
                        graph.create(company_to_transaction)
                except ValueError:
                    print(f"Invalid transaction ID: {transaction_id}")

        elif "ubo" in csv_file:
            ubo_id = clean_id(row['id'])

            # Relationships with KYC/AML checks
            kyc_aml_ids = clean_id(row.get('kyc_aml_checks', '')).split(',')
            for kyc_aml_id in kyc_aml_ids:
                try:
                    kyc_aml_node = graph.nodes.match("KYCAMLCheck", id=int(kyc_aml_id)).first()
                    if kyc_aml_node:
                        ubo_to_kyc_aml = Relationship(node, 'HAS_KYC_AML_CHECK', kyc_aml_node)
                        graph.create(ubo_to_kyc_aml)
                except ValueError:
                    print(f"Invalid KYC/AML check ID: {kyc_aml_id}")

# Import data from CSV files into Neo4j graphs and create relationships
for csv_file in csv_files:
    for percentage, graph in graphs_by_percentage.items():
        # Complete path to the CSV file
        csv_path = os.path.join(csv_directory, csv_file)

        # Read data from the CSV file using pandas
        data = pd.read_csv(csv_path, encoding='ISO-8859-1')

        # Calculate the number of rows to insert for the specific percentage
        rows_to_insert = int(len(data) * (percentage / 100))

        # Take only the first "rows_to_insert" rows
        data = data.head(rows_to_insert)

        # Create nodes and relationships in the graph
        create_nodes_and_relationships(csv_file, data, graph)

        print(f"{percentage}% of the data from {csv_file} inserted into Neo4j successfully in dataset {percentage}%.")

print("Data insertion and relationship creation completed for all datasets.")
