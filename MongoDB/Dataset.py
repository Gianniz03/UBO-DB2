import random
import csv
import json
from faker import Faker

fake = Faker()

NUM_ADMINISTRATORS = 100
NUM_COMPANIES = 10000
NUM_UBO = 1000
NUM_TRANSACTIONS = 20000
NUM_KYC_AML_CHECKS = 5000
NUM_SHAREHOLDERS = 100

legal_forms = ['S.r.l.', 'S.p.A.', 'S.a.S.', 'S.n.C.', 'S.r.l. a socio unico', 'Cooperative', 'Onlus']
currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD']

# Generate administrators
administrators = []
for administrator_id in range(1, NUM_ADMINISTRATORS + 1):
    name = fake.name()
    address = fake.address()
    birthdate = fake.date_of_birth(minimum_age=25, maximum_age=70).strftime('%Y-%m-%d')
    nationality = fake.country()
    administrators.append({
        'id': administrator_id,
        'name': name,
        'address': address,
        'birthdate': birthdate,
        'nationality': nationality
    })

with open('Dataset/administrators.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'birthdate', 'nationality']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(administrators)

print("File CSV 'administrators.csv' creato con successo.")

# Generate shareholders
shareholders = []
for shareholder_id in range(1, NUM_SHAREHOLDERS + 1):
    name = fake.name()
    shareholder_type = random.choice(['Person', 'Company'])
    ownership_percentage = round(random.uniform(0.1, 100), 2)
    address = fake.address()
    date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d') if shareholder_type == 'Person' else ''
    nationality = fake.country() if shareholder_type == 'Person' else ''

    shareholders.append({
        'id': shareholder_id,
        'name': name,
        'type': shareholder_type,
        'ownership_percentage': ownership_percentage,
        'address': address,
        'date_of_birth': date_of_birth,
        'nationality': nationality
    })

with open('Dataset/shareholders.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'type', 'ownership_percentage', 'address', 'date_of_birth', 'nationality']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(shareholders)

print("File CSV 'shareholders.csv' creato con successo.")

# Generate UBOs
ubos = []
for ubo_id in range(1, NUM_UBO + 1):
    name = fake.name()
    address = fake.address()
    birthdate = fake.date_of_birth().strftime('%Y-%m-%d')
    nationality = fake.country()
    ownership_percentage = round(random.uniform(0.1, 100.0), 2)
    ubo_type = random.choice(['Person', 'Company'])
    
    ubos.append({
        'id': ubo_id,
        'name': name,
        'address': address,
        'birthdate': birthdate,
        'nationality': nationality,
        'ownership_percentage': ownership_percentage,
        'type': ubo_type
    })

with open('Dataset/ubo.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'birthdate', 'nationality', 'ownership_percentage', 'type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(ubos)

print("File CSV 'ubo.csv' creato con successo.")

# Generate transactions
transactions = []
for transaction_id in range(1, NUM_TRANSACTIONS + 1):
    transaction_type = random.choice(['Purchase', 'Sale', 'Payment', 'Refund'])
    amount = round(random.uniform(10.0, 10000.0), 2)
    date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    currency = random.choice(currencies)
    
    transactions.append({
        'id': transaction_id,
        'type': transaction_type,
        'amount': amount,
        'date': date,
        'currency': currency
    })

with open('Dataset/transactions.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'amount', 'date', 'currency']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(transactions)

print("File CSV 'transactions.csv' creato con successo.")

# Generate KYC/AML checks
kyc_aml_checks = []
for kyc_aml_id in range(1, NUM_KYC_AML_CHECKS + 1):
    ubo_id = random.randint(1, NUM_UBO)
    check_type = random.choice(['Identity Verification', 'Sanctions Check', 'Transaction Monitoring'])
    result = random.choice(['Passed', 'Failed'])
    date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    notes = fake.text(max_nb_chars=200)
    
    kyc_aml_checks.append({
        'id': kyc_aml_id,
        'ubo_id': ubo_id,
        'type': check_type,
        'result': result,
        'date': date,
        'notes': notes
    })

with open('Dataset/kyc_aml_checks.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'ubo_id', 'type', 'result', 'date', 'notes']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(kyc_aml_checks)

print("File CSV 'kyc_aml_checks.csv' creato con successo.")

# Generate companies with integrated relationships
companies = []
for company_id in range(1, NUM_COMPANIES + 1):
    name = fake.company()
    address = fake.address()
    legal_form = random.choice(legal_forms)
    registration_details = fake.ssn()
    financial_data = [
        {'year': random.randint(2015, 2023), 'revenue': round(random.uniform(10000, 1000000), 2), 'profit': round(random.uniform(1000, 500000), 2)}
        for _ in range(random.randint(1, 5))
    ]
    
    num_administrators = random.randint(1, 3)
    administrators_ids = random.sample(range(1, NUM_ADMINISTRATORS + 1), num_administrators)
    
    num_shareholders = random.randint(1, 10)
    shareholders_ids = random.sample(range(1, NUM_SHAREHOLDERS + 1), num_shareholders)
    
    num_ubos = random.randint(1, 3)
    ubos_ids = random.sample(range(1, NUM_UBO + 1), num_ubos)
    
    num_transactions = random.randint(1, 5)
    transactions_ids = random.sample(range(1, NUM_TRANSACTIONS + 1), num_transactions)
    
    num_kyc_aml_checks = random.randint(1, 3)
    kyc_aml_checks_ids = random.sample(range(1, NUM_KYC_AML_CHECKS + 1), num_kyc_aml_checks)
    
    companies.append({
        'id': company_id,
        'name': name,
        'address': address,
        'legal_form': legal_form,
        'registration_details': registration_details,
        'financial_data': json.dumps(financial_data),  # Serializza come JSON
        'administrators': administrators_ids,
        'shareholders': shareholders_ids,
        'ubo': ubos_ids,
        'transactions': transactions_ids,
        'kyc_aml_checks': kyc_aml_checks_ids
    })

with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'legal_form', 'registration_details', 'financial_data', 'administrators', 'shareholders', 'ubo', 'transactions', 'kyc_aml_checks']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(companies)

print("File CSV 'companies.csv' creato con successo.")
