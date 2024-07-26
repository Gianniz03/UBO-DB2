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

with open('Dataset/administrators.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'birthdate', 'nationality']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for amministratore_id in range(1, NUM_ADMINISTRATORS + 1):
        nome = fake.name()
        address = fake.address()
        birthdate = fake.date_of_birth(minimum_age=25, maximum_age=70).strftime('%Y-%m-%d')
        nationality = fake.country()

        writer.writerow({
            'id': amministratore_id,
            'name': nome,
            'address': address,
            'birthdate': birthdate,
            'nationality': nationality
        })

print("File CSV 'administrators.csv' creato con successo.")

with open('Dataset/shareholders.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'type', 'ownership_percentage', 'address', 'date_of_birth', 'nationality']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for shareholder_id in range(1, NUM_SHAREHOLDERS + 1):
        name = fake.name()
        shareholder_type = random.choice(['Person', 'Company'])
        ownership_percentage = round(random.uniform(0.1, 100), 2)
        address = fake.address()

        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d') if shareholder_type == 'Person' else ''
        nationality = fake.country() if shareholder_type == 'Person' else ''

        writer.writerow({
            'id': shareholder_id,
            'name': name,
            'type': shareholder_type,
            'ownership_percentage': ownership_percentage,
            'address': address,
            'date_of_birth': date_of_birth,
            'nationality': nationality
        })

print("File CSV 'shareholders.csv' creato con successo.")

with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'legal_form', 'registration_details', 'financial_data']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for company_id in range(1, NUM_COMPANIES + 1):
        name = fake.company()
        address = fake.address()
        legal_form = random.choice(legal_forms)
        registration_details = fake.ssn()
        financial_data = [
            {'year': random.randint(2015, 2023), 'revenue': round(random.uniform(10000, 1000000), 2), 'profit': round(random.uniform(1000, 500000), 2)}
            for _ in range(random.randint(1, 5))
        ]

        writer.writerow({
            'id': company_id,
            'name': name,
            'address': address,
            'legal_form': legal_form,
            'registration_details': registration_details,
            'financial_data': json.dumps(financial_data)  # Serializza come JSON
        })

print("File CSV 'companies.csv' creato con successo.")

with open('Dataset/ubo.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'birthdate', 'nationality', 'ownership_percentage', 'type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for ubo_id in range(1, NUM_UBO + 1):
        name = fake.name()
        address = fake.address()
        birthdate = fake.date_of_birth().strftime('%Y-%m-%d')
        nationality = fake.country()
        ownership_percentage = round(random.uniform(0.1, 100.0), 2)
        ubo_type = random.choice(['Person', 'Company'])

        writer.writerow({
            'id': ubo_id,
            'name': name,
            'address': address,
            'birthdate': birthdate,
            'nationality': nationality,
            'ownership_percentage': ownership_percentage,
            'type': ubo_type
        })

print("File CSV 'ubo.csv' creato con successo.")

with open('Dataset/transactions.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'amount', 'date', 'currency']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for transaction_id in range(1, NUM_TRANSACTIONS + 1):
        type = random.choice(['Purchase', 'Sale', 'Payment', 'Refund'])
        amount = round(random.uniform(10.0, 10000.0), 2)
        date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        currency = random.choice(currencies)

        writer.writerow({
            'id': transaction_id,
            'type': type,
            'amount': amount,
            'date': date,
            'currency': currency
        })

print("File CSV 'transactions.csv' creato con successo.")

with open('Dataset/kyc_aml_checks.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'result', 'date', 'notes']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for kyc_aml_id in range(1, NUM_KYC_AML_CHECKS + 1):
        type = random.choice(['Identity Verification', 'Sanctions Check', 'Transaction Monitoring'])
        result = random.choice(['Passed', 'Failed'])
        date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        notes = fake.text(max_nb_chars=200)

        writer.writerow({
            'id': kyc_aml_id,
            'type': type,
            'result': result,
            'date': date,
            'notes': notes
        })

print("File CSV 'kyc_aml_checks.csv' creato con successo.")
