import random
import csv
from faker import Faker

fake = Faker()

# Definizione delle costanti
NUM_COMPANIES = 10000
NUM_UBO = 1000
NUM_TRANSACTIONS = 20000
NUM_KYC_AML_CHECKS = 5000  # Numero di controlli KYC/AML da generare

# Genera nomi di forma giuridica
legal_forms = ['S.r.l.', 'S.p.A.', 'S.a.S.', 'S.n.C.', 'S.r.l. a socio unico', 'Cooperative', 'Onlus']
currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD']

# Inizializza la lista per tracciare gli ID già assegnati
used_company_ids = set()
used_ubo_ids = set()
used_transaction_ids = set()
used_kyc_aml_ids = set()

# Creazione della collezione 'Aziende'
with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['company_id', 'company_name', 'company_address', 'legal_form', 'registration_details', 'financial_data']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for company_id in range(1, NUM_COMPANIES + 1):
        company_name = fake.company()
        company_address = fake.address()
        legal_form = random.choice(legal_forms)
        registration_details = fake.ssn()  # Simuliamo i dettagli di registrazione con numeri di previdenza sociale
        financial_data = [
            {'year': random.randint(2015, 2023), 'revenue': round(random.uniform(10000, 1000000), 2), 'profit': round(random.uniform(1000, 500000), 2)}
            for _ in range(random.randint(1, 5))
        ]
        
        writer.writerow({
            'company_id': company_id,
            'company_name': company_name,
            'company_address': company_address,
            'legal_form': legal_form,
            'registration_details': registration_details,
            'financial_data': financial_data
        })
        used_company_ids.add(company_id)

print("File CSV 'companies.csv' creato con successo.")

# Creazione della collezione 'Proprietari Benefici Ultimi (UBO)'
with open('Dataset/ubo.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'birthdate', 'nationality', 'ownership_percentage', 'type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for ubo_id in range(1, NUM_UBO + 1):
        name = fake.name()
        address = fake.address()
        birthdate = fake.date_of_birth()
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
        used_ubo_ids.add(ubo_id)

print("File CSV 'ubo.csv' creato con successo.")

# Creazione della collezione 'Transazioni'
with open('Dataset/transactions.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'amount', 'date', 'currency']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for transaction_id in range(1, NUM_TRANSACTIONS + 1):
        transaction_type = random.choice(['Purchase', 'Sale', 'Payment', 'Refund'])
        amount = round(random.uniform(10.0, 10000.0), 2)
        date = fake.date_between(start_date='-5y', end_date='today')
        currency = random.choice(currencies)

        writer.writerow({
            'id': transaction_id,
            'type': transaction_type,
            'amount': amount,
            'date': date,
            'currency': currency
        })
        used_transaction_ids.add(transaction_id)

print("File CSV 'transactions.csv' creato con successo.")

# Creazione della collezione 'Controlli KYC/AML'
with open('Dataset/kyc_aml_checks.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'result', 'date', 'notes']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for kyc_aml_id in range(1, NUM_KYC_AML_CHECKS + 1):
        check_type = random.choice(['Identity Verification', 'Sanctions Check', 'Transaction Monitoring'])
        result = random.choice(['Passed', 'Failed'])
        date = fake.date_between(start_date='-5y', end_date='today')
        notes = fake.text(max_nb_chars=200)

        writer.writerow({
            'id': kyc_aml_id,
            'type': check_type,
            'result': result,
            'date': date,
            'notes': notes
        })
        used_kyc_aml_ids.add(kyc_aml_id)

print("File CSV 'kyc_aml_checks.csv' creato con successo.")
