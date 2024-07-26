import random
import csv
from faker import Faker

fake = Faker()



NUM_ADMINISTRATORS = 100  # Numero di amministratori da generare
# Definizione delle costanti
NUM_COMPANIES = 10000
NUM_UBO = 1000
NUM_TRANSACTIONS = 20000
NUM_KYC_AML_CHECKS = 5000  # Numero di controlli KYC/AML da generare
NUM_SHAREHOLDERS = 100  # Numero di azionisti da generare

# Genera nomi di forma giuridica
legal_forms = ['S.r.l.', 'S.p.A.', 'S.a.S.', 'S.n.C.', 'S.r.l. a socio unico', 'Cooperative', 'Onlus']
currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD']

# Inizializza la lista per tracciare gli ID già assegnati
used_company_ids = set()
used_ubo_ids = set()
used_transaction_ids = set()
used_kyc_aml_ids = set()

# Creazione della collezione 'Administrators'
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




# Creazione della collezione 'Shareholders'
with open('Dataset/shareholders.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'type', 'ownership_percentage', 'address', 'date_of_birth', 'nationality']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for shareholder_id in range(1, NUM_SHAREHOLDERS + 1):
        name = fake.name()
        shareholder_type = random.choice(['Person', 'Company'])
        ownership_percentage = round(random.uniform(0.1, 100), 2)  # Percentuale tra 0.1 e 100
        address = fake.address()

        # I campi date_of_birth e nationality sono compilati solo se il tipo è 'Person'
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
        
print("File CSV 'shareholders.csv' creato con successo.")


# Creazione della collezione 'Aziende'
with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'legal_form', 'registration_details', 'financial_data']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for company_id in range(1, NUM_COMPANIES + 1):
        name = fake.company()
        address = fake.address()
        legal_form = random.choice(legal_forms)
        registration_details = fake.ssn()  # Simuliamo i dettagli di registrazione con numeri di previdenza sociale
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
            'financial_data': financial_data
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
        type = random.choice(['Purchase', 'Sale', 'Payment', 'Refund'])
        amount = round(random.uniform(10.0, 10000.0), 2)
        date = fake.date_between(start_date='-5y', end_date='today')
        currency = random.choice(currencies)

        writer.writerow({
            'id': transaction_id,
            'type': type,
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
        type = random.choice(['Identity Verification', 'Sanctions Check', 'Transaction Monitoring'])
        result = random.choice(['Passed', 'Failed'])
        date = fake.date_between(start_date='-5y', end_date='today')
        notes = fake.text(max_nb_chars=200)

        writer.writerow({
            'id': kyc_aml_id,
            'type': type,
            'result': result,
            'date': date,
            'notes': notes
        })
        used_kyc_aml_ids.add(kyc_aml_id)

print("File CSV 'kyc_aml_checks.csv' creato con successo.")