import random
import csv
from faker import Faker

fake = Faker()

# Definizione delle costanti
NUM_COMPANIES = 10000

# Genera nomi di forma giuridica
forme_giuridiche = ['S.r.l.', 'S.p.A.', 'S.a.S.', 'S.n.C.', 'S.r.l. a socio unico', 'Cooperative', 'Onlus']

# Inizializza la lista per tracciare gli ID già assegnati
used_company_ids = set()

# Creazione della collezione 'Aziende'
with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['company_id', 'company_name', 'company_address', 'legal_form', 'registration_details', 'financial_data']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for company_id in range(1, NUM_COMPANIES + 1):
        company_name = fake.company()
        company_address = fake.address()
        legal_form = random.choice(forme_giuridiche)
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

print("File CSV 'dataset_companies.csv' creato con successo.")
