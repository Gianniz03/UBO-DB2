import random
import csv
from faker import Faker

fake = Faker()

# Definizione delle costanti
NUM_COMPANIES = 10000
NUM_UBO = 1000  # Numero di UBO da generare

# Genera nomi di forma giuridica
forme_giuridiche = ['S.r.l.', 'S.p.A.', 'S.a.S.', 'S.n.C.', 'S.r.l. a socio unico', 'Cooperative', 'Onlus']

# Inizializza la lista per tracciare gli ID già assegnati
used_company_ids = set()
used_ubo_ids = set()

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

# Creazione della collezione 'Proprietari Benefici Ultimi (UBO)'
with open('Dataset/ubo.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'nome', 'indirizzo', 'data_di_nascita', 'nazionalità', 'percentuale_partecipazione', 'tipo']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for ubo_id in range(1, NUM_UBO + 1):
        name = fake.name()
        address = fake.address()
        birthdate = fake.date_of_birth()
        nationality = fake.country()
        ownership_percentage = round(random.uniform(0.1, 100.0), 2)
        ubo_type = random.choice(['Persona', 'Azienda'])

        writer.writerow({
            'id': ubo_id,
            'nome': name,
            'indirizzo': address,
            'data_di_nascita': birthdate,
            'nazionalità': nationality,
            'percentuale_partecipazione': ownership_percentage,
            'tipo': ubo_type
        })
        used_ubo_ids.add(ubo_id)

print("File CSV 'dataset_ubo.csv' creato con successo.")
