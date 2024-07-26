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

































































































































































































































































NUM_AMMINISTRATORI = 100  # Numero di amministratori da generare

# Creazione della collezione 'Administrators'
with open('Dataset/administrators.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'nome', 'indirizzo', 'data_di_nascita', 'nazionalità']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for amministratore_id in range(1, NUM_AMMINISTRATORI + 1):
        nome = fake.name()
        indirizzo = fake.address()
        data_di_nascita = fake.date_of_birth(minimum_age=25, maximum_age=70).strftime('%Y-%m-%d')
        nazionalità = fake.country()

        writer.writerow({
            'id': amministratore_id,
            'nome': nome,
            'indirizzo': indirizzo,
            'data_di_nascita': data_di_nascita,
            'nazionalità': nazionalità
        })

print("File CSV 'administrators.csv' creato con successo.")


NUM_AZIONISTI = 100  # Numero di azionisti da generare

# Creazione della collezione 'Shareholders'
with open('Dataset/shareholders.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'nome', 'tipo', 'percentuale_partecipazione', 'indirizzo', 'data_di_nascita', 'nazionalità']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for azionista_id in range(1, NUM_AZIONISTI + 1):
        nome = fake.name()
        tipo = random.choice(['Persona', 'Azienda'])
        percentuale_partecipazione = round(random.uniform(0.1, 100), 2)  # Percentuale tra 0.1 e 100
        indirizzo = fake.address()

        # I campi data_di_nascita e nazionalità sono compilati solo se il tipo è 'Persona'
        data_di_nascita = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d') if tipo == 'Persona' else ''
        nazionalità = fake.country() if tipo == 'Persona' else ''

        writer.writerow({
            'id': azionista_id,
            'nome': nome,
            'tipo': tipo,
            'percentuale_partecipazione': percentuale_partecipazione,
            'indirizzo': indirizzo,
            'data_di_nascita': data_di_nascita,
            'nazionalità': nazionalità
        })

print("File CSV 'shareholders.csv' creato con successo.")