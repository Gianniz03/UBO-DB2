import random
import csv
from faker import Faker

fake = Faker()

































































































































































































































































NUM_ADMINISTRATORS = 100  # Numero di amministratori da generare

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


NUM_SHAREHOLDERS = 100  # Numero di azionisti da generare

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
        })

print("File CSV 'shareholders.csv' creato con successo.")