import json
from bson import ObjectId
from pymongo import MongoClient
import time
import csv
import scipy.stats as stats
import numpy as np

def json_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError("Type not serializable")

def convert_string_fields_to_json(doc):
    if isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, str):
                try:
                    # Prova a convertire la stringa in un oggetto JSON
                    doc[key] = json.loads(value)
                except json.JSONDecodeError:
                    # Se non è una stringa JSON valida, mantieni il valore originale
                    pass
    return doc

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    mean_value = np.mean(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return mean_value, margin_of_error

def measure_query_performance(db, query_number, percentuale, iterations=30):
    tempi_successivi = []
    
    for _ in range(iterations):
        start_time = time.time()
        if query_number == 1:
            query1(db, percentuale)
        elif query_number == 2:
            query2(db, percentuale)
        elif query_number == 3:
            query3(db, percentuale)
        elif query_number == 4:
            query4(db, percentuale)
        end_time = time.time()
        tempo_esecuzione = (end_time - start_time) * 1000  # Millisecondi
        tempi_successivi.append(tempo_esecuzione)
    
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    
    return tempo_medio_successive, mean, margin_of_error

def query1(db, percentuale):
    company_name = 'Sutton-Nolan'
    companies = f'Companies {percentuale}'  # Modifica con il nome corretto della collezione
    query = db[companies].find_one({"name": company_name})  # Modifica con la query corretta

    return company_name, query

def query2(db, percentuale):
    company_id = 200
    companies = f'Companies {percentuale}'
    administrators = f'Administrators {percentuale}'
    company = db[companies].find_one({"id": company_id})
    
    if not company:
        return company_id, False, []

    company = convert_string_fields_to_json(company)
    
    # Recupera dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators].find({"id": {"$in": admin_ids}}))

    # Aggiungi i dettagli all'azienda
    company['administrators_details'] = administrators_details

    return company_id, company, administrators_details

# Versione Alternativa
""" def query2(db, percentuale):
    company_id = 4
    companies = f'Companies {percentuale}'
    administrators = f'Administrators {percentuale}'
    company = db[companies].find_one({"id": company_id})
    
    if company:
        company = convert_string_fields_to_json(company)
        if 'administrators' in company and isinstance(company['administrators'], list):
            administrators = list(db[administrators].find({"id": {"$in": company['administrators']}}))
            company['administrators_details'] = administrators
        else:
            administrators = []
            company['administrators_details'] = []
    else:
        return company_id, False, []

    return company_id, company, administrators """

def query3(db, percentuale):
    companies_collection = f'Companies {percentuale}'
    administrators_collection = f'Administrators {percentuale}'
    ubos_collection = f'UBO {percentuale}'
    company_id = 4550  # Cambia questo ID in base ai tuoi dati reali

    company = db[companies_collection].find_one({"id": company_id})

    if not company:
        return company_id, False, [], []

    company = convert_string_fields_to_json(company)
    
    # Recupera dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Recupera dettagli degli UBO
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Aggiungi i dettagli all'azienda
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details

    return company_id, company, administrators_details, ubos_details

from datetime import datetime

def query4(db, percentuale):
    companies_collection = f'Companies {percentuale}'
    administrators_collection = f'Administrators {percentuale}'
    ubos_collection = f'UBO {percentuale}'
    transactions_collection = f'Transactions {percentuale}'
    
    company_id = 1  # Cambia questo ID in base ai tuoi dati reali

    # Recupera l'azienda
    company = db[companies_collection].find_one({"id": company_id})
    
    if not company:
        return company_id, False, [], [], 0
    
    company = convert_string_fields_to_json(company)
    
    # Recupera dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Recupera dettagli degli UBO
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Recupera la somma delle transazioni
    transaction_ids = company.get('transactions', [])
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    
    transaction_summary = db[transactions_collection].aggregate([
        {"$match": {"id": {"$in": transaction_ids}, "date": {"$gte": start_date, "$lte": end_date}}},
        {"$group": {"_id": None, "total_amount": {"$sum": "$amount"}}}
    ])

    # Estrai il totale delle transazioni
    total_amount = 0
    for record in transaction_summary:
        total_amount = record.get('total_amount', 0)
    
    # Aggiungi i dettagli all'azienda
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details
    company['transactions_summary'] = total_amount
    
    return company_id, company, administrators_details, ubos_details, total_amount

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['UBO']  # Cambia 'DatabaseName' con il tuo nome del database
    
    percentuali = ['100%', '75%', '50%', '25%']  # Percentuali dei dataset

    tempi_di_risposta_prima_esecuzione = {}
    tempi_di_risposta_media = {}

    for percentuale in percentuali:
        print(f"\nAnalisi per la percentuale: {percentuale}\n")

        # Query 1
        start_time = time.time()
        company_name, query = query1(db, percentuale)
        if query:
            query_json = convert_string_fields_to_json(query)
            json_result = json.dumps(query_json, indent=4, default=json_serializer)
            print(f"Nome dell'azienda con il nome specificato: \n{json_result}\n")
        else:
            json_result = json.dumps({"error": "No results found"})
            print(f"Nessuna azienda trovata con il nome specificato: {company_name}\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 1): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 1"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(db, 1, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 1): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 1"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 2: Recupera dettagli di un'azienda e i dettagli dei suoi amministratori
        start_time = time.time()
        company_id, company, administrators = query2(db, percentuale)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            #administrators_json = json.dumps(administrators, indent=4, default=json_serializer)
            print(f"Dettagli dell'azienda con ID {company_id} e degli amministratori: \n{company_json}\n")
            #print(f"Dettagli Amministratori dell'azienda con ID {company_id} specificato: \n{administrators_json}\n")
        else:
            print(f"Nessuna azienda trovata con ID {company_id}\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 2): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 2"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(db, 2, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 2): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 2"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 3: Recupera dettagli di un'azienda, i suoi amministratori e UBO con più del 25%
        start_time = time.time()
        company_id, company, administrators, ubos = query3(db, percentuale)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Dettagli dell'azienda con ID {company_id}, amministratori e UBO: \n{company_json}\n")
        else:
            print(f"Nessuna azienda trovata con ID {company_id}\n")
        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 3"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(db, 3, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 3): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 3"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 4: Recupera dettagli di un'azienda, i suoi amministratori, UBO con più del 25% e la somma delle transazioni in un certo periodo
        start_time = time.time()
        company_id, company, administrators_details, ubos_details, total_amount = query4(db, percentuale)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Dettagli dell'azienda con ID {company_id}, amministratori e UBO e transazioni: \n{company_json}\n")
        else:
            print(f"Nessuna azienda trovata con ID {company_id}\n")
        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 4"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(db, 4, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 4): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 4"] = (tempo_medio_successive, mean, margin_of_error)

        print("-" * 70)  # Separatore tra le diverse percentuali

    # Scrivo i tempi di risposta medi della prima esecuzione in un file CSV
    with open('MongoDB/ResponseTimes/mongodb_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Millisecondi'])
        for query, tempo_prima_esecuzione in tempi_di_risposta_prima_esecuzione.items():
            dataset, query = query.split(' - ')
            writer.writerow([dataset, query, tempo_prima_esecuzione])

    with open('MongoDB/ResponseTimes/mongodb_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza (Min, Max)'])
        for query, (tempo_medio_successive, mean_value, margin_of_error) in tempi_di_risposta_media.items():
            dataset, query_name = query.split(' - ')
            min_interval = round(mean_value - margin_of_error, 2)
            max_interval = round(mean_value + margin_of_error, 2)
            intervallo_di_confidenza = f"({min_interval}, {max_interval})"  # Corretto formato
            writer.writerow([dataset, query_name, tempo_medio_successive, round(mean_value, 2), intervallo_di_confidenza])
        print("I tempi di risposta medi sono stati scritti nei file 'tempi_di_risposta_prima_esecuzione.csv' e 'tempi_di_risposta_media_30.csv'.")

if __name__ == "__main__":
    main()
