from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Use your MongoDB URI if hosted remotely
db = client['UBO']  # Replace with your database name

# Collections
administrators_collection = db['Administrators 100%']
shareholders_collection = db['Shareholders 100%']
ubo_collection = db['UBO 100%']
transactions_collection = db['Transactions 100%']
kyc_aml_checks_collection = db['KYC_AML_Checks 100%']
companies_collection = db['Companies 100%']

# Query 1: Retrieve Basic Details of a Specific Company
def query_company_basic_details(company_id):
    return companies_collection.aggregate([
        # Match the specific company
        {
            '$match': {
                'id': company_id
            }
        },
        # Join with administrators
        {
            '$lookup': {
                'from': 'Administrators 100%',
                'localField': 'administrators',
                'foreignField': 'id',
                'as': 'administrator_details'
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'address': 1,
                'legal_form': 1,
                'administrator_details': 1
            }
        }
    ])

# Query 2: Retrieve Transactions for a Specific Company
def query_company_transactions(company_id):
    return companies_collection.aggregate([
        # Match the specific company
        {
            '$match': {
                'id': company_id
            }
        },
        # Join with transactions
        {
            '$lookup': {
                'from': 'Transactions 100%',
                'localField': 'transactions',
                'foreignField': 'id',
                'as': 'transaction_details'
            }
        },
        # Unwind the transaction details
        {
            '$unwind': '$transaction_details'
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'transaction_details.type': 1,
                'transaction_details.amount': 1,
                'transaction_details.date': 1
            }
        }
    ])

# Query 3: Find Companies with UBOs and Failed KYC/AML Checks
def query_companies_with_failed_kyc_aml():
    return companies_collection.aggregate([
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Unwind the UBO details
        {
            '$unwind': '$ubo_details'
        },
        # Join with KYC/AML checks
        {
            '$lookup': {
                'from': 'KYC_AML_Checks 100%',
                'localField': 'ubo_details.id',
                'foreignField': 'ubo_id',
                'as': 'kyc_aml_check_details'
            }
        },
        # Unwind the KYC/AML check details
        {
            '$unwind': '$kyc_aml_check_details'
        },
        # Match failed checks
        {
            '$match': {
                'kyc_aml_check_details.result': 'Failed'
            }
        },
        # Project the necessary fields
        {
            '$project': {
                'name': 1,
                'ubo_details.name': 1,
                'ubo_details.ownership_percentage': 1,
                'kyc_aml_check_details.type': 1,
                'kyc_aml_check_details.date': 1
            }
        }
    ])

<<<<<<< Updated upstream
# Query 4: Analyze Financial Performance of Companies with High UBO Ownership
def query_financial_analysis_for_high_ownership_ubos():
    return companies_collection.aggregate([
        # Join with UBOs
        {
            '$lookup': {
                'from': 'UBO 100%',
                'localField': 'ubo',
                'foreignField': 'id',
                'as': 'ubo_details'
            }
        },
        # Unwind the UBO details
        {
            '$unwind': '$ubo_details'
        },
        # Match UBOs with ownership above 25%
        {
            '$match': {
                'ubo_details.ownership_percentage': {'$gt': 25}
            }
        },
        # Join with financial data
        {
            '$project': {
                'name': 1,
                'ubo_details.name': 1,
                'ubo_details.ownership_percentage': 1,
                'financial_data': 1
            }
        },
        # Unwind financial data for further analysis
        {
            '$unwind': '$financial_data'
        },
        # Aggregate financial data
        {
            '$group': {
                '_id': '$name',
                'total_revenue': {'$sum': '$financial_data.revenue'},
                'total_profit': {'$sum': '$financial_data.profit'},
                'ubos': {'$push': {
                    'ubo_name': '$ubo_details.name',
                    'ubo_ownership_percentage': '$ubo_details.ownership_percentage'
                }}
            }
        },
        # Sort by total revenue descending
        {
            '$sort': {'total_revenue': -1}
        }
    ])
=======
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
>>>>>>> Stashed changes

# Execute queries and print results
if __name__ == "__main__":
    company_id = 1  # Replace with the specific company ID you want to analyze

    print("Company Basic Details:")
    for company in query_company_basic_details(company_id):
        print(company)
    
    print("\nCompany Transactions:")
    for transaction in query_company_transactions(company_id):
        print(transaction)

    print("\nCompanies with Failed KYC/AML Checks:")
    for high_risk_company in query_companies_with_failed_kyc_aml():
        print(high_risk_company)

    print("\nFinancial Analysis for High UBO Ownership Companies:")
    for analysis in query_financial_analysis_for_high_ownership_ubos():
        print(analysis)

# Close the MongoDB connection
client.close()
