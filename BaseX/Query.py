from BaseXClient import BaseXClient
from datetime import datetime
import json
import time
import csv
import scipy.stats as stats
import numpy as np

# Funzione per serializzare oggetti datetime in formato stringa per JSON
def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Non-serializable type")

# Funzione per calcolare l'intervallo di confidenza per una media
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)  # Numero di osservazioni
    mean_value = np.mean(data)  # Media dei dati
    stderr = stats.sem(data)  # Errore standard della media
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)  # Margine d'errore
    return mean_value, margin_of_error

# Funzione per misurare la performance delle query
def measure_query_performance(session, query_function, percentage, iterations=30):
    subsequent_times = []  # Lista per memorizzare i tempi di esecuzione
    
    for _ in range(iterations):
        start_time = time.time()  # Inizio misurazione tempo
        query_function(session, percentage)
        end_time = time.time()  # Fine misurazione tempo
        execution_time = (end_time - start_time) * 1000  # Tempo di esecuzione in millisecondi
        subsequent_times.append(execution_time)
    
    # Calcola il tempo medio e l'intervallo di confidenza
    mean, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_time_subsequent = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_time_subsequent, mean, margin_of_error

# Query 1: Recupera un'azienda per nome
def query1(session, percentage):
    company_name = 'Special Company'
    query = f'''
        for $c in collection("UBO_{percentage}")//company[name='{company_name}']
        return $c
    '''
    
    query_obj = session.query(query)
    result = query_obj.execute()
    
    return company_name, result

# Query 2: Recupera i dettagli di un'azienda e dei suoi amministratori
def query2(session, percentage):
    company_id = 999999999
    query = f'''
        let $company := collection("UBO_{percentage}")//company[id={company_id}]
        let $admins := collection("UBO_{percentage}")//administrator[id=($company/administrators/id)]
        return <result>{{$company, $admins}}</result>
    '''
    result = session.query(query).execute()
    return company_id, result

# Query 3: Recupera dettagli azienda, amministratori e UBO con più del 25%
def query3(session, percentage):
    company_id = 999999999
    query = f'''
        let $company := collection("UBO_{percentage}")//company[id={company_id}]
        let $admins := collection("UBO_{percentage}")//administrator[id=($company/administrators/id)]
        let $ubos := collection("UBO_{percentage}")//ubo[id=($company/ubo/id) and ownership_percentage > 25]
        return <result>{{$company, $admins, $ubos}}</result>
    '''
    result = session.query(query).execute()
    return company_id, result

# Query 4: Recupera azienda, amministratori, UBO e transazioni
def query4(session, percentage):
    company_id = 999999999
    query = f'''
        let $company := collection("UBO_{percentage}")//company[id={company_id}]
        let $admins := collection("UBO_{percentage}")//administrator[id=($company/administrators/id)]
        let $ubos := collection("UBO_{percentage}")//ubo[id=($company/ubo/id) and ownership_percentage > 25]
        let $transactions := collection("UBO_{percentage}")//transaction[id=($company/transactions/id) and date ge '2019-01-01' and date le '2024-12-31']
        let $total := sum($transactions/amount)
        return <result>{{$company, $admins, $ubos, <total-amount>{{ $total }}</total-amount>}}</result>
    '''
    result = session.query(query).execute()
    return company_id, result

# Query 5: Recupera azienda, amministratori, UBO, transazioni e KYC/AML
def query5(session, percentage):
    company_id = 999999999
    query = f'''
        let $company := collection("UBO_{percentage}")//company[id={company_id}]
        let $admins := collection("UBO_{percentage}")//administrator[id=($company/administrators/id)]
        let $ubos := collection("UBO_{percentage}")//ubo[id=($company/ubo/id) and ownership_percentage > 25]
        let $transactions := collection("UBO_{percentage}")//transaction[id=($company/transactions/id) and currency='USD' and date ge '1950-03-10']
        let $kyc := collection("UBO_{percentage}")//check[id=($company/kyc_aml_checks/id) and date ge '1950-03-10']
        let $total := sum($transactions/amount)
        return <result>{{$company, $admins, $ubos, $kyc, <total-amount>{{ $total }}</total-amount>}}</result>
    '''
    result = session.query(query).execute()
    return company_id, result


# Funzione principale per eseguire le query e misurare le performance
def main():
    session = BaseXClient.Session('localhost', 1984, 'admin', 'admin')

    percentages = ['100', '75', '50', '25']  # Percentuali del dataset
    first_execution_response_times = {}
    average_response_times = {}

    for percentage in percentages:
        print(f"\nAnalysis by percentage: {percentage}%\n")

        # Query 1
        start_time = time.time()
        company_name, query = query1(session, percentage)
        end_time = time.time()
        if query:
            print(f"Company name with the specified name: \n{query}\n")
        else:
            print(f"No companies found with the specified name: {company_name}\n")

        first_execution_time = round((end_time - start_time) * 1000, 2)
        first_execution_response_times[f"{percentage} - Query 1"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(session, query1, percentage)
        average_response_times[f"{percentage} - Query 1"] = (average_time_subsequent, mean, margin_of_error)

        # Ripetere il processo per le altre query (query2, query3, query4, query5)
        # ...

    # Salva i risultati
    with open('basex_response_times_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds'])
        for query, first_execution_time in first_execution_response_times.items():
            dataset, query = query.split(' - ')
            writer.writerow([dataset, query, first_execution_time])

    with open('basex_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds', 'Average', 'Confidence Interval (Min, Max)'])
        for query, (average_time_subsequent, mean_value, margin_of_error) in average_response_times.items():
            dataset, query_name = query.split(' - ')
            min_interval = round(mean_value - margin_of_error, 2)
            max_interval = round(mean_value + margin_of_error, 2)
            writer.writerow([dataset, query_name, average_time_subsequent, mean_value, f"{min_interval}, {max_interval}"])

    session.close()

if __name__ == "__main__":
    main()
