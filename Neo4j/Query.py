from py2neo import Graph
import time
import csv
import scipy.stats as stats
import numpy as np
import json

# Connessione ai database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    mean_value = np.mean(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return mean_value, margin_of_error

def measure_query_performance(graph, query_func, percentuale, iterations=30):
    tempi_successivi = []
    
    for _ in range(iterations):
        start_time = time.time()
        query_func(graph)
        end_time = time.time()
        tempo_esecuzione = (end_time - start_time) * 1000  # Millisecondi
        tempi_successivi.append(tempo_esecuzione)
    
    mean, margin_of_error = calculate_confidence_interval(tempi_successivi)
    tempo_medio_successive = round(sum(tempi_successivi) / len(tempi_successivi), 2)
    
    return tempo_medio_successive, mean, margin_of_error

def query1(graph):
    company_name = 'Pittman Ltd'
    query = f"""
    MATCH (c:Companies {{name: '{company_name}'}})
    RETURN c
    """
    result = graph.run(query).data()
    return result

def query2(graph):
    company_id = 5312
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:AZIENDA_HA_AMMINISTRATORE]->(a:Administrators)
    RETURN c, collect(a) as administrators
    """
    result = graph.run(query).data()
    return result

def query3(graph):
    company_id = 5312
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:AZIENDA_HA_AMMINISTRATORE]->(a:Administrators)
    OPTIONAL MATCH (c)-[:AZIENDA_HA_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    RETURN c, collect(a) as administrators, collect(u) as ubos
    """
    result = graph.run(query).data()
    return result

def query4(graph):
    company_id = 5312
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:AZIENDA_HA_AMMINISTRATORE]->(a:Administrators)
    OPTIONAL MATCH (c)-[:AZIENDA_HA_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    OPTIONAL MATCH (c)-[:AZIENDA_HA_TRANSAZIONE]->(t:Transactios)
    WHERE t.date >= date('{start_date}') AND t.date <= date('{end_date}')
    RETURN c, collect(a) as administrators, collect(u) as ubos, sum(t.amount) as total_amount
    """
    result = graph.run(query).data()
    return result


def main():
    graphs = {
        '100%': graph100,
        '75%': graph75,
        '50%': graph50,
        '25%': graph25
    }
    
    tempi_di_risposta_prima_esecuzione = {}
    tempi_di_risposta_media = {}

    for percentuale, graph in graphs.items():
        print(f"\nAnalisi per la percentuale: {percentuale}\n")

        # Query 1
        start_time = time.time()
        query_result = query1(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Risultato Query 1: \n{json_result}\n")
        else:
            print(f"Nessuna azienda trovata con il nome specificato\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 1): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 1"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(graph, query1, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 1): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 1"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 2
        start_time = time.time()
        query_result = query2(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Risultato Query 2: \n{json_result}\n")
        else:
            print(f"Nessuna azienda trovata con ID specificato\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 2): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 2"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(graph, query2, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 2): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 2"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 3
        start_time = time.time()
        query_result = query3(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Risultato Query 3: \n{json_result}\n")
        else:
            print(f"Nessuna azienda trovata con ID specificato\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 3): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 3"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(graph, query3, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 3): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 3"] = (tempo_medio_successive, mean, margin_of_error)

        # Query 4
        start_time = time.time()
        query_result = query4(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Risultato Query 4: \n{json_result}\n")
        else:
            print(f"Nessuna azienda trovata con ID specificato\n")

        end_time = time.time()
        tempo_prima_esecuzione = round((end_time - start_time) * 1000, 2)
        print(f"Tempo di risposta (prima esecuzione - Query 4): {tempo_prima_esecuzione} ms")
        tempi_di_risposta_prima_esecuzione[f"{percentuale} - Query 4"] = tempo_prima_esecuzione

        tempo_medio_successive, mean, margin_of_error = measure_query_performance(graph, query4, percentuale)
        print(f"Tempo medio di 30 esecuzioni successive (Query 4): {tempo_medio_successive} ms")
        print(f"Intervallo di Confidenza (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        tempi_di_risposta_media[f"{percentuale} - Query 4"] = (tempo_medio_successive, mean, margin_of_error)

    # Scrivi i risultati nei file CSV
    with open('Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Millisecondi"])
        for key, value in tempi_di_risposta_prima_esecuzione.items():
            percentuale, query = key.split(' - ')
            writer.writerow([percentuale, query, value])

    with open('Neo4j/ResponseTimes/neo4j_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Millisecondi", "Media", "Intervallo di Confidenza (Min, Max)"])
        for key, (tempo_medio, mean, margin_of_error) in tempi_di_risposta_media.items():
            percentuale, query = key.split(' - ')
            writer.writerow([percentuale, query, tempo_medio, round(mean, 2), f"[{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}]"])

if __name__ == "__main__":
    main()
