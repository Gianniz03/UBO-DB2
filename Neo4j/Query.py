from py2neo import Graph
import time
import csv
import scipy.stats as stats
import numpy as np
import json

graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    average_value = np.average(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return average_value, margin_of_error

def measure_query_performance(graph, query_func, percentage, iterations=30):
    subsequent_times = []
    
    for _ in range(iterations):
        start_time = time.time()
        query_func(graph)
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Milliseconds
        subsequent_times.append(execution_time)
    
    average, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_subsequent_time = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_subsequent_time, average, margin_of_error

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
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    RETURN c, collect(a) as administrators
    """
    result = graph.run(query).data()
    return result

def query3(graph):
    company_id = 5312
    query = f"""
    MATCH (c:Companies {{id: {company_id}}})
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
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
    OPTIONAL MATCH (c)-[:COMPANY_HAS_ADMINISTRATOR]->(a:Administrators)
    OPTIONAL MATCH (c)-[:COMPANY_HAS_UBO]->(u:Ubo)
    WHERE u.ownership_percentage > 25
    OPTIONAL MATCH (c)-[:COMPANY_HAS_TRANSACTION]->(t:Transactions)
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
    
    response_times_first_execution = {}
    average_response_times = {}

    for percentage, graph in graphs.items():
        print(f"\nAnalysis for percentage: {percentage}\n")

        # Query 1
        start_time = time.time()
        query_result = query1(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Query 1 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified name\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 1): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 1"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query1, percentage)
        print(f"Average time of 30 subsequent executions (Query 1): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 1): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 1"] = (average_subsequent_time, average, margin_of_error)

        # Query 2
        start_time = time.time()
        query_result = query2(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Query 2 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 2): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 2"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query2, percentage)
        print(f"Average time of 30 subsequent executions (Query 2): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 2): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 2"] = (average_subsequent_time, average, margin_of_error)

        # Query 3
        start_time = time.time()
        query_result = query3(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Query 3 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 3): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 3"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query3, percentage)
        print(f"Average time of 30 subsequent executions (Query 3): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 3): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 3"] = (average_subsequent_time, average, margin_of_error)

        # Query 4
        start_time = time.time()
        query_result = query4(graph)
        if query_result:
            json_result = json.dumps(query_result, indent=4, default=str)
            print(f"Query 4 Result: \n{json_result}\n")
        else:
            print(f"No company found with the specified ID\n")

        end_time = time.time()
        time_first_execution = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 4): {time_first_execution} ms")
        response_times_first_execution[f"{percentage} - Query 4"] = time_first_execution

        average_subsequent_time, average, margin_of_error = measure_query_performance(graph, query4, percentage)
        print(f"Average time of 30 subsequent executions (Query 4): {average_subsequent_time} ms")
        print(f"Confidence Interval (Query 4): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 4"] = (average_subsequent_time, average, margin_of_error)

  
    with open('Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds"])
        for key, value in response_times_first_execution.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, value])

    with open('Neo4j/ResponseTimes/neo4j_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Dataset", "Query", "Milliseconds", "Average", "Confidence Interval (Min, Max)"])
        for key, (average_time, average, margin_of_error) in average_response_times.items():
            percentage, query = key.split(' - ')
            writer.writerow([percentage, query, average_time, round(average, 2), f"[{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}]"])

if __name__ == "__main__":
    main()
