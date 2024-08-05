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
                    # Attempt to convert the string to a JSON object
                    doc[key] = json.loads(value)
                except json.JSONDecodeError:
                    # If it's not a valid JSON string, keep the original value
                    pass
    return doc

def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    mean_value = np.mean(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return mean_value, margin_of_error

def measure_query_performance(db, query_number, percentage, iterations=30):
    subsequent_times = []
    
    for _ in range(iterations):
        start_time = time.time()
        if query_number == 1:
            query1(db, percentage)
        elif query_number == 2:
            query2(db, percentage)
        elif query_number == 3:
            query3(db, percentage)
        elif query_number == 4:
            query4(db, percentage)
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Milliseconds
        subsequent_times.append(execution_time)
    
    mean, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_time_subsequent = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_time_subsequent, mean, margin_of_error

def query1(db, percentage):
    company_name = 'Sutton-Nolan'
    companies = f'Companies {percentage}'  # Modify with the correct collection name
    query = db[companies].find_one({"name": company_name})  # Modify with the correct query

    return company_name, query

def query2(db, percentage):
    company_id = 200
    companies = f'Companies {percentage}'
    administrators = f'Administrators {percentage}'
    company = db[companies].find_one({"id": company_id})
    
    if not company:
        return company_id, False, []

    company = convert_string_fields_to_json(company)
    
    # Retrieve administrator details
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators].find({"id": {"$in": admin_ids}}))

    # Add details to the company
    company['administrators_details'] = administrators_details

    return company_id, company, administrators_details

# Alternative Version
""" def query2(db, percentage):
    company_id = 4
    companies = f'Companies {percentage}'
    administrators = f'Administrators {percentage}'
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

def query3(db, percentage):
    companies_collection = f'Companies {percentage}'
    administrators_collection = f'Administrators {percentage}'
    ubos_collection = f'UBO {percentage}'
    company_id = 4550  # Change this ID according to your actual data

    company = db[companies_collection].find_one({"id": company_id})

    if not company:
        return company_id, False, [], []

    company = convert_string_fields_to_json(company)
    
    # Retrieve administrator details
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Retrieve UBO details
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Add details to the company
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details

    return company_id, company, administrators_details, ubos_details

from datetime import datetime

def query4(db, percentage):
    companies_collection = f'Companies {percentage}'
    administrators_collection = f'Administrators {percentage}'
    ubos_collection = f'UBO {percentage}'
    transactions_collection = f'Transactions {percentage}'
    
    company_id = 1  # Change this ID according to your actual data

    # Retrieve the company
    company = db[companies_collection].find_one({"id": company_id})
    
    if not company:
        return company_id, False, [], [], 0
    
    company = convert_string_fields_to_json(company)
    
    # Retrieve administrator details
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Retrieve UBO details
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Retrieve the sum of transactions
    transaction_ids = company.get('transactions', [])
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    
    transaction_summary = db[transactions_collection].aggregate([
        {"$match": {"id": {"$in": transaction_ids}, "date": {"$gte": start_date, "$lte": end_date}}},
        {"$group": {"_id": None, "total_amount": {"$sum": "$amount"}}}
    ])

    # Extract the total amount of transactions
    total_amount = 0
    for record in transaction_summary:
        total_amount = record.get('total_amount', 0)
    
    # Add details to the company
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details
    company['transactions_summary'] = total_amount
    
    return company_id, company, administrators_details, ubos_details, total_amount

def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['UBO']  # Change 'DatabaseName' to your database name
    
    percentages = ['100%', '75%', '50%', '25%']  # Dataset percentages

    first_execution_response_times = {}
    average_response_times = {}

    for percentage in percentages:
        print(f"\nAnalysis for percentage: {percentage}\n")

        # Query 1
        start_time = time.time()
        company_name, query = query1(db, percentage)
        if query:
            query_json = convert_string_fields_to_json(query)
            json_result = json.dumps(query_json, indent=4, default=json_serializer)
            print(f"Company name with the specified name: \n{json_result}\n")
        else:
            json_result = json.dumps({"error": "No results found"})
            print(f"No company found with the specified name: {company_name}\n")

        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 1): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 1"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 1, percentage)
        print(f"Average time of 30 subsequent executions (Query 1): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 1"] = (average_time_subsequent, mean, margin_of_error)

        # Query 2: Retrieve company details and its administrators
        start_time = time.time()
        company_id, company, administrators = query2(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            #administrators_json = json.dumps(administrators, indent=4, default=json_serializer)
            print(f"Details of the company with ID {company_id} and administrators: \n{company_json}\n")
            #print(f"Administrator Details of the company with ID {company_id}: \n{administrators_json}\n")
        else:
            print(f"No company found with ID {company_id}\n")

        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first execution - Query 2): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 2"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 2, percentage)
        print(f"Average time of 30 subsequent executions (Query 2): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 2"] = (average_time_subsequent, mean, margin_of_error)

        # Query 3: Retrieve company details, its administrators, and UBOs with more than 25%
        start_time = time.time()
        company_id, company, administrators, ubos = query3(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Details of the company with ID {company_id}, administrators, and UBOs: \n{company_json}\n")
        else:
            print(f"No company found with ID {company_id}\n")
        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        first_execution_response_times[f"{percentage} - Query 3"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 3, percentage)
        print(f"Average time of 30 subsequent executions (Query 3): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 3"] = (average_time_subsequent, mean, margin_of_error)

        # Query 4: Retrieve company details, its administrators, UBOs with more than 25%, and the sum of transactions within a period
        start_time = time.time()
        company_id, company, administrators_details, ubos_details, total_amount = query4(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Details of the company with ID {company_id}, administrators, UBOs, and transactions: \n{company_json}\n")
        else:
            print(f"No company found with ID {company_id}\n")
        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        first_execution_response_times[f"{percentage} - Query 4"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 4, percentage)
        print(f"Average time of 30 subsequent executions (Query 4): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 4"] = (average_time_subsequent, mean, margin_of_error)

        print("-" * 70)  # Separator between different percentages

    # Write the average response times of the first execution to a CSV file
    with open('MongoDB/ResponseTimes/mongodb_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds'])
        for query, first_execution_time in first_execution_response_times.items():
            dataset, query = query.split(' - ')
            writer.writerow([dataset, query, first_execution_time])

    with open('MongoDB/ResponseTimes/mongodb_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds', 'Average', 'Confidence Interval (Min, Max)'])
        for query, (average_time_subsequent, mean_value, margin_of_error) in average_response_times.items():
            dataset, query_name = query.split(' - ')
            min_interval = round(mean_value - margin_of_error, 2)
            max_interval = round(mean_value + margin_of_error, 2)
            confidence_interval = f"({min_interval}, {max_interval})"  # Correct format
            writer.writerow([dataset, query_name, average_time_subsequent, round(mean_value, 2), confidence_interval])
        print("Average response times have been written to 'mongodb_times_of_response_first_execution.csv' and 'mongodb_response_times_average_30.csv'.")

if __name__ == "__main__":
    main()
