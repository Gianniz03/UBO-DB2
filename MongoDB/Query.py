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
