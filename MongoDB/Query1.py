from pymongo import MongoClient

# Connessione a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['UBO']
companies_collection = db['Companies 100%']
administrators_collection = db['Administrators 100%']

def query_company_basic_details(company_id):
    pipeline = [
        # Fase 1: Filtra l'azienda per ID
        {
            '$match': {
                'id': company_id
            }
        },
        # Fase 2: Lookup dei dettagli degli amministratori
        {
            '$lookup': {
                'from': 'administrators',
                'localField': 'administrators',
                'foreignField': 'id',
                'as': 'administrator_details'
            }
        },
        # Fase 3: Unwind per trasformare la lista in documenti singoli
        {
            '$unwind': {
                'path': '$administrator_details',
                'preserveNullAndEmptyArrays': True
            }
        },
        # Fase 4: Proietta i campi desiderati
        {
            '$project': {
                'name': 1,
                'address': 1,
                'legal_form': 1,
                'administrator_details.name': 1,
                'administrator_details.address': 1,
                'administrator_details.birthdate': 1,
                'administrator_details.nationality': 1
            }
        }
    ]
    
    try:
        results = companies_collection.aggregate(pipeline)
        print(f"Querying company with ID: {company_id}")
        for result in results:
            print(result)
        return results
    except Exception as e:
        print(f"Error querying company details: {e}")

# Esegui la query per un'azienda con un ID specifico
company_id = 1  # Cambia questo ID per testare altri casi
query_company_basic_details(company_id)
