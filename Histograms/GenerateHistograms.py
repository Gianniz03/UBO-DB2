import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np


# Specifica i percorsi completi dei file CSV per MongoDB e Neo4j
mongo_csv_paths = [
    "MongoDB/ResponseTimes/mongodb_times_of_response_first_execution.csv",
    "MongoDB/ResponseTimes/mongodb_response_times_average_30.csv",
]

neo4j_csv_paths = [
    "Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv",
    "Neo4j/ResponseTimes/neo4j_response_times_average_30.csv",
]


# Leggi i dati dai file CSV
data_mongo_prima_esecuzione = pd.read_csv(mongo_csv_paths[0], sep=',', dtype={'Intervallo di Confidenza (Min, Max)': str})
data_mongo_media_30 = pd.read_csv(mongo_csv_paths[1], sep=',', dtype={'Intervallo di Confidenza (Min, Max)': str})

data_neo4j_prima_esecuzione = pd.read_csv(neo4j_csv_paths[0], sep=',', dtype={'Intervallo di Confidenza (Min, Max)': str})
data_neo4j_media_30 = pd.read_csv(neo4j_csv_paths[1], sep=',', dtype={'Intervallo di Confidenza (Min, Max)': str})

# Lista delle dimensioni del dataset
dataset_sizes = ['100%', '75%', '50%', '25%']

# Lista delle query
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4']

# Definisce i colori per MongoDB e Neo4j
color_mongo = 'coral'
color_neo4j = 'purple'

# Funzione per estrarre i valori minimi e massimi dall'intervallo di confidenza
def extract_confidence_values(confidence_interval_str):
    if pd.isna(confidence_interval_str):
        return np.nan, np.nan
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Per ogni query, crea gli istogrammi
for query in queries:
    # Filtra i dati per la query corrente
    data_mongo_query_prima_esecuzione = data_mongo_prima_esecuzione[data_mongo_prima_esecuzione['Query'] == query]
    data_mongo_query_media_30 = data_mongo_media_30[data_mongo_media_30['Query'] == query]

    data_neo4j_query_prima_esecuzione = data_neo4j_prima_esecuzione[data_neo4j_prima_esecuzione['Query'] == query]
    data_neo4j_query_media_30 = data_neo4j_media_30[data_neo4j_media_30['Query'] == query]

    # Crea il primo istogramma con i tempi della prima esecuzione
    plt.figure(figsize=(12, 7))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    values_mongo_prima_esecuzione = [data_mongo_query_prima_esecuzione[data_mongo_query_prima_esecuzione['Dataset'] == size]['Millisecondi'].values[0] for size in dataset_sizes]
    values_neo4j_prima_esecuzione = [data_neo4j_query_prima_esecuzione[data_neo4j_query_prima_esecuzione['Dataset'] == size]['Millisecondi'].values[0] for size in dataset_sizes]

    plt.bar(index - bar_width/2, values_mongo_prima_esecuzione, bar_width, label='MongoDB', color=color_mongo)
    plt.bar(index + bar_width/2, values_neo4j_prima_esecuzione, bar_width, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione (ms)')
    plt.title(f'Istogramma - Tempo della Prima Esecuzione per {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    # Salva il grafico come file PNG nella cartella corrente
    filename = f'Histograms/Histogram_Time_Before_Execution_{query}.png'
    plt.savefig(filename)

    # Mostra il grafico
    plt.show()

    # Rimuovi il grafico dalla memoria
    plt.close()

    # Crea il secondo istogramma con le medie dei tempi e intervalli di confidenza
    plt.figure(figsize=(12, 7))
    values_mongo_media_30 = [data_mongo_query_media_30[data_mongo_query_media_30['Dataset'] == size]['Media'].values[0] for size in dataset_sizes]
    values_neo4j_media_30 = [data_neo4j_query_media_30[data_neo4j_query_media_30['Dataset'] == size]['Media'].values[0] for size in dataset_sizes]

    conf_intervals_mongo = [extract_confidence_values(data_mongo_query_media_30[data_mongo_query_media_30['Dataset'] == size]['Intervallo di Confidenza (Min, Max)'].values[0]) for size in dataset_sizes]
    conf_intervals_neo4j = [extract_confidence_values(data_neo4j_query_media_30[data_neo4j_query_media_30['Dataset'] == size]['Intervallo di Confidenza (Min, Max)'].values[0]) for size in dataset_sizes]

    conf_mongo_min = [conf[0] for conf in conf_intervals_mongo]
    conf_mongo_max = [conf[1] for conf in conf_intervals_mongo]
    conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
    conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]

    mongo_yerr = [np.array([values_mongo_media_30[i] - conf_mongo_min[i], conf_mongo_max[i] - values_mongo_media_30[i]]) for i in range(len(dataset_sizes))]
    neo4j_yerr = [np.array([values_neo4j_media_30[i] - conf_neo4j_min[i], conf_neo4j_max[i] - values_neo4j_media_30[i]]) for i in range(len(dataset_sizes))]

    plt.bar(index - bar_width/2, values_mongo_media_30, bar_width, yerr=np.array(mongo_yerr).T, capsize=5, label='MongoDB', color=color_mongo)
    plt.bar(index + bar_width/2, values_neo4j_media_30, bar_width, yerr=np.array(neo4j_yerr).T, capsize=5, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione medio (ms)')
    plt.title(f'Istogramma - Tempo di Esecuzione Medio per {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    # Salva il grafico come file PNG nella cartella corrente
    filename = f'Histograms/Histogram_Average_Execution_Time_{query}.png'
    plt.savefig(filename)

    # Mostra il grafico
    plt.show()

    # Rimuovi il grafico dalla memoria
    plt.close()
