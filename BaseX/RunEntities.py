import os
import subprocess

def run_python_files_in_directory(directory):
    # Elenco tutti i file nella directory
    files = os.listdir(directory)
    
    # Filtro solo i file con estensione .py
    python_files = [f for f in files if f.endswith('.py')]
    
    # Esecuzione di ogni file Python
    for file in python_files:
        file_path = os.path.join(directory, file)
        print(f"Running file: {file}")
        try:
            # Esegui il file Python usando subprocess
            subprocess.run(['python', file_path], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error during execution of {file}: {e}")

# Esempio di utilizzo: esegui i file nella directory corrente
run_python_files_in_directory('MongoDB/Entities')