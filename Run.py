import subprocess

def execute_scripts():
    try:
        # Esegui il primo script
        print(f"Script MongoDB/Query.py in esecuzione")
        subprocess.run(['python', 'MongoDB/Query.py'], check=True)

        # Esegui il secondo script
        print(f"Script Neo4j/Query.py in esecuzione")
        subprocess.run(['python', 'Neo4j/Query.py'], check=True)

        # Esegui il terzo script
        print(f"Script Histograms/GenerateHistograms.py in esecuzione")
        subprocess.run(['python', 'Histograms/GenerateHistograms.py'], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Errore durante l'esecuzione di uno script: {e}")

if __name__ == '__main__':
    execute_scripts()
