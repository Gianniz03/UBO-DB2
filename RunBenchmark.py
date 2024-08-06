import subprocess

def execute_scripts():
    try:
        # Esegui il primo script
        print(f"Running MongoDB/Query.py script")
        subprocess.run(['python', 'MongoDB/Query.py'], check=True)

        # Esegui il secondo script
        print(f"Running Neo4j/Query.py script")
        subprocess.run(['python', 'Neo4j/Query.py'], check=True)

        # Esegui il terzo script
        print(f"Running Histograms/GenerateHistograms.py script")
        subprocess.run(['python', 'Histograms/GenerateHistograms.py'], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")

if __name__ == '__main__':
    execute_scripts()
