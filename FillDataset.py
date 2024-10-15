import subprocess

def execute_scripts():
    try:

        # Esegui il terzo script
        print(f"Running Dataset/GenerateDataset.py script")
        subprocess.run(['python', 'Dataset/GenerateDataset.py'], check=True)

        # Esegui il primo script
        print(f"Running BaseX/Dataset.py script")
        subprocess.run(['python', 'BaseX/Dataset.py'], check=True)

        # Esegui il secondo script
        print(f"Running Neo4j/Dataset.py script")
        subprocess.run(['python', 'Neo4j/Dataset.py'], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing a script: {e}")

if __name__ == '__main__':
    execute_scripts()
