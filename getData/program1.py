import requests
import json
from datetime import datetime
from hdfs import InsecureClient  # Import InsecureClient for HDFS

current_date = datetime.now().strftime("%Y-%m-%d")
HDFS_file_path = f"/user/datalake/{current_date}/meteobrut/data15jours.json"
HDFS_file_path1 = f"/user/datalake/{current_date}/meteobrut/data.json"


def ingest_meteo_data():
    response = requests.get('https://api.openweathermap.org/data/2.5/forecast?q=limoges,FR1&appid=eea045ed57d81cb0b2ad92319810b8c6')
    if response.status_code == 200:
        data = response.json()

        # écrire dans le fichier JSON
        with open(HDFS_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=2)

        print(f"les données ont été sauvegardées dans {HDFS_file_path}")
    else:
        print(f"Échec de la requête avec le code : {response.status_code}")
        
 
def ingest_meteo_data1():
    response = requests.get('https://api.openweathermap.org/data/2.5/weather?q=limoges,FR&appid=a09da6bb2e938eeb1997fff56f6d14c2')
    if response.status_code == 200:
        data = response.json()

        # écrire dans le fichier JSON
        with open(HDFS_file_path1, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=2)

        print(f"les données ont été sauvegardées dans {HDFS_file_path1}")
        return data  # Return the data
    else:
        print(f"Échec de la requête avec le code : {response.status_code}")
        return None  # Return None in case of failure


def save_to_hdfs(data):
    client = InsecureClient("http://localhost:9870", user="hadoop")

    with client.write(HDFS_file_path, overwrite=True) as writer:  # Fix overwrite parameter
        json.dump(data, writer)

if __name__ == "__main__":
    meteo_data = ingest_meteo_data1()
    if meteo_data:
        save_to_hdfs(meteo_data)
        print("Data saved to HDFS successfully")
    else:
        print("Failed to fetch data")
