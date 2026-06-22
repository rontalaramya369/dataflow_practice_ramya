import requests
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1


project_id = "project-8a611ce5-dc75-4904-b12"

topic_id = "crypto-topic"


publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_id)


url = "https://restcountries.com/v3.1/all"


while True:

    response = requests.get(url)

    countries = response.json()

    for country in countries:

        message = {

            "name": country.get("name", {}).get("common"),

            "official_name": country.get("name", {}).get("official"),

            "capital": country.get("capital", [None])[0]
            if country.get("capital")
            else None,

            "region": country.get("region"),

            "subregion": country.get("subregion"),

            "population": country.get("population"),

            "area": country.get("area"),

            "independent": country.get("independent"),

            "cca2": country.get("cca2"),

            "cca3": country.get("cca3"),

            "status": country.get("status"),

            "un_member": country.get("unMember"),

            "ingestion_time": datetime.utcnow().isoformat()

        }

        publisher.publish(
            topic_path,
            json.dumps(message).encode("utf-8")
        )

        print(message)

    print("Sleeping for 60 seconds...")

    time.sleep(60)