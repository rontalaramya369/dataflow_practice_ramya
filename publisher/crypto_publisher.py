import requests
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1


project_id = "project-8a611ce5-dc75-4904-b12"

topic_id = "crypto-topic"


publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    project_id,
    topic_id
)


url = "https://jsonplaceholder.typicode.com/users"


while True:

    response = requests.get(url)

    users = response.json()

    for user in users:

        message = {

            "id": user.get("id"),

            "name": user.get("name"),

            "username": user.get("username"),

            "email": user.get("email"),

            "street": user.get("address", {}).get("street"),

            "suite": user.get("address", {}).get("suite"),

            "city": user.get("address", {}).get("city"),

            "zipcode": user.get("address", {}).get("zipcode"),

            "latitude": user.get("address", {}).get("geo", {}).get("lat"),

            "longitude": user.get("address", {}).get("geo", {}).get("lng"),

            "phone": user.get("phone"),

            "website": user.get("website"),

            "company_name": user.get("company", {}).get("name"),

            "company_phrase": user.get("company", {}).get("catchPhrase"),

            "company_bs": user.get("company", {}).get("bs"),

            "ingestion_time": datetime.utcnow().isoformat()

        }

        publisher.publish(
            topic_path,
            json.dumps(message).encode("utf-8")
        )

        print(message)

    print("Sleeping for 60 seconds")

    time.sleep(60)