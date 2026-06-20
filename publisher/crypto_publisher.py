import requests
import json
import time
from datetime import datetime

from google.cloud import pubsub_v1


PROJECT_ID = "project-8a611ce5-dc75-4904-b12"

TOPIC_ID = "crypto-topic"


publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


while True:

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"

    response = requests.get(url)

    data = response.json()

    message = {

        "coin":"bitcoin",

        "price_usd":data["bitcoin"]["usd"],

        "last_updated":datetime.utcnow().isoformat()

    }


    publisher.publish(

        topic_path,

        json.dumps(message).encode("utf-8")

    )


    print(message)

    time.sleep(5)