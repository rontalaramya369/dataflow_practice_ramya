import requests
import time
from datetime import datetime

while True:

    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    )

    data = response.json()

    print("API Response:", data)

    if "bitcoin" in data:

        record = {
            "coin": "bitcoin",
            "price_usd": data["bitcoin"]["usd"],
            "last_updated": datetime.utcnow().isoformat()
        }

        print(record)

    else:

        print("Bitcoin data not found")

    time.sleep(5)