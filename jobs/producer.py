import json
import time
import uuid
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone

# Kafka setup
KAFKA_BROKER = ['kafka-1:9092','kafka-2:9092','kafka-3:9092']
KAFKA_TOPIC = 'crypto-prices'

# CoinGecko API setup
COINGECKO_URL = 'https://api.coingecko.com/api/v3/coins/markets'
PARAMS = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,solana,cardano,ripple,dogecoin,polkadot,binancecoin,avalanche,chainlink,polygon,cosmos,uniswap,litecoin,stellar,vechain,shiba-inu,tron,tezos,neo',
    'order': 'market_cap_desc',
    'per_page': 20,
    'page': 1,
    'sparkline': 'false',
    'price_change_percentage': '24h'
}

# Desired keys to extract
desired_keys = [
    "id", "symbol", "current_price", "market_cap",
    "total_volume", "high_24h", "low_24h", "last_updated"
]

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    linger_ms=10
)

print("Streaming filtered crypto data from CoinGecko to Kafka...")


try:
    while True:
        response = requests.get(COINGECKO_URL, params=PARAMS, timeout=10)
        if response.status_code == 200:
            data = response.json()

            # Filter only desired keys from each coin
            filtered_data = [
                {key: coin.get(key) for key in desired_keys}
                for coin in data
            ]

            producer_id = "coingecko-producer-v1"

            for coin in filtered_data:
                producer.send(KAFKA_TOPIC, value={
                    "event_id": str(uuid.uuid4()),
                    "event_time": coin["last_updated"],
                    "producer_time": datetime.now(timezone.utc).isoformat(),
                    "producer_id": producer_id,
                    **coin
                })

            producer.flush()
            print(f"Pushed {len(filtered_data)} records at {datetime.now(timezone.utc).isoformat()}")

        else:
            print(f"API Error: {response.status_code} - {response.text}")

        time.sleep(30)
        
except KeyboardInterrupt:
    print("\nStopped manually.")

finally:
    producer.close()