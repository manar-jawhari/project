from confluent_kafka import Consumer
from cassandra.cluster import Cluster
import json
from datetime import datetime

# Configuration Kafka Confluent
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'financial_data_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['financial_data'])

# Connexion à Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('financial_data')

def insert_data_into_cassandra(record):
    """Insère un enregistrement dans Cassandra."""
    # Conversion de la date (str) en objet datetime.date
    record_date = datetime.strptime(record['date'], "%Y-%m-%d").date()

    session.execute(
        """
        INSERT INTO stocks (date, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            record_date, record['symbol'], record['open'], record['high'],
            record['low'], record['close'], record['volume']
        )
    )

if __name__ == '__main__':
    print("Lecture des données Kafka et stockage dans Cassandra...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur du consumer: {msg.error()}")
                continue

            # Debug: Affiche le message reçu
            print(f"Message reçu: {msg.value().decode('utf-8')}")

            # Parse le message en dictionnaire Python
            record = json.loads(msg.value().decode('utf-8'))

            # Vérifie la présence des clés nécessaires
            if all(key in record for key in ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']):
                insert_data_into_cassandra(record)
                print(f"Données enregistrées pour {record['symbol']} le {record['date']}.")
            else:
                print(f"Clés manquantes dans l'enregistrement : {record}")
    except KeyboardInterrupt:
        print("Arrêt du consumer.")
    finally:
        consumer.close()
