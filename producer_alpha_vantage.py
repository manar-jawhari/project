from confluent_kafka import Producer
import requests
import pandas as pd
import json

# Configuration Kafka Confluent
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

# API Key and Topic
API_KEY = 'TEKIC84N2U29M0MQ'
KAFKA_TOPIC = 'financial_data'

# List of symbols
symbols = ['AAPL', 'GOOG', 'MSFT', 'AMZN']

# Path for the Excel file
excel_file = "financial_data.xlsx"

def fetch_data(symbol):
    """Fetch stock data from Alpha Vantage API."""
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}&outputsize=full'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json().get('Time Series (Daily)', {})
        return [
            {
                'date': date,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume']),
                'symbol': symbol
            }
            for date, values in data.items()
        ]
    else:
        print(f"Error fetching data for {symbol}.")
        return []

def send_to_kafka(data):
    """Send data to Kafka."""
    for record in data:
        producer.produce(KAFKA_TOPIC, key=record['symbol'], value=json.dumps(record))
        print(f"Data sent for {record['symbol']} on {record['date']}.")
    producer.flush()

def save_to_excel(data):
    """Save data to an Excel file."""
    df = pd.DataFrame(data)
    df.to_excel(excel_file, index=False)
    print(f"Data saved to {excel_file}.")

if __name__ == '__main__':
    all_data = []
    for symbol in symbols:
        data = fetch_data(symbol)
        all_data.extend(data)
        send_to_kafka(data)

    save_to_excel(all_data)
    print("Data sending and saving completed.")
