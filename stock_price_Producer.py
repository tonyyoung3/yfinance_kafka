from confluent_kafka import Producer
import yfinance as yf
import time
import requests
import json
# Kafka producer configuration

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer <token>'
}


conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'stock-price-producer'
}

# Create a Kafka producer instance
producer = Producer(conf)

# Kafka topic to send stock price data
topic = 'conn-events'

# Ticker symbol of the stock (e.g., Apple Inc.)
ticker_symbol = 'BTC-USD'

#Function to fetch stock price and send to Kafka
def fetch_and_send_stock_price():
    while True:
        try:
            url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'

            response = requests.get(url, headers=headers)
            data = json.loads(response.text)
            price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]

            # Produce the stock price to the Kafka topic
            producer.produce(topic, key=ticker_symbol, value=str(price))
            producer.flush()

            print(f"Sent {ticker_symbol} price to Kafka: {price}")

        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

        # Sleep for a specified interval (e.g., 5 seconds) before fetching the next price
        time.sleep(30)

# Start sending stock price data
fetch_and_send_stock_price()


# stock = yf.Ticker(ticker_symbol)
# print(stock.info['description'])


# Make an HTTP request to the website

#https://query2.finance.yahoo.com/v8/finance/chart/btc-usd
