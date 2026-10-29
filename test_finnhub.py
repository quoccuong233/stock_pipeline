import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ["FINNHUB_API_KEY"]

url = f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={API_KEY}"

response = requests.get(url)

print(response.status_code)
print(response.json())
