from oandapyV20 import API
from oandapyV20.endpoints.accounts import AccountDetails
from dotenv import load_dotenv
import os

load_dotenv()

client = API(access_token=os.getenv('OANDA_API_KEY'), environment='practice')
r = AccountDetails(os.getenv('OANDA_ACCOUNT_ID'))
client.request(r)

account = r.response['account']
print(f"Connected successfully!")
print(f"Account ID: {account['id']}")
print(f"Balance: ${float(account['balance']):,.2f}")
print(f"Currency: {account['currency']}")
print(f"Open Trades: {account['openTradeCount']}")
