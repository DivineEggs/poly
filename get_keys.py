import os
from py_clob_client.client import ClobClient

key = os.environ.get("WALLET_KEY")
if not key:
    raise EnvironmentError("WALLET_KEY environment variable is not set.")

client = ClobClient("https://clob.polymarket.com", key=key, chain_id=137)
creds = client.create_or_derive_api_creds()

print("API_KEY:", creds.api_key)
print("SECRET:", creds.api_secret)
print("PASSPHRASE:", creds.api_passphrase)
