import requests

def fetch_deribit_data(url):
    """Fetch data from a Deribit API endpoint."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get('result', {})

# 1 request per 10 seconds is allowed
def get_bitcoin_0dte_option_chain():
    url = "https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&expired=false&kind=option"
    instruments = fetch_deribit_data(url)
    if not instruments:
        print("No options fetched from Deribit.")
        return None

    min_expiry = min(inst["expiration_timestamp"] for inst in instruments)
    shortest_expiry_options = [inst for inst in instruments if inst["expiration_timestamp"] == min_expiry]
    return shortest_expiry_options, min_expiry

# 20 requests per second allowed
def fetch_ticker_data(name):
    ticker_url = f"https://www.deribit.com/api/v2/public/ticker?instrument_name={name}"
    return fetch_deribit_data(ticker_url)

