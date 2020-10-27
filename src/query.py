import requests

url = "https://bisq.markets/api/trades?market=all&timestamp_from=1603656800&timestamp_to=1603756800&limit=2000"

payload = {}
headers= {}

response = requests.request("GET", url, headers=headers, data = payload)

print(response.text.encode('utf8'))
