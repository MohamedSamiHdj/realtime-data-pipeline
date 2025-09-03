# fetch_data.py
import os
import requests

os.makedirs("data/raw", exist_ok=True)

# NYC TLC Parquet endpoint (2023 Jan by default)
MONTHS = ["2023-01"]  # For additional: ["2023-01","2023-02",...]
BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TEMPLATE = "{base}/yellow_tripdata_{month}.parquet"

def download(url: str, out_path: str):
    if os.path.exists(out_path):
        print(f"Exists, skip: {out_path}")
        return
    print(f"-> Downloading {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print(f"Saved {out_path}")

for m in MONTHS:
    url = TEMPLATE.format(base=BASE, month=m)
    out = os.path.join("data", "raw", f"yellow_tripdata_{m}.parquet")
    download(url, out)