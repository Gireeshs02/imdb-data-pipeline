import requests
import os

datasets = {
    "title.basics.tsv.gz" : "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "name.basics.tsv.gz": "https://datasets.imdbws.com/name.basics.tsv.gz"
}

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

for filename, url in datasets.items():
    print(f"Downloading {filename}...")
    response = requests.get(url, stream=True)
    with open(os.path.join(data_dir, filename), "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {filename}")