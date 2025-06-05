import requests, io, time, json, itertools, pathlib, boto3, hashlib, sys
BASE = "https://www.bergen.kommune.no/umbraco/api/saksinnsyn"
BUCKET = "bergen-scraper-REPLACE"
s3 = boto3.client("s3")

def head(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key); return True
    except s3.exceptions.ClientError: return False

def search(term, page, size=50):
    r = requests.get(f"{BASE}/search",
                     params={"term": term, "page": page, "pageSize": size},
                     headers={"User-Agent":"buildai-scraper/0.1"}, timeout=30)
    r.raise_for_status(); return r.json()

def details(sid):
    r = requests.get(f"{BASE}/detail", params={"saksid": sid}, timeout=30)
    r.raise_for_status(); return r.json()

def put_bytes(data, key, mime):
    s3.upload_fileobj(io.BytesIO(data), BUCKET, key,
        ExtraArgs={"ContentType": mime})

def scrape_year(year):
    term = f"{year}/"
    for page in itertools.count(1):
        hits = search(term, page)
        if not hits: break
        for h in hits:
            meta = details(h["id"])
            saksnr = h["saksnummer"].replace("/","-")
            meta_key = f"{year}/{saksnr}/meta.json"
            if not head(meta_key):
                put_bytes(json.dumps(meta,ensure_ascii=False).encode(), meta_key, "application/json")
            for att in meta.get("attachments", []):
                url = att["url"]; fname = pathlib.Path(url).name
                key = f"{year}/{saksnr}/{fname}"
                if head(key): continue
                pdf = requests.get(url, timeout=60); pdf.raise_for_status()
                put_bytes(pdf.content, key, "application/pdf")
                time.sleep(0.5)

if __name__ == "__main__":
    if len(sys.argv) < 2: print("bruk: python scrape_bergen.py 2024 [2023]"); sys.exit(1)
    for yr in map(int, sys.argv[1:]): scrape_year(yr)
