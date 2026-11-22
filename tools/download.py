"""Download NYISO MIS monthly CSV archives for a set of feeds.
Usage: python download.py <outdir>
"""
import sys, os, datetime as dt, urllib.request, concurrent.futures as cf

BASE = "https://mis.nyiso.com/public/csv"
# feed dir, zip suffix, first year to try
FEEDS = {
    "rtfuelmix": ("rtfuelmix_csv", 2015),
    "realtime": ("realtime_zone_csv", 1999),
    "damlbmp": ("damlbmp_zone_csv", 1999),
    "pal": ("pal_csv", 2001),
    "ExternalLimitsFlows": ("ExternalLimitsFlows_csv", 2001),
    "LimitingConstraints": ("LimitingConstraints_csv", 2001),
    "rtasp": ("rtasp_csv", 1999),
    "RealTimeEvents": ("RealTimeEvents_csv", 1999),
    "btmactualforecast": ("BTMEstimatedActual_csv", 2019),
}

def months(y0, end):
    d = dt.date(y0, 1, 1)
    while d <= end:
        yield d
        d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)

def fetch(job):
    url, path = job
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path, "cached"
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            data = r.read()
        with open(path, "wb") as f:
            f.write(data)
        return path, "ok"
    except urllib.error.HTTPError as e:
        return path, f"http{e.code}"
    except Exception as e:
        return path, f"err {e}"

def main(out):
    today = dt.date.today()
    jobs = []
    for feed, (suffix, y0) in FEEDS.items():
        os.makedirs(f"{out}/{feed}", exist_ok=True)
        for m in months(y0, today.replace(day=1)):
            name = f"{m:%Y%m%d}{suffix}.zip"
            jobs.append((f"{BASE}/{feed}/{name}", f"{out}/{feed}/{name}"))
    counts = {}
    with cf.ThreadPoolExecutor(12) as ex:
        for path, status in ex.map(fetch, jobs):
            feed = path.split("/")[-2]
            key = (feed, status.split()[0])
            counts[key] = counts.get(key, 0) + 1
    for k in sorted(counts):
        print(k, counts[k])

if __name__ == "__main__":
    main(sys.argv[1])
