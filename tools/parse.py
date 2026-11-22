"""Parse NYISO monthly zips into one parquet file per feed per year.
Usage: python parse.py <zipdir> <outdir> [feed ...]
"""
import sys, os, io, glob, zipfile, warnings
import pandas as pd

warnings.filterwarnings("ignore")
FMTS = ["%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%y %H:%M:%S", "%m/%d/%y %H:%M"]

def parse_ts(s):
    s = s.astype(str).str.strip().str.strip('"')
    for f in FMTS:
        out = pd.to_datetime(s, format=f, errors="coerce")
        if out.notna().mean() > 0.95:
            return out
    return pd.to_datetime(s, format="mixed", errors="coerce")

def num(s):
    return pd.to_numeric(s, errors="coerce")

def read_csv(raw):
    return pd.read_csv(io.BytesIO(raw), dtype=str, skipinitialspace=True, on_bad_lines="skip")

def norm(feed, df):
    c = list(df.columns)
    if feed in ("realtime", "damlbmp"):
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "zone": df[c[1]].str.strip(),
                            "lbmp": num(df[c[3]]), "losses": num(df[c[4]]), "cong": num(df[c[5]])})
    elif feed == "pal":
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "zone": df[c[2]].str.strip(), "load": num(df[c[4]])})
    elif feed == "rtfuelmix":
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "fuel": df[c[2]].str.strip(), "mw": num(df[c[3]])})
    elif feed == "btmactualforecast":
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "zone": df[c[2]].str.strip(), "mw": num(df[c[3]])})
    elif feed == "ExternalLimitsFlows":
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "iface": df[c[1]].str.strip(), "flow": num(df[c[3]]),
                            "poslim": num(df[c[4]]), "neglim": num(df[c[5]])})
    elif feed == "LimitingConstraints":
        if "Limiting Facility" in c:
            name, cont = df["Limiting Facility"], df.get("Contingency")
        else:
            name, cont = df[c[1]], None
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "facility": name.str.strip(),
                            "contingency": cont.str.strip() if cont is not None else None,
                            "cost": num(df[c[-1]])})
    elif feed == "rtasp":
        if c[1].startswith("East"):  # legacy East/West layout
            ts = parse_ts(df[c[0]])
            parts = []
            for i, reg in ((1, "EAST"), (5, "WEST")):
                parts.append(pd.DataFrame({"ts": ts, "zone": reg, "spin10": num(df[c[i]]),
                                           "nsync10": num(df[c[i + 1]]), "op30": num(df[c[i + 2]])}))
            out = pd.concat(parts)
        else:
            out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "zone": df[c[2]].str.strip(), "spin10": num(df[c[4]]),
                                "nsync10": num(df[c[5]]), "op30": num(df[c[6]])})
    elif feed == "RealTimeEvents":
        out = pd.DataFrame({"ts": parse_ts(df[c[0]]), "msg": df[c[1]].astype(str).str.strip()})
    return out.dropna(subset=["ts"])

def do_year(feed, year, zipdir, outdir):
    frames = []
    for zp in sorted(glob.glob(f"{zipdir}/{feed}/{year}*.zip")):
        try:
            z = zipfile.ZipFile(zp)
        except zipfile.BadZipFile:
            continue
        for n in z.namelist():
            try:
                frames.append(norm(feed, read_csv(z.read(n))))
            except Exception as e:
                print("skip", feed, n, e)
    if not frames:
        return 0
    df = pd.concat(frames, ignore_index=True)
    os.makedirs(f"{outdir}/{feed}", exist_ok=True)
    df.to_parquet(f"{outdir}/{feed}/{year}.parquet", index=False)
    return len(df)

if __name__ == "__main__":
    from concurrent.futures import ProcessPoolExecutor
    zipdir, outdir = sys.argv[1], sys.argv[2]
    feeds = sys.argv[3:] or sorted(os.listdir(zipdir))
    jobs = []
    for feed in feeds:
        years = sorted({os.path.basename(p)[:4] for p in glob.glob(f"{zipdir}/{feed}/*.zip")})
        jobs += [(feed, y) for y in years]
    with ProcessPoolExecutor(2) as ex:
        futs = {ex.submit(do_year, f, y, zipdir, outdir): (f, y) for f, y in jobs}
        tot = {}
        for fu in futs:
            f, y = futs[fu]
            tot[f] = tot.get(f, 0) + fu.result()
    for f, n in tot.items():
        print(f, n)
