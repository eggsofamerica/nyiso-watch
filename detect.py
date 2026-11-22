"""NYISO watcher: detect rare/notable grid events since the last run.
python3 detect.py state.json calib.json out.json [--now 'YYYY-MM-DD HH:MM']  (times are NY local)"""
import sys, io, re, json, zipfile, urllib.request, datetime as dt, warnings
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
warnings.filterwarnings("ignore")
B = "https://mis.nyiso.com/public/csv"
ZN = {"WEST": "A (West)", "GENESE": "B (Genesee)", "CENTRL": "C (Central)", "NORTH": "D (North)", "MHK VL": "E (Mohawk Valley)",
      "CAPITL": "F (Capital)", "HUD VL": "G (Hudson Valley)", "MILLWD": "H (Millwood)", "DUNWOD": "I (Dunwoodie)",
      "N.Y.C.": "J (NYC)", "LONGIL": "K (Long Island)"}
FEED = {"fuel": ("rtfuelmix", "rtfuelmix"), "rt": ("realtime", "realtime_zone"), "load": ("pal", "pal"),
        "asp": ("rtasp", "rtasp"), "ev": ("RealTimeEvents", "RealTimeEvents"), "cons": ("LimitingConstraints", "LimitingConstraints"),
        "da": ("damlbmp", "damlbmp_zone"), "btm": ("btmactualforecast", "BTMEstimatedActual"),
        "flows": ("ExternalLimitsFlows", "ExternalLimitsFlows"), "isolf": ("isolf", "isolf")}
FMTS = ["%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%y %H:%M:%S", "%m/%d/%y %H:%M"]

def get(url):
    try:
        with urllib.request.urlopen(url, timeout=60) as r: return r.read()
    except Exception: return None

def ts(s):
    s = s.astype(str).str.strip()
    for f in FMTS:
        o = pd.to_datetime(s, format=f, errors="coerce")
        if o.notna().mean() > .95: return o
    return pd.to_datetime(s, format="mixed", errors="coerce")

def norm(k, raw):
    d = pd.read_csv(io.BytesIO(raw), dtype=str, skipinitialspace=True, on_bad_lines="skip"); c = list(d.columns)
    n = lambda i: pd.to_numeric(d[c[i]], errors="coerce")
    t = ts(d[c[0]])
    if k in ("rt", "da"): o = pd.DataFrame({"ts": t, "zone": d[c[1]], "lbmp": n(3), "cong": n(5)})
    elif k == "load": o = pd.DataFrame({"ts": t, "zone": d[c[2]], "v": n(4)})
    elif k == "fuel": o = pd.DataFrame({"ts": t, "fuel": d[c[2]], "v": n(3)})
    elif k == "btm": o = pd.DataFrame({"ts": t, "zone": d[c[2]], "v": n(3)})
    elif k == "asp": o = pd.DataFrame({"ts": t, "zone": d[c[2]], "spin10": n(4)})
    elif k == "ev": o = pd.DataFrame({"ts": t, "msg": d[c[1]].astype(str)})
    elif k == "isolf": o = pd.DataFrame({"ts": t, "v": pd.to_numeric(d["NYISO"], errors="coerce")})
    elif k == "flows": o = pd.DataFrame({"ts": t, "iface": d[c[1]].str.strip(), "v": n(3), "lim": n(4)})
    elif k == "cons":
        o = pd.DataFrame({"ts": t, "fac": d["Limiting Facility"] if "Limiting Facility" in c else d[c[1]], "cost": n(len(c) - 1)})
    return o.dropna(subset=["ts"])

def day(k, D):
    f, s = FEED[k]; raw = get(f"{B}/{f}/{D:%Y%m%d}{s}.csv")
    if raw is None:  # older days live only in the monthly zip
        z = get(f"{B}/{f}/{D:%Y%m}01{s}_csv.zip")
        if z is None: return None
        try: raw = zipfile.ZipFile(io.BytesIO(z)).read(f"{D:%Y%m%d}{s}.csv")
        except Exception: return None
    try: return norm(k, raw)
    except Exception: return None

def days(k, d0, d1):
    out = [day(k, d0 + dt.timedelta(i)) for i in range((d1 - d0).days + 1)]
    out = [o for o in out if o is not None and len(o)]
    return pd.concat(out, ignore_index=True) if out else None

def month(k, m):
    f, s = FEED[k]; z = get(f"{B}/{f}/{m:%Y%m}01{s}_csv.zip")
    if z is None: return None
    zf = zipfile.ZipFile(io.BytesIO(z)); fr = []
    for nm in zf.namelist():
        if nm.endswith(".csv"):
            try: fr.append(norm(k, zf.read(nm)))
            except Exception: pass
    return pd.concat(fr, ignore_index=True) if fr else None

def since(k, before, hit, floor=dt.date(1999, 11, 1)):
    """Latest timestamp before `before` where hit(df) is true, scanning monthly archives backward."""
    m = before.date().replace(day=1); ms = []
    while m >= floor: ms.append(m); m = (m - dt.timedelta(1)).replace(day=1)
    for i in range(0, len(ms), 12):
        with ThreadPoolExecutor(12) as ex: frames = list(ex.map(lambda x: month(k, x), ms[i:i + 12]))
        best = None
        for f in frames:
            if f is None or not len(f): continue
            f = f[f.ts < before]; t = hit(f)
            if t is not None and len(t): best = max(best, t.max()) if best is not None else t.max()
        if best is not None: return best
    return None

def hourly(df, col, by=None, minn=10):
    df = df.assign(h=(df.ts - pd.Timedelta(seconds=1)).dt.floor("h"))
    g = df.groupby(["h"] + ([by] if by else []))[col]
    o = g.mean().to_frame("v").join(g.count().rename("n")).reset_index()
    return o[o.n >= minn]

def ago(t, now):
    if t is None: return "at least Nov 1999 (the start of NYISO's public archive)"
    days_ = (now - t).days
    return f"{t:%b %-d, %Y}" + (f" ({days_ // 365} yr ago)" if days_ >= 730 else f" ({days_} days ago)" if days_ >= 2 else "")

def zone_hourly_hit(zone, thr, op):
    def f(df):
        h = hourly(df[df.zone == zone], "lbmp", minn=1)
        h = h[h.v >= thr] if op == ">=" else h[h.v <= thr]
        return h.h
    return f

def nyca_hourly(load):
    l = load[load.zone.isin(ZN)].groupby("ts").v.agg(["sum", "count"]).reset_index()
    l = l[l["count"] >= 11].rename(columns={"sum": "v"})
    return hourly(l, "v")

IMPORTS = {"Hydro-Québec": ["SCH - HQ - NY", "SCH - HQ_CEDARS", "SCH - HQ_CHPE"], "Ontario": ["SCH - OH - NY"],
           "PJM": ["SCH - PJ - NY", "SCH - PJM_HTP", "SCH - PJM_NEPTUNE", "SCH - PJM_VFT"],
           "New England": ["SCH - NE - NY", "SCH - NPX_1385", "SCH - NPX_CSC"]}
LINES = {"CHPE": "SCH - HQ_CHPE", "Neptune": "SCH - PJM_NEPTUNE", "Cross-Sound": "SCH - NPX_CSC"}
MIX = {"Gas & dual fuel": ["Natural Gas", "Dual Fuel"], "Nuclear": ["Nuclear"], "Hydro": ["Hydro"], "Wind": ["Wind"],
       "Other renewables": ["Other Renewables"], "Other fossil": ["Other Fossil Fuels"]}

def day_stats(D, label, end):
    r1 = lambda v, n=0: None if v is None or pd.isna(v) else round(float(v), n)
    cut = lambda df: None if df is None else df[df.ts <= min(end, pd.Timestamp(D) + pd.Timedelta(days=1))]
    out = {"label": label, "date": str(D)}
    L = cut(day("load", D))
    if L is not None and len(L):
        h = nyca_hourly(L); h = h[(h.h >= pd.Timestamp(D)) & (h.h < end)]
        if len(h):
            pk = h.loc[h.v.idxmax()]; mn = h.loc[h.v.idxmin()]
            out["hours"] = int(len(h))
            out["load"] = {"energy_gwh": r1(h.v.sum() / 1000, 1), "peak_mw": r1(pk.v), "peak_at": f"{pk.h:%-I%p}",
                           "min_mw": r1(mn.v), "min_at": f"{mn.h:%-I%p}"}
    F = cut(day("fuel", D))
    if F is not None and len(F):
        w = F.pivot_table(index="ts", columns="fuel", values="v", aggfunc="mean").dropna()
        w = w[(w.index > pd.Timestamp(D)) & (w.index <= end)]
        if len(w):
            tot = w.sum(axis=1).sum(); mix = {k: r1(w[[c for c in v if c in w]].sum(axis=1).sum() / tot, 3) for k, v in MIX.items()}
            hrs = len(w) / 12
            out["gen"] = {"energy_gwh": r1(w.sum(axis=1).mean() * hrs / 1000, 1), "mix": mix,
                          "zero_carbon": r1(sum(mix[k] for k in ("Nuclear", "Hydro", "Wind", "Other renewables")), 3)}
    X = cut(day("flows", D))
    if X is not None and len(X):
        X = X[(X.ts > pd.Timestamp(D)) & (X.ts <= end)]
        piv = X.pivot_table(index="ts", columns="iface", values="v", aggfunc="mean")
        imp = {}
        for g, cols in IMPORTS.items():
            cols = [c for c in cols if c in piv]
            if cols:
                ser = piv[cols].sum(axis=1)
                imp[g] = {"avg_mw": r1(ser.mean()), "max_mw": r1(ser.max()), "min_mw": r1(ser.min())}
        out["imports"] = imp
        out["net_import_avg_mw"] = r1(sum(v["avg_mw"] for v in imp.values()))
        out["lines"] = {k: {"avg_mw": r1(piv[c].mean()), "max_mw": r1(piv[c].max())} for k, c in LINES.items() if c in piv}
        tl = X[X.iface.isin(["CENTRAL EAST - VC", "UPNY CONED", "TOTAL EAST"])].assign(p=lambda x: x.v / x.lim)
        out["transfers"] = {i: {"max_mw": r1(g.v.max()), "max_pct_limit": r1(g.p.max(), 2)} for i, g in tl.groupby("iface")}
    R = cut(day("rt", D))
    if R is not None and len(R):
        rh = hourly(R[R.zone.isin(ZN)], "lbmp", "zone"); rh = rh[(rh.h >= pd.Timestamp(D)) & (rh.h < end)]
        if len(rh):
            pk = rh.loc[rh.v.idxmax()]
            g = rh.groupby("zone").v
            out["rt"] = {"avg_J": r1(g.mean().get("N.Y.C."), 2), "avg_A": r1(g.mean().get("WEST"), 2), "avg_K": r1(g.mean().get("LONGIL"), 2),
                         "max": r1(pk.v), "max_zone": ZN[pk.zone].split()[0], "max_at": f"{pk.h:%-I%p}",
                         "min": r1(rh.v.min()), "min_zone": ZN[rh.loc[rh.v.idxmin()].zone].split()[0], "min_at": f"{rh.loc[rh.v.idxmin()].h:%-I%p}"}
    A = day("da", D)
    if A is not None and len(A):
        g = A[A.zone.isin(ZN)].groupby("zone").lbmp.mean()
        out["da"] = {"avg_J": r1(g.get("N.Y.C."), 2), "avg_A": r1(g.get("WEST"), 2), "avg_K": r1(g.get("LONGIL"), 2)}
    C = cut(day("cons", D))
    if C is not None and len(C):
        C = C[C.ts > pd.Timestamp(D)]
        if len(C):
            g = C.assign(a=C.cost.abs()).groupby("fac").agg(total=("a", "sum"), n=("a", "size"), peak=("a", "max")).sort_values("total", ascending=False).head(3)
            out["constraints"] = [{"name": f, "intervals": int(r.n), "peak": r1(r.peak)} for f, r in g.iterrows()]
    if pd.Timestamp(D) + pd.Timedelta(days=1) <= end:
        b = day("btm", D)
        if b is not None and len(b):
            s = b[b.zone == "SYSTEM"]
            if len(s): pk = s.loc[s.v.idxmax()]; out["btm_peak"] = {"mw": r1(pk.v), "at": f"{pk.ts:%-I%p}", "gwh": r1(s.v.sum() / 1000, 1)}
            if len(s) and L is not None and len(L):
                bh = s.assign(h=s.ts.dt.floor("h")).groupby("h").v.mean()
                gl = nyca_hourly(L).set_index("h").v
                tl = (gl + bh.reindex(gl.index).fillna(0)).dropna()
                if len(tl) and "load" in out:
                    pk2 = tl.idxmax(); mn2 = tl.idxmin()
                    out["load"]["with_btm"] = {"energy_gwh": r1(tl.sum() / 1000, 1), "peak_mw": r1(tl[pk2]), "peak_at": f"{pk2:%-I%p}",
                                               "min_mw": r1(tl[mn2]), "min_at": f"{mn2:%-I%p}"}
                if "gen" in out:
                    bg = s.v.sum() / 1000; gg = out["gen"]["energy_gwh"]; tot2 = gg + bg
                    mix2 = {k: r1(v * gg / tot2, 3) for k, v in out["gen"]["mix"].items() if v is not None}
                    mix2["Behind-the-meter solar"] = r1(bg / tot2, 3)
                    out["gen"]["with_btm"] = {"energy_gwh": r1(tot2, 1), "mix": mix2,
                                              "zero_carbon": r1((out["gen"]["zero_carbon"] * gg + bg) / tot2, 3)}
    return out

def today_ahead(D):
    r1 = lambda v, n=0: None if v is None or pd.isna(v) else round(float(v), n)
    out = {"date": str(D)}
    fc = day("isolf", D)
    if fc is not None and len(fc):
        f = fc[fc.ts.dt.date == D]
        if len(f):
            pk = f.loc[f.v.idxmax()]; mn = f.loc[f.v.idxmin()]
            out["load_fc"] = {"energy_gwh": r1(f.v.sum() / 1000, 1), "peak_mw": r1(pk.v), "peak_at": f"{pk.ts:%-I%p}",
                              "min_mw": r1(mn.v), "min_at": f"{mn.ts:%-I%p}"}
    A = day("da", D)
    if A is not None and len(A):
        A = A[A.zone.isin(ZN)]
        if len(A):
            g = A.groupby("zone").lbmp.mean(); pk = A.loc[A.lbmp.idxmax()]; mn = A.loc[A.lbmp.idxmin()]
            out["da"] = {"avg_J": r1(g.get("N.Y.C."), 2), "avg_K": r1(g.get("LONGIL"), 2), "avg_A": r1(g.get("WEST"), 2),
                         "max": r1(pk.lbmp), "max_zone": ZN[pk.zone].split()[0], "max_at": f"{pk.ts:%-I%p}",
                         "min": r1(mn.lbmp), "min_zone": ZN[mn.zone].split()[0], "min_at": f"{mn.ts:%-I%p}"}
    return out

def main(sp, cp, op, now):
    st, cal = json.load(open(sp)), json.load(open(cp))
    R, cool = st["records"], st.setdefault("cooldown", {})
    alerts, watch = [], []
    end = (now - pd.Timedelta(minutes=12)).floor("h")            # hours starting before `end` are complete
    cur = pd.Timestamp(st["cursor"]) if st.get("cursor") else end - pd.Timedelta(hours=2)
    cur = max(cur, end - pd.Timedelta(hours=48))
    d0, d1 = (cur - pd.Timedelta(hours=1)).date(), now.date()
    data = {k: days(k, d0, d1) for k in ("fuel", "rt", "load", "asp", "ev", "cons")}

    def cooled(key, hours):
        t = cool.get(key)
        return t is None or now - pd.Timestamp(t) >= pd.Timedelta(hours=hours)
    def alert(key, hours, typ, title, detail, t, sev="notable"):
        if hours and not cooled(key, hours): return
        cool[key] = now.isoformat(timespec="minutes")
        alerts.append({"id": re.sub(r"[^A-Za-z0-9_-]+", "-", f"{typ}-{key}")[:100] + f"-{pd.Timestamp(t):%Y%m%d%H%M}", "ts": pd.Timestamp(t).isoformat(timespec="minutes"),
                       "type": typ, "title": title, "detail": detail, "severity": sev})
    def record(key, val, t, better, label, fmt, cool_h=12):
        old = R.get(key)
        if old is None or better(val, old[0]):
            prev = f" Previous record: {fmt(old[0])} on {pd.Timestamp(old[1]):%b %-d, %Y}." if old else ""
            R[key] = [round(float(val), 3), pd.Timestamp(t).isoformat(timespec="minutes")]
            alert("rec:" + key, cool_h, "record", f"New record: {label} {fmt(val)}", f"{label[0].upper() + label[1:]} hit {fmt(val)} " + (f"on {pd.Timestamp(t):%a %b %-d}" if key.endswith("_d") else f"in the hour starting {pd.Timestamp(t):%-I%p %b %-d}") + f".{prev}", t, "record")
            return True
    new = lambda h: h[(h.h >= cur) & (h.h < end)]
    mw = lambda v: f"{v:,.0f} MW"; pctf = lambda v: f"{v:.1%}"

    # ---- prices
    rt = data["rt"]
    if rt is not None:
        rt = rt[rt.zone.isin(ZN)]; rh = new(hourly(rt, "lbmp", "zone"))
        hi = rh[rh.v > 500]
        pk = hi.loc[hi.v.idxmax()] if len(hi) else None
        if pk is not None and (cooled("price_spike", 18) or pk.v >= 2 * st.get("peaks", {}).get("price", 1e9)):
            st.setdefault("peaks", {})["price"] = float(pk.v); cool.pop("price_spike", None)
            zs = sorted(hi.zone.unique(), key=lambda z: ZN[z])
            t0 = since("rt", hi.h.min(), zone_hourly_hit(pk.zone, pk.v, ">="))
            alert("price_spike", 18, "price", f"Price spike: Zone {ZN[pk.zone].split()[0]} ${pk.v:,.0f}/MWh",
                  f"Hourly real-time LBMP topped $500 in {len(zs)} of 11 zones ({', '.join(ZN[z].split()[0] for z in zs)}). Peak: Zone {ZN[pk.zone]} at ${pk.v:,.0f}/MWh, hour starting {pk.h:%-I%p %b %-d}. At least one zone was above $500 for {hi.h.nunique()} hour(s) between {hi.h.min():%-I%p} and {hi.h.max() + pd.Timedelta(hours=1):%-I%p}. Highest hourly price in that zone since {ago(t0, now)}.",
                  pk.h, "major" if len(zs) >= 6 else "notable")
        neg = rh[rh.v < 0]; nh_ = neg.groupby("h").zone.nunique(); bad = nh_[nh_ >= 3]
        if len(bad) and cooled("negative", 12):
            lo = neg[neg.h.isin(bad.index)]; pk = lo.loc[lo.v.idxmin()]; zs = sorted(lo.zone.unique(), key=lambda z: ZN[z])
            def hit(df):
                h = hourly(df[df.zone.isin(ZN)], "lbmp", "zone", minn=1); c = h[h.v < 0].groupby("h").zone.nunique()
                return pd.Series(c[c >= 3].index)
            t0 = since("rt", bad.index.min(), hit)
            alert("negative", 12, "price", f"Negative prices in {bad.max()} zones",
                  f"Hourly real-time LBMP was negative in {bad.max()} of 11 zones at once ({', '.join(ZN[z].split()[0] for z in zs)}), for {len(bad)} hour(s) between {bad.index.min():%-I%p} and {bad.index.max() + pd.Timedelta(hours=1):%-I%p} {bad.index.min():%b %-d}. Lowest: Zone {ZN[pk.zone]} at {'-' if pk.v < 0 else ''}${abs(pk.v):,.0f}/MWh. Negative prices this widespread happen on about 6 days a year. Last time 3+ zones went negative together: {ago(t0, now)}.",
                  bad.index.min(), "major" if bad.max() >= 6 else "notable")
        # watch: J-A spread, J congestion
        p = rh.pivot(index="h", columns="zone", values="v")
        if "N.Y.C." in p and "WEST" in p:
            for h, v in (p["N.Y.C."] - p["WEST"]).dropna().items():
                lo_, hi_ = cal["watch"]["ja_spread"][str(h.month)]
                if v > hi_ or v < lo_: watch.append(f"Zone J minus Zone A spread ${v:,.0f} at {h:%-I%p} (99% range this month: ${lo_:,.0f} to ${hi_:,.0f})")
    # ---- reserves
    if data["asp"] is not None:
        ah = new(hourly(data["asp"], "spin10", "zone"))
        a = ah[ah.v > 500]
        if len(a):
            pk = a.loc[a.v.idxmax()]
            alert("reserve", 6, "reliability", f"Reserve shortage pricing: ${pk.v:,.0f}/MWh spinning reserves",
                  f"10-minute spinning reserve price averaged ${pk.v:,.0f}/MWh for the hour starting {pk.h:%-I%p %b %-d} ({pk.zone}). Sustained reserve prices this high mean the system was short of reserves. This happens on about 7 days a year.", pk.h)
    # ---- load
    if data["load"] is not None:
        L = data["load"]; nh = new(nyca_hourly(L))
        jh = new(hourly(L[L.zone == "N.Y.C."], "v"))
        for r in nh.itertuples():
            if r.h.month in (11, 12, 1, 2, 3): record("nyca_winter_load_h", r.v, r.h, lambda a, b: a > b, "winter NYCA load", mw)
            else: record("nyca_load_h", r.v, r.h, lambda a, b: a > b, "all-time NYCA load", mw)
            if 10 <= r.h.hour <= 15: record("midday_low_load_h", r.v, r.h, lambda a, b: a < b, "lowest midday NYCA load", mw, 20)
        for r in jh.itertuples(): record("nyc_load_h", r.v, r.h, lambda a, b: a > b, "NYC (Zone J) load", mw)
        if len(nh):
            pk = nh.loc[nh.v.idxmax()]; win = pk.h.month in (11, 12, 1, 2, 3)
            if pk.v > (24000 if win else 30000) and cooled("load_high", 20):
                def hit(df, thr=pk.v):
                    h = nyca_hourly(df); return h[h.v >= thr].h
                t0 = since("load", nh.h.min(), hit, dt.date(2001, 6, 1))
                alert("load_high", 20, "load", f"High demand: NYCA load {mw(pk.v)}",
                      f"Statewide load averaged {mw(pk.v)} in the hour starting {pk.h:%-I%p %b %-d}. Highest since {ago(t0, now)}. All-time record: {mw(R['nyca_load_h'][0])} ({pd.Timestamp(R['nyca_load_h'][1]):%b %Y}).", pk.h)
            for h, v in nh.set_index("h").v.items():
                lo_, hi_ = cal["watch"]["nyca_load"][str(h.month)]
                if v < lo_: watch.append(f"NYCA load unusually low for the month: {mw(v)} at {h:%-I%p}")
    # ---- fuel mix
    F = data["fuel"]
    if F is not None:
        w = F.pivot_table(index="ts", columns="fuel", values="v", aggfunc="mean").dropna()
        tot = w.sum(axis=1); zc = w[["Wind", "Hydro", "Nuclear", "Other Renewables"]].sum(axis=1)
        s = pd.DataFrame({"ts": w.index, "wind": w["Wind"].values, "oren": w["Other Renewables"].values,
                          "nuc": w["Nuclear"].values, "zc": (zc / tot).values, "gas": ((w["Natural Gas"] + w["Dual Fuel"]) / tot).values})
        for col, key, lab, fm in (("wind", "wind_h", "hourly wind output", mw), ("oren", "oren_h", "hourly 'Other Renewables' (grid solar, etc.) output", mw),
                                  ("zc", "zc_share_h", "zero-carbon share of generation (post-Indian Point)", pctf)):
            for r in new(hourly(s, col)).itertuples(): record(key, r.v, r.h, lambda a, b: a > b, lab, fm)
        n5 = s.set_index("ts").nuc; drop = n5 - n5.rolling("20min").max()
        d = drop[(drop.index >= cur) & (drop.index < now) & (drop < -400)]
        if len(d):
            t = d.idxmin(); before = n5[t] - d.min()
            alert("nuclear_trip", 6, "reliability", f"Nuclear output dropped {abs(d.min()):,.0f} MW",
                  f"Statewide nuclear output fell from about {mw(before)} to {mw(n5[t])} within 20 minutes around {t:%-I:%M%p %b %-d}. That usually means a unit tripped offline. Drops this large have happened on only one day in the past two years.", t, "major")
        gh = new(hourly(s, "gas"))
        for h, v in gh.set_index("h").v.items():
            lo_, hi_ = cal["watch"]["gas_share"][str(h.month)]
            if v < lo_ or v > hi_: watch.append(f"Gas share of generation {v:.0%} at {h:%-I%p} (99% range this month {lo_:.0%} to {hi_:.0%})")
    # ---- operator events
    if data["ev"] is not None:
        e = data["ev"][(data["ev"].ts >= cur) & (data["ev"].ts < now)]
        for r in e[e.msg.str.contains("major emergency state", case=False) & ~e.msg.str.contains("no longer", case=False)].itertuples():
            alert(f"emerg:{r.ts}", 0, "reliability", "NYISO declared a Major Emergency", f"{r.msg.strip('* ')} ({r.ts:%-I:%M%p %b %-d}). This has happened on only about 10 days since 2016.", r.ts, "major")
        cr = e[e.msg.str.startswith("Capacity request")]
        if len(cr):
            px = sorted(set(cr.msg.str.extract(r"Proxy:?\s*([A-Z\-]+)")[0].dropna()))
            alert("caprequest", 20, "reliability", "NYISO requested emergency capacity from neighbors",
                  f"NYISO posted capacity requests to neighboring systems via {', '.join(px) or 'external proxies'} (first at {cr.ts.min():%-I:%M%p %b %-d}). This is an emergency procedure. It has occurred on about 24 days in the past two years.", cr.ts.min())
    # ---- novel binding constraints
    C = data["cons"]
    if C is not None:
        big = C[(C.ts >= cur) & (C.ts < now) & (C.cost.abs() >= 500)]
        earlier = set(C[C.ts < cur].fac)
        cands = [f for f in big.fac.unique() if f not in earlier and cooled("cons:" + f, 24 * 365)][:4]
        if cands:
            ms = []; m = cur.date().replace(day=1)
            for _ in range(25): m = (m - dt.timedelta(1)).replace(day=1); ms.append(m)
            with ThreadPoolExecutor(12) as ex: hist = [x for x in ex.map(lambda x: month("cons", x), ms) if x is not None]
            curm = month("cons", cur.date().replace(day=1)); hist += [curm[curm.ts < cur.normalize()]] if curm is not None else []
            seen = set(pd.concat(hist).fac) if hist else set()
            for f in cands:
                if f in seen: continue
                r = big[big.fac == f].sort_values("cost").iloc[0]
                alert("cons:" + f, 24 * 365, "transmission", f"New binding constraint: {f}",
                      f"{f} bound with a shadow price of ${abs(r.cost):,.0f} at {r.ts:%-I:%M%p %b %-d}. It hadn't bound in real time at all in the prior two years. That can signal a new outage, a topology change, or a new flow pattern.", r.ts)
    # ---- once per day: yesterday's daily records + BTM solar; tomorrow's DA prices
    yday = (now - pd.Timedelta(hours=26)).date()
    if st.get("daily_done") != str(yday) and now.hour >= 1:
        fy = days("fuel", yday, yday)
        if fy is not None:
            w = fy.pivot_table(index="ts", columns="fuel", values="v", aggfunc="mean").dropna()
            if len(w) > 260:
                tot = w.sum(axis=1); zc = w[["Wind", "Hydro", "Nuclear", "Other Renewables"]].sum(axis=1)
                record("wind_d", w["Wind"].mean() * 24, pd.Timestamp(yday), lambda a, b: a > b, "daily wind energy", lambda v: f"{v:,.0f} MWh", 0)
                record("zc_share_d", zc.sum() / tot.sum(), pd.Timestamp(yday), lambda a, b: a > b, "daily zero-carbon share (post-Indian Point)", pctf, 0)
        b = days("btm", yday, yday)
        if b is not None:
            bh = hourly(b[b.zone == "SYSTEM"].assign(ts=lambda x: x.ts + pd.Timedelta(minutes=1)), "v", minn=1)
            if len(bh):
                pk = bh.loc[bh.v.idxmax()]; record("btm_h", pk.v, pk.h, lambda a, b: a > b, "behind-the-meter solar (NYISO estimate)", mw, 0)
                if fy is not None:
                    fw = hourly(fy[fy.fuel.isin(["Wind", "Other Renewables"])].groupby("ts").v.sum().reset_index(), "v")
                    ws = fw.merge(bh, on="h"); ws["t"] = ws.v_x + ws.v_y
                    if len(ws):
                        p2 = ws.loc[ws.t.idxmax()]; record("ws_h", p2.t, p2.h, lambda a, b: a > b, "wind + solar incl. behind-the-meter", mw, 0)
            st["daily_done"] = str(yday)
    for tom in (now.date(), (now + pd.Timedelta(days=1)).date()):
        if st.get("da_done") and st["da_done"] >= str(tom): continue
        da = day("da", tom)
        if da is not None and len(da) >= 200:
            st["da_done"] = str(tom)
            dd = da[da.zone.isin(ZN)].groupby("zone").lbmp.mean()
            thr = pd.Series(cal["da_p99"]).reindex(dd.index); hi = dd[dd > thr]
            last = st.get("da_last")
            quiet = not last or (pd.Timestamp(tom) - pd.Timestamp(last[0])).days > 3 or hi.max() >= 1.25 * last[1]
            if len(hi) and quiet:
                z = hi.idxmax(); v = hi.max(); st["da_last"] = [str(tom), float(v)]
                def hit(df, z=z, v=v):
                    g = df[df.zone == z].assign(d=lambda x: x.ts.dt.date).groupby("d").lbmp.mean(); return pd.to_datetime(pd.Series(g[g >= v].index))
                t0 = since("da", pd.Timestamp(tom), hit)
                alert("da_high", 0, "price", f"Day-ahead prices {'today' if tom == now.date() else 'tomorrow'}: Zone {ZN[z].split()[0]} ${v:,.0f}/MWh avg",
                      f"Day-ahead prices for {tom:%a %b %-d} average ${v:,.0f}/MWh in Zone {ZN[z]}. {len(hi)} zone(s) are above their 99th percentile of the past three years. Highest day-ahead daily average in that zone since {ago(t0, now)}.", pd.Timestamp(tom))
    # ---- load forecast heads-up (NYISO's forecast for the next several days)
    fcs = [f for f in (day("isolf", now.date()), day("isolf", now.date() + dt.timedelta(1))) if f is not None and len(f)]
    if fcs:
        fc = pd.concat(fcs).drop_duplicates("ts", keep="last")
        fc = fc[fc.ts.dt.date >= now.date()]
        pkd = fc.groupby(fc.ts.dt.date).v.max()
        thr = lambda D: 23000 if D.month in (11, 12, 1, 2, 3) else 29000 if D.month in (5, 6, 7, 8, 9) else None
        hot = {D: v for D, v in pkd.items() if thr(D) and v > thr(D)}
        if hot:
            D = max(hot, key=hot.get); v = hot[D]; last = st.get("fc_last")
            fresh = not last or now - pd.Timestamp(last[2]) > pd.Timedelta(hours=72) or v >= 1.03 * last[1]
            if fresh:
                st["fc_last"] = [str(D), float(v), now.isoformat(timespec="minutes")]
                def hit(df, thr_=v):
                    h = nyca_hourly(df); return h[h.v >= thr_].h
                t0 = since("load", now, hit, dt.date(2001, 6, 1))
                win = D.month in (11, 12, 1, 2, 3); rk = "nyca_winter_load_h" if win else "nyca_load_h"
                near = v >= 0.98 * R[rk][0]
                alert("fcst_load", 0, "load", f"Forecast: NYCA load {mw(v)} on {D:%a %b %-d}",
                      f"NYISO's load forecast shows statewide demand peaking near {mw(v)} on {D:%a %b %-d} ({'today' if D == now.date() else str((D - now.date()).days) + ' day(s) out'}). If it holds, that would be the highest actual load since {ago(t0, now)}. The {'winter' if win else 'all-time'} record is {mw(R[rk][0])} ({pd.Timestamp(R[rk][1]):%b %Y}). Forecasts several days out can shift.",
                      pd.Timestamp(D), "major" if near else "notable")
    # ---- summary for judgment
    summ = {}
    if rt is not None and len(rh): summ["max_hourly_lbmp"] = {ZN[z].split()[0]: round(v) for z, v in rh.groupby("zone").v.max().items()}
    if F is not None and len(gh): summ["gas_share_range"] = [round(gh.v.min(), 3), round(gh.v.max(), 3)]
    if data["load"] is not None and len(nh): summ["nyca_load_peak"] = round(nh.v.max())
    st["cursor"] = max(end, cur).isoformat(timespec="minutes")
    for k in list(cool):
        if not k.startswith("cons:") and now - pd.Timestamp(cool[k]) > pd.Timedelta(days=3): del cool[k]
    json.dump(st, open(sp, "w"), separators=(",", ":"))
    try:
        today = now.date()
        daily = {"asof": now.isoformat(timespec="minutes"), "today": today_ahead(today), "yesterday": day_stats(today - dt.timedelta(1), "Yesterday", end)}
    except Exception as ex:
        daily = {"asof": now.isoformat(timespec="minutes"), "error": str(ex)[:200]}
    json.dump({"window": [str(cur), str(end)], "alerts": alerts, "watch": watch[:8], "summary": summ, "daily": daily}, open(op, "w"), indent=1)
    print(json.dumps({"alerts": len(alerts), "watch": len(watch), "window": [str(cur), str(end)]}))

if __name__ == "__main__":
    a = sys.argv[1:]
    now = pd.Timestamp(a[a.index("--now") + 1]) if "--now" in a else pd.Timestamp.now(tz="America/New_York").tz_localize(None)
    main(a[0], a[1], a[2], now)
