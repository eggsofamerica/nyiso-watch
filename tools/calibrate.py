"""Compute records and percentile tables from the deep archive -> calib.json, state_seed.json"""
import duckdb, json, pandas as pd
c = duckdb.connect("/home/claude/nyiso/nyiso.duckdb")
Z = ('CAPITL','CENTRL','DUNWOD','GENESE','HUD VL','LONGIL','MHK VL','MILLWD','N.Y.C.','NORTH','WEST')
q = lambda s: c.sql(s).df()

def rec(sql, mode="max"):
    df = q(sql).dropna()
    r = df.loc[df.v.idxmax() if mode == "max" else df.v.idxmin()]
    return [round(float(r.v), 3), pd.Timestamp(r.h).strftime("%Y-%m-%dT%H:%M")]

c.sql("""create or replace table chpe_d as select cast(ts as date)::timestamp h, avg(flow) v from flows
         where iface='SCH - HQ_CHPE' group by 1 having count(*)>=200""")
records = {
  "nyca_load_h": rec("select h, load v from nyca_h where n>=6"),
  "nyca_winter_load_h": rec("select h, load v from nyca_h where n>=6 and month(h) in (11,12,1,2,3)"),
  "nyc_load_h": rec("select h, nyc v from nyc_h"),
  "midday_low_load_h": rec("select h, load v from nyca_h where n>=6 and hour(h) between 10 and 15 and h>='2015-01-01' and load>7000", "min"),
  "wind_h": rec("select h, wind v from fuel_h"),
  "wind_d": rec("select cast(h as date)::timestamp h, sum(wind) v from fuel_h group by 1 having count(*)>=22"),
  "oren_h": rec("select h, oren v from fuel_h"),
  "btm_h": rec("select h, btm v from btm_h"),
  "ws_h": rec("select f.h, wind+oren+btm v from fuel_h f join btm_h b using(h)"),
  "zc_share_h": rec("select h, (wind+hydro+nuc+oren)/tot v from fuel_h where h>='2021-05-01'"),
  "zc_share_d": rec("select cast(h as date)::timestamp h, sum(wind+hydro+nuc+oren)/sum(tot) v from fuel_h where h>='2021-05-01' group by 1 having count(*)>=22"),
  "chpe_d": rec("select h, v from chpe_d"),
}

# DA p99 by zone (3y) for next-day preview trigger
da_p99 = {r.zone: round(float(r.p), 1) for r in q(f"""select zone, quantile_cont(lbmp,0.99) p from da_d
          where d>='2023-11-21' and zone in {Z} group by 1""").itertuples()}

# Watch-list percentiles by month (3y window): p0.5 / p99.5 of hourly measures
c.sql(f"""create or replace table rt_hw as select h, max(case when zone='N.Y.C.' then lbmp end) - max(case when zone='WEST' then lbmp end) ja,
          max(case when zone='N.Y.C.' then cong end) jcong from
          (select date_trunc('hour', ts - interval 1 second) h, zone, avg(lbmp) lbmp, avg(cong) cong from rt5
           where ts>='2023-11-21' and zone in ('N.Y.C.','WEST') group by 1,2) group by 1""")
c.sql("""create or replace table da_rt as select r.h, max(r.lbmp - d.lbmp) gap_max, min(r.lbmp - d.lbmp) gap_min from rt_h r
         join da d on d.ts = r.h and d.zone = r.zone where r.h>='2023-11-21' group by 1""")
c.sql("""create or replace table hq_h as select date_trunc('hour', ts - interval 1 second) h, sum(flow) v from
         (select ts, avg(flow) flow from flows where ts>='2023-11-21' and iface in ('SCH - HQ - NY','SCH - HQ_CHPE','SCH - HQ_CEDARS') group by ts, iface) group by 1""")
measures = {
  "ja_spread": "select h, ja v from rt_hw",
  "j_congestion": "select h, -jcong v from rt_hw",
  "rt_minus_da_max": "select h, gap_max v from da_rt",
  "rt_minus_da_min": "select h, gap_min v from da_rt",
  "gas_share": "select h, gas/tot v from fuel_h where h>='2023-11-21'",
  "hydro": "select h, hydro v from fuel_h where h>='2023-11-21'",
  "nuclear": "select h, nuc v from fuel_h where h>='2023-11-21'",
  "nyca_load": "select h, load v from nyca_h where h>='2023-11-21' and n>=6",
  "wind": "select h, wind v from fuel_h where h>='2023-11-21'",
}
pct = {}
for name, sql in measures.items():
    df = q(sql).dropna()
    df["m"] = pd.to_datetime(df.h).dt.month
    g = df.groupby("m").v.quantile([0.005, 0.995]).unstack()
    pct[name] = {int(m): [round(float(a), 1 if abs(a) > 1 else 4), round(float(b), 1 if abs(b) > 1 else 4)] for m, (a, b) in g.iterrows()}

calib = {"da_p99": da_p99, "watch": pct, "built": "2026-11-21"}
json.dump(calib, open("/home/claude/nyiso/calib.json", "w"), separators=(",", ":"))
json.dump({"records": records, "cursor": None, "cooldown": {}, "da_done": None, "daily_done": None},
          open("/home/claude/nyiso/state_seed.json", "w"), indent=0)
print(json.dumps(records, indent=1)); print(da_p99)
print(len(json.dumps(calib, separators=(",", ":"))), "bytes calib")
