# NYISO Watch

Monitors NYISO's public data (mis.nyiso.com) and logs rare, notable grid events and a daily summary to the NYISO Watch dashboard on claude.ai.

## How it runs

A Claude scheduled task runs three times a day (8:44am, 1:44pm, 6:44pm New York time). Each run clones this repository, runs `detect.py`, and writes alerts, the daily summary, and the detector's state to the dashboard's storage.

```
python3 detect.py state.json calib.json out.json [--now 'YYYY-MM-DD HH:MM']
```

`--now` replays a past moment, which is how the triggers were tested against historical events.

## Files

- `detect.py` — the detector: alert triggers, record tracking, "last time this happened" lookbacks, and the daily summary.
- `calib.json` — thresholds calibrated from NYISO's archive (2023–2026 percentiles and day-ahead 99th percentiles by zone).
- `dashboard/index.html` — the dashboard page (a copy; the live page is published on claude.ai).
- `tools/` — one-time scripts used to download NYISO's monthly archives back to 1999, parse them, and compute records and calibration.

## Changing the detector

The scheduled task checks that `detect.py` matches an expected checksum before running it. After editing `detect.py`, update that checksum in the scheduled task's instructions, or the run will stop and report a mismatch.
