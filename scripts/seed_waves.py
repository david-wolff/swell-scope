#!/usr/bin/env python3
import math, random, sqlite3
from datetime import datetime, timedelta

DB = "swellscope.db"
LOCATION = "Leme-RJ"
SOURCE = "mock"

def clamp(v, a, b): return max(a, min(b, v))

def gen_series(start_dt, end_dt, step=timedelta(hours=1)):
    rng = random.Random(42)
    t = start_dt
    base_hs, seas_amp = 1.2, 0.5
    num_swells = int(((end_dt - start_dt).days / 365) * 10)
    swell_centers = [start_dt + timedelta(days=rng.uniform(0, (end_dt - start_dt).days)) for _ in range(num_swells)]
    while t <= end_dt:
        doy = t.timetuple().tm_yday
        season = math.cos((doy - 200) / 365 * 2 * math.pi)
        hs = base_hs + seas_amp * (-season)
        for c in swell_centers:
            dt_days = abs((t - c).total_seconds()) / 86400.0
            hs += 1.5 * math.exp(-(dt_days**2) / (2*1.2**2))
        hs += rng.gauss(0, 0.15); hs = clamp(hs, 0.2, 5.5)
        tp = clamp(6 + hs*2.5 + rng.gauss(0, 0.6), 5.0, 20.0)
        dp = (150 + 20*math.sin(t.timestamp()/ (3600*24*10)) + rng.gauss(0,6)) % 360
        sst = clamp(22 - 3*season + rng.gauss(0,0.4), 16, 30)
        air = clamp(24 - 5*season + rng.gauss(0,0.8), 12, 36)
        wind_speed = clamp(rng.gauss(4, 1.5) + (hs-1.2)*0.7, 0, 18)
        wind_dir = (220 + 30*math.sin(t.timestamp()/ (3600*24*5)) + rng.gauss(0,10)) % 360
        yield t, hs, tp, dp, sst, air, wind_speed, wind_dir
        t += step

def main():
    con = sqlite3.connect(DB)
    con.execute("""
    CREATE TABLE IF NOT EXISTS waverecord (
      id INTEGER PRIMARY KEY,
      ts TEXT NOT NULL,     -- 'YYYY-MM-DD HH:MM:SS' (local)
      hs REAL, tp REAL, dp REAL,
      sst REAL, air_temp REAL,
      wind_speed REAL, wind_dir REAL,
      source TEXT, location TEXT
    );
    """)

    # 1) Dedup por ts (segurança)
    con.execute("""
      DELETE FROM waverecord
      WHERE rowid NOT IN (SELECT MIN(rowid) FROM waverecord GROUP BY ts)
    """)

    # 2) Índice ÚNICO em ts (necessário pro ON CONFLICT(ts))
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_waverecord_ts ON waverecord(ts)")

    end = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=365*2)

    rows = []
    for t, hs, tp, dp, sst, air, ws, wd in gen_series(start, end):
        ts = t.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        rows.append((ts, hs, tp, dp, sst, air, ws, wd, SOURCE, LOCATION))
        if len(rows) >= 5000:
            con.executemany("""
              INSERT INTO waverecord (ts, hs, tp, dp, sst, air_temp, wind_speed, wind_dir, source, location)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(ts) DO UPDATE SET
                hs=excluded.hs, tp=excluded.tp, dp=excluded.dp,
                sst=excluded.sst, air_temp=excluded.air_temp,
                wind_speed=excluded.wind_speed, wind_dir=excluded.wind_dir,
                source=excluded.source, location=excluded.location
            """, rows)
            rows.clear()
    if rows:
        con.executemany("""
          INSERT INTO waverecord (ts, hs, tp, dp, sst, air_temp, wind_speed, wind_dir, source, location)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(ts) DO UPDATE SET
            hs=excluded.hs, tp=excluded.tp, dp=excluded.dp,
            sst=excluded.sst, air_temp=excluded.air_temp,
            wind_speed=excluded.wind_speed, wind_dir=excluded.wind_dir,
            source=excluded.source, location=excluded.location
        """, rows)
    con.commit()
    con.close()
    print("Seed OK (2 anos de ondas).")

if __name__ == "__main__":
    main()
