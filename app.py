from __future__ import annotations
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from sqlmodel import select, and_
from typing import Optional, List
from datetime import datetime, timezone, time, timedelta
import requests
from zoneinfo import ZoneInfo
from scheduler import start_scheduler
from models import create_db_and_tables, get_session, WaveRecord
from collector import run_collector

LAT, LON = -22.9649, -43.1729

app = FastAPI(title="Swell Scope PoC")

@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    run_collector()
    start_scheduler()
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!doctype html>
    <html lang="pt-br">
      <head><meta charset="utf-8"><title>Swell Scope PoC</title></head>
      <body style="font-family: system-ui, -apple-system, Segoe UI, Roboto; padding: 24px">
        <h1>Swell Scope PoC</h1>
        <ul>
          <li><a href="/health">/health</a></li>
          <li><a href="/waves/">/waves/</a> (do SQLite)</li>
          <li><a href="/tides/">/tides/</a> (online)</li>
          <li><a href="/debug/seed">/debug/seed</a> (popular 3 ondas de teste)</li>
          <li><a href="/debug/stats">/debug/stats</a> (diagnóstico rápido)</li>
          <li><a href="/dashboard">/dashboard</a> (gráficos)</li>
          <li><a href="/docs">/docs</a> (Swagger)</li>
        </ul>
      </body>
    </html>
    """

@app.get("/waves/")
def get_wave_data(
    start: Optional[str] = Query(None, description="Formato ISO ex: 2025-08-24T00:00:00"),
    end: Optional[str] = Query(None, description="Formato ISO ex: 2025-08-24T23:59:59")
):
    try:
        if start:
            dt_start = datetime.fromisoformat(start)
        else:
            today_utc = datetime.now(timezone.utc).date()
            dt_start = datetime.combine(today_utc, time(0), tzinfo=timezone.utc)

        dt_end = datetime.fromisoformat(end) if end else None
    except ValueError:
        raise HTTPException(status_code=400, detail="Parâmetros de data inválidos")

    with get_session() as session:
        stmt = select(WaveRecord).where(WaveRecord.ts >= dt_start)
        if dt_end:
            stmt = stmt.where(WaveRecord.ts <= dt_end)
        stmt = stmt.order_by(WaveRecord.ts.asc())
        rows: List[WaveRecord] = list(session.exec(stmt))

    return JSONResponse({
        "items": [
            {
                "ts": row.ts.isoformat(),
                "hs": row.hs,
                "tp": row.tp,
                "dp": row.dp,
                "sst": row.sst,
                "air_temp": row.air_temp,
                "wind_speed": row.wind_speed,
                "wind_dir": row.wind_dir,
                "source": row.source,
                "location": row.location,
            }
            for row in rows
        ]
    })

@app.get("/waves/summary")
def waves_summary():
    today_utc = datetime.now(timezone.utc).date()
    dt_start = datetime.combine(today_utc, time(0), tzinfo=timezone.utc)
    dt_end = datetime.combine(today_utc, time(23, 59), tzinfo=timezone.utc)

    with get_session() as s:
        stmt = select(WaveRecord).where(
            WaveRecord.ts >= dt_start,
            WaveRecord.ts <= dt_end,
            WaveRecord.source == "open-meteo"
        )
        rows = list(s.exec(stmt))

    if not rows:
        return JSONResponse({"date": str(today_utc), "summary": None})

    def avg(values):
        valid = [v for v in values if v is not None]
        return round(sum(valid) / len(valid), 2) if valid else None

    summary = {
        "date": str(today_utc),
        "hs_avg": avg([r.hs for r in rows]),
        "tp_avg": avg([r.tp for r in rows]),
        "dp_avg": avg([r.dp for r in rows]),
        "sst_avg": avg([r.sst for r in rows]),
        "air_temp_avg": avg([r.air_temp for r in rows]),
        "wind_speed_avg": avg([r.wind_speed for r in rows]),
        "wind_dir_avg": avg([r.wind_dir for r in rows]),
    }

    return JSONResponse(summary)



@app.get("/tides/")
def get_tides(start: Optional[datetime] = None, end: Optional[datetime] = None):
    API_KEY = "10f2a39e-e464-11ed-a26f-0242ac130002-10f2a42a-e464-11ed-a26f-0242ac130002"
    LOCAL_TZ = ZoneInfo("America/Sao_Paulo")

    # --- helper: garante datetime c/ timezone local (se vier naive, assume local) ---
    def to_local(dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            # interpretamos valores sem timezone como hora local
            return dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)

    today = datetime.now(tz=LOCAL_TZ).date()
    day_after_tomorrow = today + timedelta(days=2)

    # janela pedida à Stormglass (pode ajustar se quiser mais dias)
    params = {
        "lat": LAT,
        "lng": LON,
        "start": today.isoformat(),              # API aceita datas ISO (YYYY-MM-DD)
        "end": day_after_tomorrow.isoformat(),   # cobrimos hoje + amanhã
        "height": "true",
        "type": "true",
    }

    headers = {"Authorization": API_KEY}
    url = "https://api.stormglass.io/v2/tide/extremes/point"

    response = requests.get(url, params=params, headers=headers, timeout=20)
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="Erro ao consultar a Stormglass")

    data = response.json().get("data", [])
    if not data:
        return {"items": []}

    # limites de filtro em horário local (sempre tz-aware)
    start_local = to_local(start) or datetime.combine(today, time.min, tzinfo=LOCAL_TZ)
    end_local   = to_local(end)   or datetime.combine(day_after_tomorrow, time.max, tzinfo=LOCAL_TZ)

    result = []
    for item in data:
        # item["time"] vem em UTC (ex.: "2025-08-27T07:32:00+00:00" ou "…Z")
        raw = item.get("time")
        if not raw:
            continue
        try:
            ts_aware_utc = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            # fallback simples (sem microssegundos); ajuste o formato se necessário
            ts_aware_utc = datetime.strptime(raw.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")

        ts_local = ts_aware_utc.astimezone(LOCAL_TZ)

        # agora a comparação é entre tz-aware no mesmo fuso
        if start_local <= ts_local <= end_local:
            result.append({
                "ts": ts_local.isoformat(),           # devolvendo em horário local
                "type": item.get("type"),
                "height": item.get("height"),
                "source": "stormglass",
                "location": "Leme-RJ",
            })

    return {"items": result}

@app.get("/debug/seed")
def debug_seed():
    samples = [
        {"ts": "2025-08-24T00:00:00", "hs": 1.6, "tp": 12, "dp": 180},
        {"ts": "2025-08-24T01:00:00", "hs": 1.7, "tp": 13, "dp": 185},
        {"ts": "2025-08-24T02:00:00", "hs": 1.5, "tp": 11, "dp": 190},
    ]
    with get_session() as s:
        for x in samples:
            s.add(WaveRecord(
                ts=datetime.fromisoformat(x["ts"]),
                hs=x["hs"], tp=x["tp"], dp=x["dp"],
                source="seed", location="Leme-RJ"
            ))
        s.commit()
    return {"ok": True, "inserted": len(samples)}

@app.get("/debug/stats")
def debug_stats():
    with get_session() as s:
        total = list(s.exec(select(WaveRecord)))
        last = s.exec(select(WaveRecord).order_by(WaveRecord.ts.desc())).first()
    return {
        "count": len(total),
        "last_ts": last.ts.isoformat() if last else None,
        "last_sample": (
            {
                "hs": last.hs, "tp": last.tp, "dp": last.dp,
                "sst": last.sst, "air_temp": last.air_temp,
                "wind_speed": last.wind_speed, "wind_dir": last.wind_dir
            } if last else None
        )
    }

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    with get_session() as s:
        rows = list(s.exec(select(WaveRecord).order_by(WaveRecord.ts.asc())))
    times = [r.ts.isoformat() for r in rows]
    hs = [r.hs for r in rows]
    tp = [r.tp for r in rows]
    tair = [r.air_temp for r in rows]
    sst = [r.sst for r in rows]

    return f"""
<!doctype html>
<html lang="pt-br">
  <head>
    <meta charset="utf-8" />
    <title>Swell Scope – Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
    <style>
      body{{font-family:system-ui,-apple-system,Segoe UI,Roboto; padding:24px; background:#0b1117; color:#e5e7eb}}
      .grid{{display:grid; gap:16px}}
      @media(min-width:900px){{.grid{{grid-template-columns:1fr 1fr}}}}
      .card{{background:#111827; border:1px solid #1f2937; border-radius:12px; padding:12px}}
      h2{{margin:6px 0 12px; font-size:16px; color:#cbd5e1}}
      canvas{{width:100%; height:340px}}
    </style>
  </head>
  <body>
    <h1>Swell Scope – Dashboard</h1>
    <div class="grid">
      <div class="card">
        <h2>Ondas: Hs (m) e Tp (s)</h2>
        <canvas id="waves"></canvas>
      </div>
      <div class="card">
        <h2>Temperaturas: Ar × Mar (°C)</h2>
        <canvas id="temps"></canvas>
      </div>
    </div>
    <script>
      const times = {times};
      const hs = {hs};
      const tp = {tp};
      const tair = {tair};
      const sst = {sst};

      function mkChart(id, labelA, dataA, labelB, dataB) {{
        const ctx = document.getElementById(id);
        new Chart(ctx, {{
          type: 'line',
          data: {{
            labels: times,
            datasets: [
              {{ label: labelA, data: dataA, spanGaps: true }},
              {{ label: labelB, data: dataB, spanGaps: true }}
            ]
          }},
          options: {{
            responsive: true,
            interaction: {{ mode: 'index', intersect: false }},
            scales: {{
              x: {{ ticks: {{ maxRotation: 0, autoSkip: true }} }}
            }}
          }}
        }});
      }}

      mkChart('waves', 'Hs (m)', hs, 'Tp (s)', tp);
      mkChart('temps', 'Ar (°C)', tair, 'Mar (°C)', sst);
    </script>
  </body>
</html>
"""
