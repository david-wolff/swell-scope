import requests
from datetime import datetime
from models import WaveRecord, get_session
from sqlmodel import select

LAT, LON = -22.9649, -43.1729
LOCATION = "Leme-RJ"
SOURCE = "open-meteo"

import requests
from datetime import datetime
from models import WaveRecord, get_session
from sqlmodel import select
from sqlalchemy import text  # necessário para rodar SQL bruto

LAT, LON = -22.9649, -43.1729
LOCATION = "Leme-RJ"
SOURCE = "open-meteo"

def run_collector():
    print("[START] Coletando dados do mar e da atmosfera...")

    # Chamada 1: dados oceânicos
    marine_url = (
        "https://marine-api.open-meteo.com/v1/marine"
        f"?latitude={LAT}&longitude={LON}"
        "&hourly=wave_height,wave_period,wave_direction,sea_surface_temperature"
        "&past_days=1&forecast_days=2&cell_selection=sea&timezone=UTC"
    )
    marine_r = requests.get(marine_url)
    if marine_r.status_code != 200:
        print("[ERROR] Marine API call failed:", marine_r.status_code, marine_r.text)
        return
    marine_data = marine_r.json().get("hourly")
    if not marine_data:
        print("[ERROR] Marine API response missing 'hourly'")
        return

    # Chamada 2: dados atmosféricos
    meteo_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        "&hourly=temperature_2m,wind_speed_10m,wind_direction_10m"
        "&past_days=1&forecast_days=2&timezone=UTC"
    )
    meteo_r = requests.get(meteo_url)
    if meteo_r.status_code != 200:
        print("[ERROR] Meteo API call failed:", meteo_r.status_code, meteo_r.text)
        return
    meteo_data = meteo_r.json().get("hourly")
    if not meteo_data:
        print("[ERROR] Meteo API response missing 'hourly'")
        return

    inserted = 0
    with get_session() as session:
        for i in range(len(marine_data["time"])):
            ts = datetime.fromisoformat(marine_data["time"][i])

            # Verifica duplicata
            existing = session.exec(
                select(WaveRecord).where(
                    WaveRecord.ts == ts,
                    WaveRecord.source == SOURCE,
                    WaveRecord.location == LOCATION
                )
            ).first()

            if existing:
                continue

            session.add(WaveRecord(
                ts=ts,
                hs=marine_data["wave_height"][i],
                tp=marine_data["wave_period"][i],
                dp=marine_data["wave_direction"][i],
                sst=marine_data["sea_surface_temperature"][i],
                air_temp=meteo_data["temperature_2m"][i],
                wind_speed=meteo_data["wind_speed_10m"][i],
                wind_dir=meteo_data["wind_direction_10m"][i],
                source=SOURCE,
                location=LOCATION,
            ))
            inserted += 1

        session.commit()

        # 🚿 Limpeza de duplicatas no final
        session.exec(text("""
            DELETE FROM waverecord
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM waverecord
                GROUP BY ts, source, location
            );
        """))
        session.commit()

    print(f"[OK] Inseridos {inserted} novos registros no banco (duplicatas removidas).")
