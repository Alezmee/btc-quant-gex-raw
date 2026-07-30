"""
Descarga el snapshot crudo de opciones BTC en Deribit (OI, IV marcada,
greeks reportadas por Deribit, spot) y lo guarda particionado por fecha
en data/raw/YYYY-MM-DD/HHMMSS.parquet.

Este repo NO calcula GEX/DEX/regimen ni nada derivado — solo guarda el
dato crudo, para que el repo de calculo (btc-quant-gex) pueda
reconstruir cualquier metrica desde cero, las veces que haga falta,
incluso si la formula de calculo cambia mas adelante.

Uso:
    python3 guardar_crudo.py --max-instrumentos 100
"""

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://www.deribit.com/api/v2"
OUT_DIR = Path("./data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_instrumentos_btc_options():
    r = requests.get(
        f"{BASE_URL}/public/get_instruments",
        params={"currency": "BTC", "kind": "option", "expired": "false"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["result"]


def get_ticker(instrument_name):
    r = requests.get(
        f"{BASE_URL}/public/ticker",
        params={"instrument_name": instrument_name},
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("result")


def parse_instrument_name(nombre):
    """Formato Deribit: BTC-3JUL26-58000-C -> (vencimiento_str, strike, tipo)"""
    partes = nombre.split("-")
    return partes[1], float(partes[2]), ("C" if partes[3] == "C" else "P")


def deribit_fecha_a_timestamp(fecha_str):
    return pd.to_datetime(fecha_str, format="%d%b%y").tz_localize("UTC") + pd.Timedelta(hours=8)


def construir_dataset_crudo(max_instrumentos):
    instrumentos = get_instrumentos_btc_options()
    nombres = [i["instrument_name"] for i in instrumentos][:max_instrumentos]

    filas = []
    ahora = pd.Timestamp.now(tz="UTC")

    for idx, nombre in enumerate(nombres, 1):
        print(f"[{idx}/{len(nombres)}] {nombre}")
        try:
            t = get_ticker(nombre)
        except requests.RequestException as e:
            print(f"  aviso: no se pudo bajar {nombre} ({e})")
            continue

        if not t or t.get("open_interest") is None:
            continue

        vto_str, strike, tipo = parse_instrument_name(nombre)
        vto_dt = deribit_fecha_a_timestamp(vto_str)
        tiempo_anios = max((vto_dt - ahora).total_seconds(), 0) / (365 * 24 * 3600)
        greeks = t.get("greeks") or {}

        filas.append({
            "snapshot_ts": ahora,
            "instrument_name": nombre,
            "strike": strike,
            "tipo": tipo,
            "vencimiento": vto_dt,
            "tiempo_anios": tiempo_anios,
            "open_interest": t.get("open_interest", 0.0),
            "iv_marcada": (t.get("mark_iv") or 0.0) / 100.0,
            "gamma_actual": greeks.get("gamma", 0.0),
            "delta_actual": greeks.get("delta", 0.0),
            "vega_actual": greeks.get("vega", 0.0),
            "spot_subyacente": t.get("underlying_price") or t.get("index_price"),
        })
        time.sleep(0.05)

    return pd.DataFrame(filas)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-instrumentos", type=int, default=100)
    args = parser.parse_args()

    print("Descargando snapshot crudo de opciones BTC...")
    df = construir_dataset_crudo(args.max_instrumentos)

    if df.empty:
        print("No se obtuvieron datos. Revisa la conexion o el limite de instrumentos.")
        return

    ahora = datetime.now(timezone.utc)
    carpeta_dia = OUT_DIR / ahora.strftime("%Y-%m-%d")
    carpeta_dia.mkdir(parents=True, exist_ok=True)

    archivo = carpeta_dia / f"{ahora.strftime('%H%M%S')}.parquet"
    df.to_parquet(archivo, index=False)
    print(f"Snapshot crudo guardado en {archivo} ({len(df)} instrumentos)")


if __name__ == "__main__":
    main()
