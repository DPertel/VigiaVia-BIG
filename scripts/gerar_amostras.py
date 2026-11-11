"""Gera amostras LOCAIS em amostras/ para testar o pipeline sem Airflow.

ATENÇÃO: o clima aqui é SINTÉTICO (mesmo formato do Open-Meteo). Os dados reais vêm do DAG (requisito 01).
Uso: python scripts/gerar_amostras.py [--inicio 2026-11-01] [--dias 5]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))
from vigiavia_fontes import LAT, LON, _rng, gerar_relatos_demo  # noqa: E402


def clima_sintetico(ds: str) -> dict:
    r = _rng("clima", ds)
    chuva_dia = r.random() < 0.4
    horas = [f"{ds}T{h:02d}:00" for h in range(24)]
    temp = [round(17 + 7 * max(0, 1 - abs(h - 15) / 9) + r.uniform(-0.8, 0.8), 1) for h in range(24)]
    prec = [round(r.uniform(0, 4), 1) if chuva_dia and 13 <= h <= 19 and r.random() < 0.6 else 0.0 for h in range(24)]
    if ds.endswith("-03"):
        prec[5] = None  # um nulo de propósito para a regra de qualidade da Prata
    return {
        "latitude": LAT, "longitude": LON, "generationtime_ms": 0.5, "utc_offset_seconds": -10800,
        "timezone": "America/Sao_Paulo", "timezone_abbreviation": "GMT-3", "elevation": 760.0,
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm"},
        "hourly": {"time": horas, "temperature_2m": temp, "precipitation": prec},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", default="2026-11-01")
    ap.add_argument("--dias", type=int, default=5)
    a = ap.parse_args()
    ini = date.fromisoformat(a.inicio)
    for i in range(a.dias):
        ds = (ini + timedelta(days=i)).isoformat()
        for fonte, dados in (("clima", clima_sintetico(ds)), ("relatos", gerar_relatos_demo(ds))):
            p = RAIZ / "amostras" / fonte / f"{fonte}_{ds}.json"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(dados, ensure_ascii=False, indent=None), encoding="utf-8")
            print("gerado", p.relative_to(RAIZ))


if __name__ == "__main__":
    main()
