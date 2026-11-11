"""Fontes de dados do VigiaVia usadas pela coleta (requisitos 01 e 02).

Fonte 1 (EXTERNA, REAL)  : Open-Meteo - chuva e temperatura horárias do centro de SP (Bela Vista).
                           Chuva é o principal gatilho de formação de buracos; serve para cruzar com os relatos.
Fonte 2 (SIMULADA)       : relatos de buracos do VigiaVia (mesmo formato do MVP: lat/lon, severidade, status).
                           Dados de demonstração, sinalizados com origem_dado = "demonstracao", gerados de forma
                           determinística (mesma data -> mesmo arquivo) para o pipeline ser reproduzível.

Justificativa da simulação: o MVP guarda os relatos em SQLite dentro do container e não expõe um feed histórico
de dados abertos; a base real de São Paulo (chamados SP156) tem dados pessoais e outro formato. O simulador abaixo
imita o contrato do MVP e inclui, de propósito, registros sujos (coordenada nula, severidade inválida e linha
duplicada) para as regras de qualidade da Prata terem o que barrar.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

LAT, LON = -23.5558, -46.6396  # Bela Vista, São Paulo
CIDADE = "São Paulo - Bela Vista"
N_LOCAIS = 24                   # pontos de buraco recorrentes (geram relatos duplicados por localização)
SEVERIDADES = ["baixa", "media", "alta"]
STATUS_FINAIS = ["confirmado", "descartado"]


def _rng(*partes) -> random.Random:
    chave = "|".join(str(p) for p in partes).encode()
    return random.Random(int(hashlib.sha256(chave).hexdigest()[:16], 16))


def _locais() -> list[dict]:
    """Pontos fixos (mesmos em qualquer dia) espalhados ~600 m ao redor do centro do piloto."""
    r = _rng("locais")
    return [
        {
            "via": f"Trecho demo {i + 1:02d} (Bela Vista)",
            "lat": round(LAT + r.uniform(-0.0055, 0.0055), 6),
            "lon": round(LON + r.uniform(-0.0055, 0.0055), 6),
        }
        for i in range(N_LOCAIS)
    ]


def _relatos_do_dia(d: date) -> list[dict]:
    r = _rng("relatos", d.isoformat())
    locais = _locais()
    saida = []
    for n in range(r.randint(14, 26)):
        loc = r.choice(locais)
        criado = datetime(d.year, d.month, d.day, r.randint(6, 21), r.randint(0, 59), r.randint(0, 59))
        origem = r.choices(["cidadao", "detector"], weights=[45, 55])[0]
        saida.append(
            {
                "id_relato": "R" + hashlib.sha1(f"{d}-{n}".encode()).hexdigest()[:10],
                "id_relator": f"dev-{r.randint(1, 40):03d}",  # identificador pseudônimo do aparelho
                "lat": round(loc["lat"] + r.uniform(-0.0002, 0.0002), 6),
                "lon": round(loc["lon"] + r.uniform(-0.0002, 0.0002), 6),
                "via": loc["via"],
                "severidade": r.choices(SEVERIDADES, weights=[40, 40, 20])[0],
                "confianca": round(r.uniform(0.35, 0.98), 2) if origem == "detector" else None,
                "origem": origem,
                "status": "pendente",
                "criado_em": criado.isoformat(),
                "atualizado_em": criado.isoformat(),
                "origem_dado": "demonstracao",
            }
        )
    return saida


def gerar_relatos_demo(ds: str) -> list[dict]:
    """Registros que 'chegam' no dia ds: relatos novos + atualizações de status de relatos do dia anterior
    (exercita o MERGE da Prata) + 3 registros sujos (exercitam as regras de qualidade)."""
    d = date.fromisoformat(ds)
    novos = _relatos_do_dia(d)
    r = _rng("atualizacoes", ds)
    atualizacoes = []
    for base in _relatos_do_dia(d - timedelta(days=1))[:5]:
        up = dict(base)
        up["status"] = r.choice(STATUS_FINAIS)
        up["atualizado_em"] = datetime(d.year, d.month, d.day, r.randint(7, 18), r.randint(0, 59), 0).isoformat()
        atualizacoes.append(up)

    sujos = []
    if novos:
        s1 = dict(novos[0]); s1["id_relato"] = "RSUJO" + ds.replace("-", "") + "A"; s1["lat"] = None
        s2 = dict(novos[1]); s2["id_relato"] = "RSUJO" + ds.replace("-", "") + "B"; s2["severidade"] = "XPTO"
        s3 = dict(novos[2])  # linha repetida: mesmo id_relato, mesmos valores
        sujos = [s1, s2, s3]
    return novos + atualizacoes + sujos
