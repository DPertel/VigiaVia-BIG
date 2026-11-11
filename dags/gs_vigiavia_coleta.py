"""DAG de coleta do VigiaVia (requisitos 02 e 03).

- Agendado (@daily), com catchup para recuperar os dias desde start_date.
- Duas coletas em PARALELO (clima e relatos) -> uma tarefa que CONFERE se os arquivos chegaram.
- Um arquivo por coleta, com a data no nome, na pasta chegada/<fonte>/.
- Nenhuma chave no código: tudo em Airflow Variables (Admin > Variables). Nenhuma é obrigatória para rodar:
    VIGIAVIA_CHEGADA_DIR   pasta de pouso (padrão: <repo>/chegada)
    VIGIAVIA_ALERTA_URL    webhook (Teams/Slack/n8n) que recebe a notificação de falha (opcional)
    OPENMETEO_API_KEY      só se usar o plano comercial do Open-Meteo (opcional; a API gratuita não usa chave)
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pendulum
import requests

try:  # Airflow 3
    from airflow.sdk import DAG, Variable
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parent))
from vigiavia_fontes import CIDADE, LAT, LON, gerar_relatos_demo  # noqa: E402

log = logging.getLogger(__name__)

CHEGADA = Path(Variable.get("VIGIAVIA_CHEGADA_DIR", default=str(Path(__file__).resolve().parents[1] / "chegada")))
URL_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
URL_FORECAST = "https://api.open-meteo.com/v1/forecast"  # o archive tem ~5 dias de atraso; dias recentes vêm daqui


def _destino(fonte: str, ds: str) -> Path:
    p = CHEGADA / fonte / f"{fonte}_{ds}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _chuva_preenchida(payload: dict) -> bool:
    return any(v is not None for v in payload.get("hourly", {}).get("temperature_2m", []))


def coletar_clima(ds: str, **_):
    params = {
        "latitude": LAT, "longitude": LON, "start_date": ds, "end_date": ds,
        "hourly": "temperature_2m,precipitation", "timezone": "America/Sao_Paulo",
    }
    chave = Variable.get("OPENMETEO_API_KEY", default=None)
    if chave:
        params["apikey"] = chave
    r = requests.get(URL_ARCHIVE, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if not _chuva_preenchida(payload):  # dia recente demais para o archive: usa a API de previsão/histórico curto
        log.warning("archive sem dados para %s; tentando forecast", ds)
        r = requests.get(URL_FORECAST, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
    _destino("clima", ds).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.info("clima %s: %d horas", ds, len(payload.get("hourly", {}).get("time", [])))


def coletar_relatos(ds: str, **_):
    relatos = gerar_relatos_demo(ds)
    _destino("relatos", ds).write_text(json.dumps(relatos, ensure_ascii=False), encoding="utf-8")
    log.info("relatos %s: %d registros (%s)", ds, len(relatos), CIDADE)


def conferir_chegada(ds: str, **_):
    """Falha (e notifica) se algum arquivo do dia não chegou, está vazio ou não é JSON válido."""
    problemas = []
    for fonte in ("clima", "relatos"):
        arq = CHEGADA / fonte / f"{fonte}_{ds}.json"
        if not arq.exists() or arq.stat().st_size == 0:
            problemas.append(f"{arq.name}: ausente ou vazio")
            continue
        try:
            json.loads(arq.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            problemas.append(f"{arq.name}: JSON inválido ({e})")
    if problemas:
        raise RuntimeError("Conferência falhou: " + "; ".join(problemas))
    log.info("conferência ok para %s", ds)


def notificar_falha(contexto):
    msg = f"[VigiaVia] falha na tarefa {contexto['task_instance'].task_id} (execução {contexto.get('ds')})"
    log.error(msg)
    url = Variable.get("VIGIAVIA_ALERTA_URL", default=None)
    if url:
        try:
            requests.post(url, json={"text": msg}, timeout=10)
        except requests.RequestException as e:  # a notificação nunca pode derrubar o pipeline
            log.error("não foi possível enviar o alerta: %s", e)


with DAG(
    dag_id="gs_vigiavia_coleta",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2), "on_failure_callback": notificar_falha},
    tags=["gs", "vigiavia", "big-data"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    relatos = PythonOperator(task_id="coletar_relatos", python_callable=coletar_relatos)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> [clima, relatos] >> conferir
