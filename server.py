"""
Servidor MCP remoto para Apple Health.

A diferencia del de Garmin, este servidor NO se conecta directamente a
Apple (no existe esa opción: Apple Health no tiene API en la nube). En su
lugar:

  1. La app "Health Auto Export" en el iPhone envía los datos de Salud
     aquí, al endpoint /ingest, con una clave secreta (Automatización ->
     REST API, formato JSON).
  2. Este servidor agrupa esos datos por día y los guarda en Upstash (una
     base de datos gratuita en la nube), para que sobrevivan aunque
     Render reinicie el servicio.
  3. Expone herramientas MCP que leen esos datos guardados, para que
     Claude pueda consultarlos.

Formato esperado en /ingest (el que envía Health Auto Export):
  {
    "data": {
      "metrics": [
        {"name": "step_count", "units": "count",
         "data": [{"qty": 1234, "date": "2026-09-21 08:00:00 +0200"}]},
        ...
      ],
      "workouts": [
        {"name": "Running", "start": "2026-09-21 07:00:00 +0200", ...},
        ...
      ]
    }
  }

Variables de entorno necesarias (en Render, no en este archivo):
  - INGEST_SECRET             -> clave que debe traer el envío para poder
                                  guardar datos (evita que cualquiera te
                                  rellene la base de datos)
  - UPSTASH_REDIS_REST_URL    -> de tu cuenta de Upstash
  - UPSTASH_REDIS_REST_TOKEN  -> de tu cuenta de Upstash
  - PORT                      -> la pone Render automáticamente
"""

import os
import json
import datetime
from typing import Optional

import requests
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

mcp = FastMCP(
    "apple-health-mcp",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)

# --- Almacenamiento en Upstash (Redis gratuito en la nube) ---------------


def _upstash_url() -> Optional[str]:
    return os.environ.get("UPSTASH_REDIS_REST_URL")


def _upstash_token() -> Optional[str]:
    return os.environ.get("UPSTASH_REDIS_REST_TOKEN")


def _upstash_set(key: str, value: dict):
    url = _upstash_url()
    token = _upstash_token()
    if not url or not token:
        raise RuntimeError(
            "Faltan UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN en el hosting."
        )
    resp = requests.post(
        f"{url}/set/{key}",
        headers={"Authorization": f"Bearer {token}"},
        data=json.dumps(value),
        timeout=10,
    )
    resp.raise_for_status()


def _upstash_get(key: str) -> Optional[dict]:
    url = _upstash_url()
    token = _upstash_token()
    if not url or not token:
        raise RuntimeError(
            "Faltan UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN en el hosting."
        )
    resp = requests.get(
        f"{url}/get/{key}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    result = data.get("result")
    if result is None:
        return None
    return json.loads(result)


def _today() -> str:
    return datetime.date.today().isoformat()


def _day_from_timestamp(ts: str) -> str:
    """Extrae 'YYYY-MM-DD' de un timestamp tipo '2026-09-21 08:00:00 +0200'."""
    return (ts or "").strip()[:10]


# --- Endpoint que recibe los datos de Health Auto Export -----------------


@mcp.custom_route("/ingest", methods=["POST"])
async def ingest(request: Request) -> JSONResponse:
    secret = os.environ.get("INGEST_SECRET")
    provided = request.headers.get("x-api-key")
    if not secret or provided != secret:
        return JSONResponse({"error": "no autorizado"}, status_code=401)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)

    # Soporta tanto el formato real de Health Auto Export ({"data": {...}})
    # como un envío simple directo ({"metrics": ..., "workouts": ...}).
    data = payload.get("data", payload)
    metrics = data.get("metrics", []) or []
    workouts = data.get("workouts", []) or []

    days: dict = {}

    def bucket_for(day: str) -> dict:
        return days.setdefault(day, {"date": day, "metrics": {}, "workouts": []})

    for metric in metrics:
        name = metric.get("name", "unknown")
        for point in metric.get("data", []) or []:
            ts = point.get("date", "")
            day = _day_from_timestamp(ts)
            if not day:
                continue
            b = bucket_for(day)
            b["metrics"].setdefault(name, []).append(point)

    for workout in workouts:
        ts = (
            workout.get("start")
            or workout.get("startDate")
            or workout.get("date")
            or ""
        )
        day = _day_from_timestamp(ts)
        if not day:
            continue
        bucket_for(day)["workouts"].append(workout)

    if not days:
        # Nada reconocible en el payload; guarda igualmente bajo hoy, para
        # poder inspeccionarlo si algo viene con un formato distinto.
        days[_today()] = {"date": _today(), "raw": payload}

    saved_dates = []
    for day, bucket in days.items():
        key = f"healthdata:{day}"
        try:
            existing = _upstash_get(key)
        except Exception:
            existing = None
        if isinstance(existing, dict) and "metrics" in existing:
            merged_metrics = existing.get("metrics", {})
            for name, points in bucket["metrics"].items():
                merged_metrics.setdefault(name, [])
                merged_metrics[name].extend(points)
            bucket["metrics"] = merged_metrics
            bucket["workouts"] = (existing.get("workouts") or []) + bucket["workouts"]
        try:
            _upstash_set(key, bucket)
            saved_dates.append(day)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse({"status": "guardado", "dates": saved_dates})


# --- Herramientas expuestas a Claude -------------------------------------


@mcp.tool()
def get_daily_summary(date: str = "") -> dict:
    """Devuelve los datos de Apple Health de un día (formato YYYY-MM-DD),
    tal como los exporta Health Auto Export: un diccionario 'metrics' con
    una lista de lecturas (qty + fecha) por cada tipo de métrica (steps,
    heart_rate, sleep_analysis, active_energy, etc.), y una lista
    'workouts' con los entrenamientos de ese día. Si una métrica tiene
    varias lecturas ese día, hay que sumarlas o hacer la media según
    corresponda (sumar para cosas acumulativas como pasos o calorías,
    media para cosas como frecuencia cardiaca).
    Si no se indica fecha, usa el día de hoy. Si no hay datos para ese
    día, puede que la app Health Auto Export no haya exportado todavía."""
    d = date or _today()
    data = _upstash_get(f"healthdata:{d}")
    if data is None:
        return {
            "date": d,
            "status": "sin_datos",
            "nota": "No hay datos guardados para este día todavía.",
        }
    return data


@mcp.tool()
def get_range_summary(start_date: str, end_date: str) -> list:
    """Devuelve los resúmenes diarios de Apple Health entre dos fechas
    (formato YYYY-MM-DD), ambas incluidas."""
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    results = []
    day = start
    while day <= end:
        d = day.isoformat()
        data = _upstash_get(f"healthdata:{d}")
        if data is not None:
            results.append(data)
        day += datetime.timedelta(days=1)
    return results


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http")
