"""
Servidor MCP remoto para Apple Health.

A diferencia del de Garmin, este servidor NO se conecta directamente a
Apple (no existe esa opción: Apple Health no tiene API en la nube). En su
lugar:

  1. Un Atajo (Shortcut) en el iPhone lee los datos de Salud y los envía
     aquí, al endpoint /ingest, con una clave secreta.
  2. Este servidor guarda esos datos en Upstash (una base de datos gratuita
     en la nube), para que sobrevivan aunque Render reinicie el servicio.
  3. Expone herramientas MCP que leen esos datos guardados, para que Claude
     pueda consultarlos.

Variables de entorno necesarias (en Render, no en este archivo):
  - INGEST_SECRET             -> clave que debe traer el Atajo para poder
                                  enviar datos (evita que cualquiera te
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


# --- Endpoint que recibe los datos del Atajo de iPhone -------------------


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

    date = payload.get("date") or _today()
    try:
        _upstash_set(f"healthdata:{date}", payload)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse({"status": "guardado", "date": date})


# --- Herramientas expuestas a Claude -------------------------------------


@mcp.tool()
def get_daily_summary(date: str = "") -> dict:
    """Devuelve el resumen de datos de Apple Health de un día (formato
    YYYY-MM-DD): pasos, sueño, frecuencia cardiaca, HRV y entrenamientos.
    Si no se indica fecha, usa el día de hoy. Si no hay datos para ese día,
    puede que el Atajo del iPhone no se haya ejecutado todavía."""
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
