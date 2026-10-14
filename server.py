"""
Servidor MCP remoto para Apple Health.

Apple Health no tiene API en la nube, así que:

  1. La app "Health Auto Export" en el iPhone envía los datos de Salud aquí,
     al endpoint /ingest, con una clave secreta (Automatización -> REST API,
     formato JSON).
  2. Este servidor agrupa esos datos por día y los guarda en Upstash (Redis
     gratuito en la nube), para que sobrevivan a los reinicios de Render.
  3. Expone herramientas MCP que leen esos datos YA AGREGADOS, para que
     Claude pueda consultarlos sin recibir miles de lecturas por minuto.

Novedades (v2):
  - /ingest ya NO duplica datos: cada lectura se identifica por fecha+fuente
    y la más nueva sustituye a la anterior (el sueño ya no sale repetido).
    Los entrenos se guardan compactos (resumen + parciales, sin series por
    minuto) y también se deduplican.
  - get_daily_summary / get_range_summary devuelven los datos agregados por
    día: SUMA para pasos, distancia, energía, pisos, minutos...; MEDIA/mín/máx
    para frecuencia cardiaca, HRV, SpO2... Con unidades. Si Health Auto Export
    manda el total diario (marca 00:00:00) se usa ese. El sueño sale
    deduplicado y los entrenos solo con resumen y parciales.
  - get_metric_by_hour permite bajar al detalle por horas de una métrica.
  - Funciona con los datos antiguos ya guardados (se agregan al leer).
  - La energía de Apple suele venir en kJ; se añade el equivalente en kcal.

Formato esperado en /ingest (el que envía Health Auto Export):
  {"data": {"metrics": [{"name": "step_count", "units": "count",
                          "data": [{"qty": 1234, "date": "2026-09-21 08:00:00 +0200"}]}],
            "workouts": [{"name": "Running", "start": "2026-09-21 07:00:00 +0200", ...}]}}

Variables de entorno necesarias (en Render, no en este archivo):
  - INGEST_SECRET             -> clave que debe traer el envío
  - UPSTASH_REDIS_REST_URL    -> de tu cuenta de Upstash
  - UPSTASH_REDIS_REST_TOKEN  -> de tu cuenta de Upstash
  - PORT                      -> la pone Render automáticamente
"""

import os
import json
import datetime
import traceback
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
        timeout=30,
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
        timeout=30,
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


# --- Utilidades de agregación --------------------------------------------

# Métricas acumulativas (se SUMAN). El resto (FC, HRV, SpO2, velocidades...)
# se resumen con media/mín/máx.
_CUMULATIVE_EXACT = {
    "step_count",
    "steps",
    "flights_climbed",
    "apple_exercise_time",
    "apple_stand_time",
    "apple_stand_hour",
    "time_in_daylight",
    "mindful_minutes",
    "swimming_stroke_count",
    "push_count",
    "number_of_times_fallen",
    "handwashing",
    "inhaler_usage",
    "uv_exposure",
}


def _is_cumulative(name: str) -> bool:
    n = (name or "").lower()
    return n in _CUMULATIVE_EXACT or "distance" in n or "energy" in n or n.startswith(
        "dietary_"
    )


def _is_midnight(p: dict) -> bool:
    """Entrada 'total diario' de Health Auto Export: marca 00:00:00."""
    return (p.get("date") or "")[11:19] == "00:00:00" and not p.get("hourly")


def _num(x) -> Optional[float]:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    return None


def _val(p: dict) -> Optional[float]:
    for k in ("qty", "Avg", "avg", "value"):
        v = _num(p.get(k))
        if v is not None:
            return v
    return None


def _min(p: dict) -> Optional[float]:
    v = _num(p.get("Min"))
    return v if v is not None else _val(p)


def _max(p: dict) -> Optional[float]:
    v = _num(p.get("Max"))
    return v if v is not None else _val(p)


def _r(x: Optional[float], nd: int = 2):
    return None if x is None else round(x, nd)


def _pkey(p: dict) -> tuple:
    """Identidad de una lectura, para no duplicarla al reenviar datos."""
    return (
        p.get("date"),
        p.get("source"),
        p.get("sleepStart"),
        p.get("sleepEnd"),
        p.get("inBedStart"),
        p.get("startDate"),
        p.get("endDate"),
        str(p.get("value")) if not isinstance(p.get("value"), (int, float)) else None,
        bool(p.get("hourly")),
    )


def _merge_points(old: list, new: list) -> list:
    """Une dos listas sin duplicar; si coinciden, gana la nueva."""
    merged: dict = {}
    for p in list(old or []) + list(new or []):
        if isinstance(p, dict):
            merged[_pkey(p)] = p
    return sorted(merged.values(), key=lambda p: str(p.get("date") or ""))


def _hourly(points: list, cumulative: bool) -> list:
    """Agrupa lecturas por hora (para adelgazar días muy grandes)."""
    groups: dict = {}
    for p in points:
        ts = p.get("date") or ""
        groups.setdefault(ts[:13], []).append(p)
    out = []
    for h, ps in sorted(groups.items()):
        tz = (ps[0].get("date") or "")[19:]
        date = f"{h}:00:00{tz}"
        vals = [v for v in map(_val, ps) if v is not None]
        if not vals:
            continue
        if cumulative:
            out.append({"date": date, "qty": round(sum(vals), 4), "hourly": True})
        else:
            mins = [m for m in map(_min, ps) if m is not None]
            maxs = [m for m in map(_max, ps) if m is not None]
            out.append(
                {
                    "date": date,
                    "Avg": round(sum(vals) / len(vals), 3),
                    "Min": min(mins),
                    "Max": max(maxs),
                    "hourly": True,
                }
            )
    return out


def _thin_bucket(bucket: dict, limit: int = 900_000) -> dict:
    """Si el día pesa demasiado para Upstash, pasa las lecturas a horas."""
    if len(json.dumps(bucket)) <= limit:
        return bucket
    for name, pts in list(bucket.get("metrics", {}).items()):
        if len(pts) > 40 and name != "sleep_analysis":
            mids = [p for p in pts if _is_midnight(p)]
            rest = [p for p in pts if not _is_midnight(p)]
            bucket["metrics"][name] = sorted(
                mids + _hourly(rest, _is_cumulative(name)),
                key=lambda p: str(p.get("date") or ""),
            )
    return bucket


# --- Entrenos: solo resumen + parciales ----------------------------------

_KEEP_LISTS = {"splits", "segments", "laps", "events", "activities", "intervals",
               "parciales_km", "serie_fc"}


def _kj_to_kcal(d):
    """{'qty': 1200, 'units': 'kJ'} -> añade 'kcal'."""
    if isinstance(d, dict) and isinstance(d.get("qty"), (int, float)):
        if "kj" in str(d.get("units", "")).lower():
            d = dict(d)
            d["kcal"] = round(d["qty"] / 4.184, 1)
    return d



def _hhmmss(ts: str) -> str:
    return (ts or "")[11:19]


def _downsample_hr(hr: list, max_points: int = 120) -> list:
    """Serie de FC reducida a <=120 puntos: [hora, media, máx]."""
    pts = [p for p in hr if isinstance(p, dict) and _val(p) is not None]
    pts.sort(key=lambda p: str(p.get("date") or ""))
    if not pts:
        return []
    step = max(1, -(-len(pts) // max_points))
    out = []
    for i in range(0, len(pts), step):
        grp = pts[i:i + step]
        vals = [_val(p) for p in grp]
        maxs = [_max(p) for p in grp]
        out.append([_hhmmss(grp[0].get("date")), round(sum(vals) / len(vals), 1), max(maxs)])
    return out


def _km_splits(dist_series: list, start: str, unit_hint: str = "km") -> list:
    """Parciales por km a partir de la serie de distancia (por minuto)."""
    pts = [p for p in dist_series if isinstance(p, dict) and _val(p) is not None]
    pts.sort(key=lambda p: str(p.get("date") or ""))
    if not pts:
        return []
    u = str(pts[0].get("units") or unit_hint).lower()
    factor = 0.001 if u in ("m", "meter", "meters", "metre", "metres") else 1.0
    try:
        t0 = datetime.datetime.strptime(start[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return []
    out, cum, next_km, last_t, last_cum = [], 0.0, 1, t0, 0.0
    for p in pts:
        try:
            t = datetime.datetime.strptime(p["date"][:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        cum += _val(p) * factor
        while cum >= next_km:
            # interpola el instante exacto en el que se cruza el km
            frac = (next_km - last_cum) / (cum - last_cum) if cum > last_cum else 1
            tk = last_t + (t - last_t) * frac
            prev = out[-1]["_t"] if out else t0
            secs = (tk - prev).total_seconds()
            out.append({"km": next_km, "tiempo_s": round(secs),
                        "ritmo_min_km": f"{int(secs // 60)}:{int(secs % 60):02d}",
                        "_t": tk})
            next_km += 1
        last_t, last_cum = t, cum
    for o in out:
        o.pop("_t", None)
    return out


def _compact_workout(w: dict, keep_splits: bool = True) -> dict:
    """Quita las series por minuto (FC, ruta, energía...) y deja el resumen.
    Conserva splits/segmentos (parciales). Calcula min/media/máx de FC a
    partir de la serie antes de descartarla."""
    out: dict = {}
    for k, v in w.items():
        if isinstance(v, list):
            if k in _KEEP_LISTS:
                if keep_splits:
                    out[k] = v[:200]
                else:
                    out[f"{k}_n"] = len(v)
            elif len(v) <= 5 and not any(isinstance(i, (list, dict)) for i in v):
                out[k] = v
            else:
                out[f"{k}_puntos"] = len(v)  # serie descartada
        else:
            out[k] = v

    hr = w.get("heartRateData")
    if isinstance(hr, list) and hr:
        avgs = [v for v in map(_val, [p for p in hr if isinstance(p, dict)]) if v is not None]
        mins = [m for m in map(_min, [p for p in hr if isinstance(p, dict)]) if m is not None]
        maxs = [m for m in map(_max, [p for p in hr if isinstance(p, dict)]) if m is not None]
        if avgs:
            out["fc_serie_resumen"] = {
                "media": round(sum(avgs) / len(avgs), 1),
                "min": mins and min(mins),
                "max": maxs and max(maxs),
                "lecturas": len(hr),
            }

        out["serie_fc"] = _downsample_hr(hr)

    dist = w.get("walkingAndRunningDistance") or w.get("cyclingDistance")
    if isinstance(dist, list) and dist and not out.get("parciales_km"):
        ks = _km_splits(dist, str(w.get("start") or w.get("startDate") or ""))
        if ks:
            out["parciales_km"] = ks

    for k in ("activeEnergyBurned", "totalEnergy", "activeEnergy", "basalEnergy"):
        if k in out:
            out[k] = _kj_to_kcal(out[k])
    return out


def _workout_key(w: dict) -> tuple:
    return (w.get("id") or (w.get("name"), w.get("start") or w.get("startDate")),)


def _merge_workouts(old: list, new: list) -> list:
    merged: dict = {}
    for w in list(old or []) + list(new or []):
        if isinstance(w, dict):
            merged[_workout_key(w)] = _compact_workout(w, keep_splits=True)
    return list(merged.values())


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
        print("Error decodificando el JSON recibido en /ingest:")
        traceback.print_exc()
        return JSONResponse({"error": "JSON inválido"}, status_code=400)

    try:
        saved = _ingest_payload(payload)
        return JSONResponse({"status": "guardado", "dates": saved})
    except Exception as e:
        print("FALLO en /ingest:")
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


def _ingest_payload(payload: dict) -> list:
    # Soporta {"data": {...}} (Health Auto Export) y {"metrics":..., "workouts":...}.
    data = payload.get("data", payload)
    metrics = data.get("metrics", []) or []
    workouts = data.get("workouts", []) or []
    print(f"/ingest recibido: {len(metrics)} métricas, {len(workouts)} entrenamientos")

    days: dict = {}

    def bucket_for(day: str) -> dict:
        return days.setdefault(
            day, {"date": day, "metrics": {}, "units": {}, "workouts": []}
        )

    for metric in metrics:
        name = metric.get("name", "unknown")
        units = metric.get("units")
        for point in metric.get("data", []) or []:
            if not isinstance(point, dict):
                continue
            day = _day_from_timestamp(
                point.get("date") or point.get("sleepStart") or point.get("startDate") or ""
            )
            if not day:
                continue
            b = bucket_for(day)
            b["metrics"].setdefault(name, []).append(point)
            if units:
                b["units"][name] = units

    for workout in workouts:
        if not isinstance(workout, dict):
            continue
        ts = workout.get("start") or workout.get("startDate") or workout.get("date") or ""
        day = _day_from_timestamp(ts)
        if not day:
            continue
        bucket_for(day)["workouts"].append(workout)

    if not days:
        days[_today()] = {"date": _today(), "raw": payload}

    saved_dates = []
    for day, bucket in days.items():
        key = f"healthdata:{day}"
        try:
            existing = _upstash_get(key)
        except Exception:
            print(f"No se pudo leer {key} (se sobrescribe):")
            traceback.print_exc()
            existing = None

        if "raw" not in bucket:
            old_metrics = {}
            old_units = {}
            old_workouts = []
            if isinstance(existing, dict) and "metrics" in existing:
                old_metrics = existing.get("metrics") or {}
                old_units = existing.get("units") or {}
                old_workouts = existing.get("workouts") or []

            merged_metrics = {}
            for name in set(old_metrics) | set(bucket["metrics"]):
                merged_metrics[name] = _merge_points(
                    old_metrics.get(name, []), bucket["metrics"].get(name, [])
                )
            bucket["metrics"] = merged_metrics
            bucket["units"] = {**old_units, **bucket["units"]}
            bucket["workouts"] = _merge_workouts(old_workouts, bucket["workouts"])
            bucket = _thin_bucket(bucket)

        _upstash_set(key, bucket)
        saved_dates.append(day)
        print(f"Guardado {key} correctamente.")
    return saved_dates


# --- Agregación al leer ---------------------------------------------------


def _agg_metric(name: str, points: list, units: Optional[str]) -> dict:
    points = _merge_points([], points)  # quita duplicados de datos antiguos
    res: dict = {"unidades": units, "lecturas": len(points)}
    mids = [p for p in points if _is_midnight(p)]
    rest = [p for p in points if not _is_midnight(p)]
    rest_vals = [v for v in map(_val, rest) if v is not None]
    mid_vals = [v for v in map(_val, mids) if v is not None]

    if not rest_vals and not mid_vals:
        # Métrica no numérica: devuelve unas pocas muestras
        res["muestras"] = points[:10]
        return res

    if _is_cumulative(name):
        rest_sum = sum(rest_vals)
        mid = max(mid_vals) if mid_vals else None
        if mid is not None and (not rest_vals or mid >= rest_sum * 0.9):
            total = max(mid, rest_sum)
            res["fuente"] = "total_diario" if mid >= rest_sum else "suma_lecturas"
        else:
            total = rest_sum + sum(mid_vals)
            res["fuente"] = "suma_lecturas"
        res["total"] = _r(total, 3)
        if units and "kj" in units.lower():
            res["equivalente_kcal"] = _r(total / 4.184, 1)
    else:
        use = rest if rest_vals else mids
        vals = [v for v in map(_val, use) if v is not None]
        mins = [m for m in map(_min, use) if m is not None]
        maxs = [m for m in map(_max, use) if m is not None]
        res["media"] = _r(sum(vals) / len(vals), 2)
        res["min"] = _r(min(mins), 2)
        res["max"] = _r(max(maxs), 2)
        last = sorted(use, key=lambda p: str(p.get("date") or ""))[-1]
        res["ultima"] = _r(_val(last), 2)
    return res


_SLEEP_FIELDS = (
    "sleepStart", "sleepEnd", "inBedStart", "inBedEnd",
    "totalSleep", "asleep", "deep", "core", "rem", "awake", "inBed",
)


def _agg_sleep(points: list, units: Optional[str]) -> list:
    sessions: dict = {}
    for p in _merge_points([], points):
        if not isinstance(p, dict):
            continue
        k = (p.get("sleepStart") or p.get("inBedStart") or p.get("date"),
             p.get("sleepEnd") or p.get("inBedEnd"))
        if k in sessions:
            continue
        s = {}
        for f in _SLEEP_FIELDS:
            if f in p:
                s[f] = _r(p[f], 2) if isinstance(p[f], (int, float)) else p[f]
        if s:
            sessions[k] = s
    out = list(sessions.values())
    if out:
        out[0]["unidades"] = units or "hr"
        return out

    # Formato por fases (value + qty): suma por fase
    by_stage: dict = {}
    for p in points:
        if isinstance(p, dict) and _val(p) is not None:
            by_stage[str(p.get("value", "?"))] = by_stage.get(str(p.get("value", "?")), 0) + _val(p)
    return [{"por_fase": {k: _r(v, 2) for k, v in by_stage.items()}, "unidades": units}] if by_stage else []


def _summarize_day(data: dict, detail: bool = True) -> dict:
    d = data.get("date")
    if "metrics" not in data:
        return {"date": d, "status": "formato_desconocido",
                "nota": "Datos guardados sin el formato esperado."}
    units = data.get("units") or {}
    out_metrics: dict = {}
    sleep: list = []
    for name, pts in (data.get("metrics") or {}).items():
        if name == "sleep_analysis":
            sleep = _agg_sleep(pts, units.get(name))
        else:
            out_metrics[name] = _agg_metric(name, pts, units.get(name))
    workouts = []
    for w in data.get("workouts") or []:
        if isinstance(w, dict):
            c = _compact_workout(w, keep_splits=detail)
            c.pop("serie_fc", None)
            c.pop("serie_fc_n", None)
            workouts.append(c)
    return {
        "date": d,
        "metrics": out_metrics,
        "sleep": sleep,
        "workouts": workouts,
        "nota": "Totales ya agregados por día. Energía de Apple en kJ (ver equivalente_kcal).",
    }


# --- Herramientas expuestas a Claude -------------------------------------


@mcp.tool()
def get_daily_summary(date: str = "") -> dict:
    """Resumen de Apple Health de un día (YYYY-MM-DD), ya agregado:
    'metrics' con total (pasos, distancia, energía, pisos...) o
    media/min/max/ultima (FC, HRV, SpO2...) por métrica, con unidades;
    'sleep' con las sesiones de sueño sin duplicados; 'workouts' solo con
    resumen y parciales (splits/segmentos), sin series por minuto.
    Si no se indica fecha, usa hoy. Para ver el detalle por horas de una
    métrica usa get_metric_by_hour."""
    d = date or _today()
    data = _upstash_get(f"healthdata:{d}")
    if data is None:
        return {
            "date": d,
            "status": "sin_datos",
            "nota": "No hay datos guardados para este día todavía.",
        }
    return _summarize_day(data, detail=True)


@mcp.tool()
def get_range_summary(start_date: str, end_date: str) -> list:
    """Resúmenes diarios agregados entre dos fechas (YYYY-MM-DD), ambas
    incluidas (máx. 31 días). Entrenos solo con resumen (sin parciales;
    para verlos usa get_daily_summary de ese día)."""
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    if (end - start).days > 30:
        end = start + datetime.timedelta(days=30)
    results = []
    day = start
    while day <= end:
        data = _upstash_get(f"healthdata:{day.isoformat()}")
        if data is not None:
            results.append(_summarize_day(data, detail=False))
        day += datetime.timedelta(days=1)
    return results


@mcp.tool()
def get_metric_by_hour(date: str, metric: str) -> dict:
    """Detalle por horas de una métrica de un día (p. ej. metric='step_count'
    o 'heart_rate'). Suma por hora en métricas acumulativas; media/min/max
    por hora en las demás. Úsalo solo si hace falta más detalle que el
    resumen diario."""
    data = _upstash_get(f"healthdata:{date}")
    if data is None:
        return {"date": date, "status": "sin_datos"}
    pts = (data.get("metrics") or {}).get(metric)
    if pts is None:
        return {
            "date": date,
            "status": "metrica_no_encontrada",
            "disponibles": sorted((data.get("metrics") or {}).keys()),
        }
    cum = _is_cumulative(metric)
    rest = [p for p in _merge_points([], pts) if not _is_midnight(p)]
    rows = []
    for h in _hourly(rest, cum):
        row = {"hora": h["date"][11:16]}
        row.update({k: v for k, v in h.items() if k in ("qty", "Avg", "Min", "Max")})
        rows.append(row)
    return {
        "date": date,
        "metric": metric,
        "unidades": (data.get("units") or {}).get(metric),
        "tipo": "suma_por_hora" if cum else "media_min_max_por_hora",
        "horas": rows,
    }


@mcp.tool()
def get_workout_detail(date: str, index: int = 0) -> dict:
    """Detalle de UN entreno (el nº 'index' de ese día, empezando en 0) para
    analizarlo a fondo: resumen completo, parciales por km (tiempo y ritmo),
    splits/segmentos del reloj y la curva de frecuencia cardiaca reducida a
    ~120 puntos ([hora, FC media, FC máx]) para ver deriva cardiaca y
    zonas. Úsalo tras get_daily_summary."""
    data = _upstash_get(f"healthdata:{date}")
    if data is None:
        return {"date": date, "status": "sin_datos"}
    ws = [w for w in (data.get("workouts") or []) if isinstance(w, dict)]
    if not ws or index >= len(ws) or index < 0:
        return {"date": date, "status": "entreno_no_encontrado", "entrenos_del_dia": len(ws)}
    return {"date": date, "index": index, **_compact_workout(ws[index], keep_splits=True)}


def _wk_km(w: dict) -> Optional[float]:
    d = w.get("distance")
    if isinstance(d, dict) and isinstance(d.get("qty"), (int, float)):
        u = str(d.get("units", "km")).lower()
        return d["qty"] * (1.609344 if u.startswith("mi") else 0.001 if u == "m" else 1.0)
    return None


def _wk_hr(w: dict) -> Optional[float]:
    for k in ("avgHeartRate",):
        v = w.get(k)
        if isinstance(v, dict):
            v = v.get("qty")
        if isinstance(v, (int, float)):
            return float(v)
    r = (w.get("fc_serie_resumen") or {}).get("media")
    return float(r) if isinstance(r, (int, float)) else None


@mcp.tool()
def get_trends(days: int = 14) -> dict:
    """Tendencias de los últimos N días (por defecto 14, máx. 60) para
    valorar la recuperación y la carga de entrenamiento: tabla diaria
    (pasos, kcal activas, FC en reposo, HRV, horas de sueño, nº de entrenos
    y km) más medias de la última semana frente a la anterior, y resumen
    de carreras (km, sesiones, ritmo medio, FC media) por semana."""
    days = max(2, min(int(days), 60))
    end = datetime.date.today()
    rows, weeks = [], {}
    for i in range(days - 1, -1, -1):
        day = end - datetime.timedelta(days=i)
        data = _upstash_get(f"healthdata:{day.isoformat()}")
        if data is None:
            continue
        sm = _summarize_day(data, detail=False)
        m = sm["metrics"]

        def tot(n):
            return (m.get(n) or {}).get("total")

        def avg(n):
            return (m.get(n) or {}).get("media")

        act = tot("active_energy")
        if act is not None and "kj" in str((m["active_energy"].get("unidades") or "")).lower():
            act = act / 4.184
        sleep_h = None
        if sm["sleep"]:
            sleep_h = sm["sleep"][0].get("totalSleep") or sm["sleep"][0].get("asleep")
        km = sum(_wk_km(w) or 0 for w in sm["workouts"])
        rows.append({
            "fecha": day.isoformat(),
            "pasos": tot("step_count"),
            "kcal_activas": _r(act, 0),
            "fc_reposo": avg("resting_heart_rate"),
            "hrv": avg("heart_rate_variability"),
            "sueno_h": sleep_h,
            "entrenos": len(sm["workouts"]),
            "km_entrenos": _r(km, 2) if km else 0,
        })
        wk_id = day.isocalendar()
        wkey = f"{wk_id[0]}-S{wk_id[1]:02d}"
        for w in sm["workouts"]:
            name = str(w.get("name", "")).lower()
            kmw = _wk_km(w)
            if kmw and any(x in name for x in ("run", "carrera", "correr")):
                b = weeks.setdefault(wkey, {"sesiones": 0, "km": 0.0, "seg": 0.0, "fc": []})
                b["sesiones"] += 1
                b["km"] += kmw
                b["seg"] += float(w.get("duration") or 0)
                hr = _wk_hr(w)
                if hr:
                    b["fc"].append(hr)

    def mean(key, chunk):
        v = [r[key] for r in chunk if isinstance(r.get(key), (int, float))]
        return _r(sum(v) / len(v), 1) if v else None

    last, prev = rows[-7:], rows[-14:-7]
    comp = {
        k: {"ultimos_7d": mean(k, last), "7d_anteriores": mean(k, prev)}
        for k in ("pasos", "kcal_activas", "fc_reposo", "hrv", "sueno_h")
    }
    carreras = {}
    for k, b in sorted(weeks.items()):
        pace = None
        if b["km"] > 0 and b["seg"] > 0:
            sp = b["seg"] / b["km"]
            pace = f"{int(sp // 60)}:{int(sp % 60):02d}"
        carreras[k] = {
            "sesiones": b["sesiones"], "km": _r(b["km"], 1), "ritmo_medio_min_km": pace,
            "fc_media": _r(sum(b["fc"]) / len(b["fc"]), 0) if b["fc"] else None,
        }
    return {"dias": rows, "comparativa_semanal": comp, "carreras_por_semana": carreras}


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http")
