import hashlib
import os
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

app = FastAPI(title="CinemaAbyss Proxy Service", version="1.0.0")


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


PORT = env_int("PORT", 8000)
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082").rstrip("/")

GRADUAL_MIGRATION = env_bool("GRADUAL_MIGRATION", False)
MOVIES_MIGRATION_PERCENT = max(0, min(100, env_int("MOVIES_MIGRATION_PERCENT", 0)))

client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))


@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()


@app.get("/health")
async def health():
    return {
        "status": True,
        "service": "proxy-service",
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
        "monolith_url": MONOLITH_URL,
        "movies_service_url": MOVIES_SERVICE_URL,
        "events_service_url": EVENTS_SERVICE_URL,
    }


def stable_bucket(key: str) -> int:

    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    n = int(h[:8], 16)
    return n % 100


def choose_movies_backend(request: Request) -> str:
    """
    Выбираем куда слать /api/movies*:
    - если X-Force-Service установлен -> принудительно
    - иначе если миграция выключена -> MONOLITH
    - иначе -> детерминированный процентный роутинг
    """
    forced = (request.headers.get("x-force-service") or "").strip().lower()
    if forced in ("monolith", "movies"):
        return forced

    if not GRADUAL_MIGRATION:
        return "monolith"

    # Стабильный ключ:
    # 1) X-Request-Id (если есть)
    # 2) query id (если есть)
    # 3) ip + path + query
    req_id = request.headers.get("x-request-id")
    q_id = request.query_params.get("id")

    if req_id:
        key = f"rid:{req_id}"
    elif q_id:
        key = f"qid:{q_id}"
    else:
        ip = request.client.host if request.client else "unknown"
        key = f"ip:{ip}|{request.method}|{request.url.path}?{request.url.query}"

    bucket = stable_bucket(key)
    return "movies" if bucket < MOVIES_MIGRATION_PERCENT else "monolith"


def pick_backend(request: Request) -> Tuple[str, str]:
    """
    Возвращает (backend_name, base_url)
    backend_name: monolith|movies|events
    """
    path = request.url.path

    forced = (request.headers.get("x-force-service") or "").strip().lower()
    if forced in ("monolith", "movies", "events"):
        if forced == "monolith":
            return "monolith", MONOLITH_URL
        if forced == "movies":
            return "movies", MOVIES_SERVICE_URL
        return "events", EVENTS_SERVICE_URL

    if path.startswith("/api/movies"):
        backend = choose_movies_backend(request)
        return (backend, MOVIES_SERVICE_URL) if backend == "movies" else ("monolith", MONOLITH_URL)

    if path.startswith("/api/events"):
        return "events", EVENTS_SERVICE_URL

    # Всё остальное — монолит
    return "monolith", MONOLITH_URL


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def filter_request_headers(headers: httpx.Headers) -> dict:
    out = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS:
            continue
        if lk == "host":
            continue
        out[k] = v
    return out


def filter_response_headers(headers: httpx.Headers) -> dict:
    out = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in HOP_BY_HOP_HEADERS:
            continue
        out[k] = v
    return out


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(full_path: str, request: Request):
    backend_name, base_url = pick_backend(request)

    target_url = f"{base_url}/{full_path}"
    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"
    body = await request.body()

    headers = filter_request_headers(request.headers)
    headers["X-Proxy-Backend"] = backend_name

    try:
        upstream = await client.request(
            method=request.method,
            url=target_url,
            content=body if body else None,
            headers=headers,
        )
    except httpx.RequestError as e:
        return JSONResponse(
            status_code=502,
            content={
                "error": "bad_gateway",
                "message": f"Upstream request failed: {str(e)}",
                "backend": backend_name,
                "target_url": target_url,
            },
        )

    resp_headers = filter_response_headers(upstream.headers)
    resp_headers["X-Proxy-Backend"] = backend_name

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=resp_headers,
        media_type=upstream.headers.get("content-type"),
    )
