from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from datetime import datetime, timezone, timedelta
from typing import Optional, List
import logging
import os

ES_HOST     = os.getenv("ES_HOST", "http://localhost:9200")
API_VERSION = "1.0.0"
API_TITLE   = "Maritime Intelligence API"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("maritime-api")
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description="""
    ## API de suivi maritime temps reel

    Donnees AIS mondiales via Kafka -> Elasticsearch.

    ### Fonctionnalites
    * Suivi en temps reel de tous les navires
    * Detection d anomalies par Machine Learning
    * Predictions de position future
    * Statistiques et analyses
    """,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_es() -> Elasticsearch:
    return Elasticsearch(ES_HOST, request_timeout=30)


def now_minus(minutes: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_hits(response: dict) -> List[dict]:
    hits = response.get("hits", {}).get("hits", [])
    results = []
    for hit in hits:
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        results.append(doc)
    return results


@app.get("/", tags=["Info"])
def root():
    return {
        "name":    API_TITLE,
        "version": API_VERSION,
        "status":  "running",
        "docs":    "http://localhost:8000/docs",
        "endpoints": {
            "ships":       "/ships",
            "ships_live":  "/ships/live",
            "ship_detail": "/ships/{mmsi}",
            "anomalies":   "/anomalies",
            "predictions": "/predictions",
            "stats":       "/stats",
            "countries":   "/countries",
            "search":      "/search?q=nom_navire",
            "health":      "/health",
        }
    }


@app.get("/health", tags=["Info"])
def health_check():
    try:
        es = get_es()
        cluster = es.cluster.health()
        return {
            "status":    "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elasticsearch": {
                "status":  cluster["status"],
                "cluster": cluster["cluster_name"],
            },
            "indices": {
                "ships":       {"count": es.count(index="ships").get("count", 0)},
                "anomalies":   {"count": es.count(index="anomalies").get("count", 0)},
                "predictions": {"count": es.count(index="predictions").get("count", 0)},
            }
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service indisponible: {str(e)}")


@app.get("/ships", tags=["Ships"])
def get_ships(
    limit:     int            = Query(default=50,  ge=1, le=500),
    country:   Optional[str]  = Query(default=None),
    min_speed: Optional[float] = Query(default=None),
    max_speed: Optional[float] = Query(default=None),
):
    es = get_es()
    must_clauses = []

    if country:
        must_clauses.append({"match": {"country": country}})

    speed_range = {}
    if min_speed is not None:
        speed_range["gte"] = min_speed
    if max_speed is not None:
        speed_range["lte"] = max_speed
    if speed_range:
        must_clauses.append({"range": {"speed": speed_range}})

    query = {
        "query": {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "size":  limit,
        "sort":  [{"ingested_at": {"order": "desc"}}],
    }

    try:
        resp  = es.search(index="ships", body=query)
        ships = extract_hits(resp)
        total = resp["hits"]["total"]["value"]
        return {"total": total, "returned": len(ships), "ships": ships}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ships/live", tags=["Ships"])
def get_live_ships(
    minutes: int = Query(default=5, ge=1, le=60)
):
    es    = get_es()
    since = now_minus(minutes)

    query = {
        "query": {"range": {"ingested_at": {"gte": since}}},
        "size":  500,
        "sort":  [{"speed": {"order": "desc"}}],
    }

    try:
        resp  = es.search(index="ships", body=query)
        ships = extract_hits(resp)
        total = resp["hits"]["total"]["value"]
        return {"active_since": since, "total_active": total, "ships": ships}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ships/{mmsi}", tags=["Ships"])
def get_ship_by_mmsi(mmsi: str):
    es = get_es()
    query = {
        "query": {"term": {"mmsi": mmsi}},
        "size":  1,
        "sort":  [{"ingested_at": {"order": "desc"}}],
    }

    try:
        resp = es.search(index="ships", body=query)
        hits = resp["hits"]["hits"]

        if not hits:
            raise HTTPException(status_code=404, detail=f"Navire MMSI {mmsi} introuvable")

        ship = hits[0]["_source"]

        try:
            anom_resp = es.search(index="anomalies", body={
                "query": {"term": {"mmsi": mmsi}},
                "size":  10,
                "sort":  [{"detected_at": {"order": "desc"}}],
            })
            anomalies = extract_hits(anom_resp)
        except Exception:
            anomalies = []

        try:
            pred_resp   = es.search(index="predictions", body={
                "query": {"term": {"mmsi": mmsi}},
                "size":  5,
                "sort":  [{"predicted_at": {"order": "desc"}}],
            })
            predictions = extract_hits(pred_resp)
        except Exception:
            predictions = []

        return {"ship": ship, "anomalies": anomalies, "predictions": predictions}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies", tags=["Anomalies"])
def get_anomalies(
    limit:    int           = Query(default=50, ge=1, le=500),
    severity: Optional[str] = Query(default=None),
    type:     Optional[str] = Query(default=None),
):
    es = get_es()
    must_clauses = []

    if severity:
        must_clauses.append({"term": {"severity": severity.upper()}})
    if type:
        must_clauses.append({"term": {"anomaly_type": type.upper()}})

    query = {
        "query": {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "size":  limit,
        "sort":  [{"detected_at": {"order": "desc"}}],
    }

    try:
        resp      = es.search(index="anomalies", body=query)
        anomalies = extract_hits(resp)
        total     = resp["hits"]["total"]["value"]
        return {"total": total, "returned": len(anomalies), "anomalies": anomalies}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies/{anomaly_id}", tags=["Anomalies"])
def get_anomaly(anomaly_id: str):
    es = get_es()
    try:
        doc = es.get(index="anomalies", id=anomaly_id)
        return doc["_source"]
    except Exception:
        raise HTTPException(status_code=404, detail=f"Anomalie {anomaly_id} introuvable")


@app.get("/predictions", tags=["Predictions"])
def get_predictions(
    limit: int = Query(default=50, ge=1, le=500),
):
    es = get_es()
    query = {
        "query": {"match_all": {}},
        "size":  limit,
        "sort":  [{"predicted_at": {"order": "desc"}}],
    }

    try:
        resp        = es.search(index="predictions", body=query)
        predictions = extract_hits(resp)
        total       = resp["hits"]["total"]["value"]
        return {"total": total, "returned": len(predictions), "predictions": predictions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictions/{mmsi}", tags=["Predictions"])
def get_prediction_by_mmsi(mmsi: str):
    es = get_es()
    query = {
        "query": {"term": {"mmsi": mmsi}},
        "size":  1,
        "sort":  [{"predicted_at": {"order": "desc"}}],
    }

    try:
        resp = es.search(index="predictions", body=query)
        hits = resp["hits"]["hits"]
        if not hits:
            raise HTTPException(status_code=404, detail=f"Pas de prediction pour MMSI {mmsi}")
        return hits[0]["_source"]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", tags=["Analytics"])
def get_stats():
    es    = get_es()
    since = now_minus(10)

    query = {
        "query": {"match_all": {}},
        "size":  0,
        "aggs": {
            "speed_stats":    {"stats": {"field": "speed"}},
            "top_countries":  {"terms": {"field": "country.keyword", "size": 5}},
            "active_ships":   {"filter": {"range": {"ingested_at": {"gte": since}}}},
        }
    }

    anom_query = {
        "query": {"match_all": {}},
        "size":  0,
        "aggs": {
            "by_severity": {"terms": {"field": "severity.keyword"}}
        }
    }

    try:
        ships_resp = es.search(index="ships",     body=query)
        anom_resp  = es.search(index="anomalies", body=anom_query)

        aggs          = ships_resp["aggregations"]
        speed_stats   = aggs["speed_stats"]
        top_countries = [
            {"country": b["key"], "count": b["doc_count"]}
            for b in aggs["top_countries"]["buckets"]
        ]
        severity_counts = {
            b["key"]: b["doc_count"]
            for b in anom_resp["aggregations"]["by_severity"]["buckets"]
        }

        return {
            "ships": {
                "total":           ships_resp["hits"]["total"]["value"],
                "active_last_10m": aggs["active_ships"]["doc_count"],
                "speed": {
                    "average": round(speed_stats.get("avg", 0) or 0, 2),
                    "min":     round(speed_stats.get("min", 0) or 0, 2),
                    "max":     round(speed_stats.get("max", 0) or 0, 2),
                },
                "top_countries": top_countries,
            },
            "anomalies": {
                "total":  anom_resp["hits"]["total"]["value"],
                "HIGH":   severity_counts.get("HIGH",   0),
                "MEDIUM": severity_counts.get("MEDIUM", 0),
                "LOW":    severity_counts.get("LOW",    0),
            },
            "predictions": {
                "total": es.count(index="predictions").get("count", 0)
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/countries", tags=["Analytics"])
def get_countries(
    limit: int = Query(default=20, ge=1, le=100)
):
    es = get_es()
    query = {
        "query": {"match_all": {}},
        "size":  0,
        "aggs": {
            "countries": {
                "terms": {
                    "field": "country.keyword",
                    "size":  limit,
                    "order": {"_count": "desc"}
                }
            }
        }
    }

    try:
        resp    = es.search(index="ships", body=query)
        buckets = resp["aggregations"]["countries"]["buckets"]
        return {
            "countries": [
                {"country": b["key"], "ship_count": b["doc_count"]}
                for b in buckets
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search", tags=["Ships"])
def search_ships(
    q:     str = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
):
    es = get_es()
    query = {
        "query": {
            "match": {
                "ship_name": {
                    "query":     q,
                    "operator":  "and",
                    "fuzziness": "AUTO",
                }
            }
        },
        "size": limit,
        "sort": [{"ingested_at": {"order": "desc"}}],
    }

    try:
        resp  = es.search(index="ships", body=query)
        ships = extract_hits(resp)
        total = resp["hits"]["total"]["value"]
        return {"query": q, "total": total, "returned": len(ships), "ships": ships}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))