"""
====================================================
  🚢  MARITIME API — FastAPI Backend
====================================================

Ce fichier est le CERVEAU de l'API.
Il définit toutes les routes (URLs) disponibles.

Architecture :
    Kibana / React / Power BI
           ↓  HTTP requests
      FastAPI (ce fichier)
           ↓  queries
      Elasticsearch
           ↓  données
      Index: ships, anomalies, predictions

Routes disponibles :
    GET /                          → info API
    GET /health                    → santé du système
    GET /ships                     → liste des navires
    GET /ships/{mmsi}              → un navire précis
    GET /ships/live                → navires actifs (5 dernières minutes)
    GET /anomalies                 → toutes les anomalies
    GET /anomalies/{id}            → une anomalie précise
    GET /predictions               → toutes les prédictions
    GET /predictions/{mmsi}        → prédiction d'un navire
    GET /stats                     → statistiques globales
    GET /countries                 → navires par pays
    GET /search?q=MAERSK           → recherche par nom

====================================================
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from datetime import datetime, timezone, timedelta
from typing import Optional, List
import logging
import os

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
ES_HOST      = os.getenv("ES_HOST", "http://localhost:9200")
API_VERSION  = "1.0.0"
API_TITLE    = "Maritime Intelligence API"

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("maritime-api")
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────
# FastAPI crée automatiquement une documentation interactive
# sur http://localhost:8000/docs — tu peux tester toutes les
# routes directement depuis le navigateur !
app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description="""
    ## API de suivi maritime temps réel
    
    Données AIS mondiales via Kafka → Elasticsearch.
    
    ### Fonctionnalités
    * 🚢 Suivi en temps réel de tous les navires
    * 🚨 Détection d'anomalies par Machine Learning
    * 📍 Prédictions de position future
    * 📊 Statistiques et analyses
    """,
    docs_url="/docs",       # Swagger UI → http://localhost:8000/docs
    redoc_url="/redoc",     # ReDoc UI  → http://localhost:8000/redoc
)

# ─────────────────────────────────────────────────────────────
# CORS — permet au frontend React d'appeler l'API
# ─────────────────────────────────────────────────────────────
# Sans CORS, le navigateur bloque les requêtes cross-origin.
# allow_origins=["*"] autorise TOUS les domaines (dev seulement)
# En production, remplacer par ["https://ton-site.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# ELASTICSEARCH CLIENT
# ─────────────────────────────────────────────────────────────
def get_es() -> Elasticsearch:
    """Retourne un client Elasticsearch connecté."""
    return Elasticsearch(ES_HOST, request_timeout=30)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def now_minus(minutes: int) -> str:
    """Retourne un timestamp ISO 8601 d'il y a N minutes."""
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def extract_hits(response: dict) -> List[dict]:
    """Extrait les documents d'une réponse Elasticsearch."""
    hits = response.get("hits", {}).get("hits", [])
    results = []
    for hit in hits:
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        results.append(doc)
    return results


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────
# ROUTE 1 — Page d'accueil de l'API
# ─────────────────────────────────────────────────────────────
@app.get("/", tags=["Info"])
def root():
    """
    Page d'accueil — informations générales sur l'API.
    
    Retourne la version, le statut et la liste des endpoints.
    """
    return {
        "name":     API_TITLE,
        "version":  API_VERSION,
        "status":   "running",
        "docs":     "http://localhost:8000/docs",
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


# ─────────────────────────────────────────────────────────────
# ROUTE 2 — Santé du système
# ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["Info"])
def health_check():
    """
    Vérifie que tous les services sont opérationnels.
    
    Retourne le statut de :
    - L'API FastAPI elle-même
    - Elasticsearch
    - Les 3 index (ships, anomalies, predictions)
    """
    try:
        es = get_es()
        cluster = es.cluster.health()

        # Compte les documents dans chaque index
        ships_count      = es.count(index="ships").get("count", 0)
        anomalies_count  = es.count(index="anomalies").get("count", 0)
        predictions_count = es.count(index="predictions").get("count", 0)

        return {
            "status":      "healthy",
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "elasticsearch": {
                "status":  cluster["status"],        # green/yellow/red
                "cluster": cluster["cluster_name"],
            },
            "indices": {
                "ships":       {"count": ships_count},
                "anomalies":   {"count": anomalies_count},
                "predictions": {"count": predictions_count},
            }
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service indisponible: {str(e)}")


# ─────────────────────────────────────────────────────────────
# ROUTE 3 — Liste des navires
# ─────────────────────────────────────────────────────────────
@app.get("/ships", tags=["Ships"])
def get_ships(
    limit:   int            = Query(default=50,  ge=1, le=500,  description="Nombre de navires à retourner (max 500)"),
    country: Optional[str]  = Query(default=None, description="Filtrer par pays ex: France"),
    min_speed: Optional[float] = Query(default=None, description="Vitesse minimum en nœuds"),
    max_speed: Optional[float] = Query(default=None, description="Vitesse maximum en nœuds"),
):
    """
    Retourne la liste des navires.
    
    Paramètres optionnels :
    - **limit** : nombre de résultats (défaut 50, max 500)
    - **country** : filtrer par pays (ex: France, Pays-Bas)
    - **min_speed** : vitesse minimum en nœuds
    - **max_speed** : vitesse maximum en nœuds
    
    Exemples :
    - `/ships` → 50 derniers navires
    - `/ships?country=France` → navires français
    - `/ships?min_speed=15` → navires rapides
    - `/ships?country=Pays-Bas&limit=100` → 100 navires hollandais
    """
    es = get_es()

    # Construction de la requête dynamique
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
        "size": limit,
        "sort": [{"ingested_at": {"order": "desc"}}],  # plus récents en premier
    }

    try:
        resp = es.search(index="ships", body=query)
        ships = extract_hits(resp)
        total = resp["hits"]["total"]["value"]

        return {
            "total":   total,
            "returned": len(ships),
            "ships":   ships,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 4 — Navires actifs (temps réel)
# ─────────────────────────────────────────────────────────────
@app.get("/ships/live", tags=["Ships"])
def get_live_ships(
    minutes: int = Query(default=5, ge=1, le=60, description="Navires actifs dans les N dernières minutes")
):
    """
    Retourne UNIQUEMENT les navires actifs récemment.
    
    Utilise le champ ingested_at pour filtrer les navires
    dont la position a été reçue dans les N dernières minutes.
    
    - `/ships/live` → navires des 5 dernières minutes
    - `/ships/live?minutes=30` → navires des 30 dernières minutes
    """
    es    = get_es()
    since = now_minus(minutes)

    query = {
        "query": {
            "range": {"ingested_at": {"gte": since}}
        },
        "size": 500,
        "sort": [{"speed": {"order": "desc"}}],  # plus rapides en premier
    }

    try:
        resp  = es.search(index="ships", body=query)
        ships = extract_hits(resp)
        total = resp["hits"]["total"]["value"]

        return {
            "active_since": since,
            "total_active": total,
            "ships":        ships,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 5 — Détail d'un navire par MMSI
# ─────────────────────────────────────────────────────────────
@app.get("/ships/{mmsi}", tags=["Ships"])
def get_ship_by_mmsi(mmsi: str):
    """
    Retourne toutes les informations d'un navire spécifique.
    
    Le MMSI est l'identifiant unique du navire (9 chiffres).
    Ex: `/ships/235097314` → détails du VOS FAIRNESS
    
    Retourne aussi :
    - Les anomalies détectées pour ce navire
    - Les prédictions de position future
    """
    es = get_es()

    # Cherche le navire le plus récent avec ce MMSI
    query = {
        "query":  {"term": {"mmsi": mmsi}},
        "size":   1,
        "sort":   [{"ingested_at": {"order": "desc"}}],
    }

    try:
        resp = es.search(index="ships", body=query)
        hits = resp["hits"]["hits"]

        if not hits:
            raise HTTPException(status_code=404, detail=f"Navire MMSI {mmsi} introuvable")

        ship = hits[0]["_source"]

        # Cherche les anomalies de ce navire
        anom_query = {
            "query": {"term": {"mmsi": mmsi}},
            "size":  10,
            "sort":  [{"detected_at": {"order": "desc"}}],
        }
        try:
            anom_resp  = es.search(index="anomalies", body=anom_query)
            anomalies  = extract_hits(anom_resp)
        except Exception:
            anomalies  = []

        # Cherche les prédictions de ce navire
        pred_query = {
            "query": {"term": {"mmsi": mmsi}},
            "size":  5,
            "sort":  [{"predicted_at": {"order": "desc"}}],
        }
        try:
            pred_resp   = es.search(index="predictions", body=pred_query)
            predictions = extract_hits(pred_resp)
        except Exception:
            predictions = []

        return {
            "ship":        ship,
            "anomalies":   anomalies,
            "predictions": predictions,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 6 — Liste des anomalies
# ─────────────────────────────────────────────────────────────
@app.get("/anomalies", tags=["Anomalies"])
def get_anomalies(
    limit:    int           = Query(default=50, ge=1, le=500),
    severity: Optional[str] = Query(default=None, description="HIGH, MEDIUM ou LOW"),
    type:     Optional[str] = Query(default=None, description="SPEED_EXCESSIVE, VESSEL_STOPPED, UNUSUAL_PATTERN"),
):
    """
    Retourne les anomalies détectées par le ML Engine.
    
    Filtres disponibles :
    - **severity** : HIGH, MEDIUM, LOW
    - **type** : SPEED_EXCESSIVE, VESSEL_STOPPED, UNUSUAL_PATTERN
    
    Exemples :
    - `/anomalies` → toutes les anomalies
    - `/anomalies?severity=HIGH` → anomalies graves seulement
    - `/anomalies?type=VESSEL_STOPPED` → navires à l'arrêt suspect
    """
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

        return {
            "total":     total,
            "returned":  len(anomalies),
            "anomalies": anomalies,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 7 — Détail d'une anomalie par ID
# ─────────────────────────────────────────────────────────────
@app.get("/anomalies/{anomaly_id}", tags=["Anomalies"])
def get_anomaly(anomaly_id: str):
    """
    Retourne le détail complet d'une anomalie.
    
    L'ID est retourné dans la liste /anomalies dans le champ _id.
    """
    es = get_es()
    try:
        doc = es.get(index="anomalies", id=anomaly_id)
        return doc["_source"]
    except Exception:
        raise HTTPException(status_code=404, detail=f"Anomalie {anomaly_id} introuvable")


# ─────────────────────────────────────────────────────────────
# ROUTE 8 — Liste des prédictions
# ─────────────────────────────────────────────────────────────
@app.get("/predictions", tags=["Predictions"])
def get_predictions(
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Retourne les prédictions de position future générées par le ML Engine.
    
    Chaque prédiction contient :
    - Position actuelle du navire
    - Position prédite dans 30 minutes
    - Position prédite dans 1 heure
    - Score de confiance (0.0 à 1.0)
    """
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

        return {
            "total":       total,
            "returned":    len(predictions),
            "predictions": predictions,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 9 — Prédiction d'un navire spécifique
# ─────────────────────────────────────────────────────────────
@app.get("/predictions/{mmsi}", tags=["Predictions"])
def get_prediction_by_mmsi(mmsi: str):
    """
    Retourne la dernière prédiction de position pour un navire.
    
    Ex: `/predictions/235097314` → où sera le VOS FAIRNESS dans 30min et 1h
    """
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
            raise HTTPException(status_code=404, detail=f"Pas de prédiction pour MMSI {mmsi}")
        return hits[0]["_source"]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────
# ROUTE 10 — Statistiques globales
# ─────────────────────────────────────────────────────────────
@app.get("/stats", tags=["Analytics"])
def get_stats():
    """
    Retourne les statistiques globales du système maritime.
    
    Inclut :
    - Nombre total de navires trackés
    - Vitesse moyenne, min, max
    - Nombre d'anomalies par sévérité
    - Top 5 pays avec le plus de navires
    - Navires actifs dans les 10 dernières minutes
    """
    es    = get_es()
    since = now_minus(10)

    # Requête avec agrégations Elasticsearch
    query = {
        "query": {"match_all": {}},
        "size":  0,  # On veut seulement les agrégations, pas les docs
        "aggs": {
            # Vitesse moyenne, min, max
            "speed_stats": {
                "stats": {"field": "speed"}
            },
            # Top 5 pays
            "top_countries": {
                "terms": {"field": "country.keyword", "size": 5}
            },
            # Navires actifs récemment
            "active_ships": {
                "filter": {
                    "range": {"ingested_at": {"gte": since}}
                }
            },
        }
    }

    # Requête séparée pour les anomalies par sévérité
    anom_query = {
        "query": {"match_all": {}},
        "size":  0,
        "aggs": {
            "by_severity": {
                "terms": {"field": "severity.keyword"}
            }
        }
    }

    try:
        ships_resp = es.search(index="ships",     body=query)
        anom_resp  = es.search(index="anomalies", body=anom_query)

        aggs         = ships_resp["aggregations"]
        speed_stats  = aggs["speed_stats"]
        top_countries = [
            {"country": b["key"], "count": b["doc_count"]}
            for b in aggs["top_countries"]["buckets"]
        ]
        active_count  = aggs["active_ships"]["doc_count"]

        # Anomalies par sévérité
        severity_counts = {
            b["key"]: b["doc_count"]
            for b in anom_resp["aggregations"]["by_severity"]["buckets"]
        }

        return {
            "ships": {
                "total":          ships_resp["hits"]["total"]["value"],
                "active_last_10m": active_count,
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


# ─────────────────────────────────────────────────────────────
# ROUTE 11 — Navires par pays
# ─────────────────────────────────────────────────────────────
@app.get("/countries", tags=["Analytics"])
def get_countries(
    limit: int = Query(default=20, ge=1, le=100)
):
    """
    Retourne le nombre de navires trackés par pays.
    
    Utilise les agrégations Elasticsearch pour grouper
    les navires par pays de manière très efficace.
    """
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


# ─────────────────────────────────────────────────────────────
# ROUTE 12 — Recherche par nom de navire
# ─────────────────────────────────────────────────────────────
@app.get("/search", tags=["Ships"])
def search_ships(
    q:     str = Query(..., description="Nom du navire à chercher ex: MAERSK"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """
    Recherche des navires par nom (full-text search).
    
    Utilise la recherche full-text d'Elasticsearch.
    La recherche est insensible à la casse.
    
    Exemples :
    - `/search?q=MAERSK` → tous les navires MAERSK
    - `/search?q=express` → tous les navires avec 'express' dans le nom
    """
    es = get_es()
    query = {
        "query": {
            "match": {
                "ship_name": {
                    "query":    q,
                    "operator": "and",
                    "fuzziness": "AUTO",  # tolère les fautes de frappe
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

        return {
            "query":    q,
            "total":    total,
            "returned": len(ships),
            "ships":    ships,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))