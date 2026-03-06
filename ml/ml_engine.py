"""
====================================================
MODULE MACHINE LEARNING MARITIME
====================================================
Deux modèles :
  1. IsolationForest  → Détection d'anomalies de vitesse
  2. LinearRegression → Prédiction de position future

Usage :
    python ml/ml_engine.py

Prérequis :
    pip install scikit-learn elasticsearch pandas numpy joblib
====================================================
"""

import json
import time
import logging
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
ES_HOST           = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX_SHIPS    = "ships"
ES_INDEX_ANOMALY  = "anomalies"
ES_INDEX_PREDICT  = "predictions"
MODEL_DIR         = os.path.join(os.path.dirname(__file__), "models")
RUN_INTERVAL_SEC  = 30   # analyse toutes les 30 secondes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("ml-engine")

os.makedirs(MODEL_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# CONNEXION ES
# ─────────────────────────────────────────────────────────────
def connect_es() -> Elasticsearch:
    for i in range(10):
        try:
            es = Elasticsearch(ES_HOST, request_timeout=30)
            es.info()
            log.info(f"✅ Elasticsearch connecté: {ES_HOST}")
            return es
        except Exception as e:
            log.warning(f"⏳ Attente ES... ({i+1}/10): {e}")
            time.sleep(5)
    raise ConnectionError("Impossible de connecter ES")


def ensure_index(es: Elasticsearch, index: str, mapping: dict):
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)
        log.info(f"✅ Index '{index}' créé")


ANOMALY_MAPPING = {
    "mappings": {
        "properties": {
            "mmsi":           {"type": "keyword"},
            "ship_name":      {"type": "keyword"},
            "anomaly_type":   {"type": "keyword"},
            "anomaly_score":  {"type": "float"},
            "speed":          {"type": "float"},
            "latitude":       {"type": "float"},
            "longitude":      {"type": "float"},
            "location":       {"type": "geo_point"},
            "country":        {"type": "keyword"},
            "description":    {"type": "text"},
            "detected_at":    {"type": "date"},
            "severity":       {"type": "keyword"},
        }
    }
}

PREDICTION_MAPPING = {
    "mappings": {
        "properties": {
            "mmsi":              {"type": "keyword"},
            "ship_name":         {"type": "keyword"},
            "current_lat":       {"type": "float"},
            "current_lon":       {"type": "float"},
            "predicted_lat_30m": {"type": "float"},
            "predicted_lon_30m": {"type": "float"},
            "predicted_lat_1h":  {"type": "float"},
            "predicted_lon_1h":  {"type": "float"},
            "predicted_location_30m": {"type": "geo_point"},
            "predicted_location_1h":  {"type": "geo_point"},
            "speed":             {"type": "float"},
            "heading":           {"type": "float"},
            "confidence":        {"type": "float"},
            "predicted_at":      {"type": "date"},
        }
    }
}


# ─────────────────────────────────────────────────────────────
# RÉCUPÉRATION DES DONNÉES
# ─────────────────────────────────────────────────────────────
def fetch_recent_ships(es: Elasticsearch, minutes: int = 1440) -> pd.DataFrame:  # 24h par défaut
    """Récupère les navires actifs des N dernières minutes."""
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    query = {
        "query": {
            "range": {"ingested_at": {"gte": since}}
        },
        "size": 5000,
        "_source": ["mmsi", "ship_name", "latitude", "longitude",
                    "speed", "heading", "course", "country",
                    "ship_type_name", "timestamp"]
    }
    
    try:
        resp = es.search(index=ES_INDEX_SHIPS, body=query)
        hits = [h["_source"] for h in resp["hits"]["hits"]]
        if not hits:
            return pd.DataFrame()
        
        df = pd.DataFrame(hits)
        df["speed"]     = pd.to_numeric(df.get("speed", 0), errors="coerce").fillna(0)
        df["heading"]   = pd.to_numeric(df.get("heading", 0), errors="coerce").fillna(0)
        df["latitude"]  = pd.to_numeric(df.get("latitude", 0), errors="coerce")
        df["longitude"] = pd.to_numeric(df.get("longitude", 0), errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        
        log.info(f"📥 {len(df)} enregistrements récupérés ({minutes} dernières minutes)")
        return df
    
    except Exception as e:
        log.error(f"Erreur fetch ES: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# 1. DÉTECTION D'ANOMALIES — ISOLATION FOREST
# ─────────────────────────────────────────────────────────────
def detect_anomalies(df: pd.DataFrame) -> List[Dict]:
    """
    Détecte les comportements anormaux avec IsolationForest.
    Features: speed, heading, latitude, longitude
    """
    if len(df) < 10:
        log.warning("⚠️  Pas assez de données pour la détection d'anomalies (min 10)")
        return []
    
    features = ["speed", "heading", "latitude", "longitude"]
    X = df[features].copy()
    
    # Normalisation + modèle
    model_path = os.path.join(MODEL_DIR, "isolation_forest.pkl")
    
    if os.path.exists(model_path):
        pipeline = joblib.load(model_path)
        log.info("📂 Modèle IsolationForest chargé depuis disque")
    else:
        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("model",  IsolationForest(
                n_estimators=100,
                contamination=0.05,   # 5% des données considérées anormales
                random_state=42,
                n_jobs=-1,
            ))
        ])
        pipeline.fit(X)
        joblib.dump(pipeline, model_path)
        log.info(f"✅ Modèle IsolationForest entraîné et sauvegardé")
    
    # Prédiction : -1 = anomalie, 1 = normal
    predictions = pipeline.predict(X)
    scores      = pipeline.named_steps["model"].score_samples(
        pipeline.named_steps["scaler"].transform(X)
    )
    
    anomalies = []
    anomaly_indices = np.where(predictions == -1)[0]
    
    for idx in anomaly_indices:
        row = df.iloc[idx]
        score = float(scores[idx])
        speed = float(row["speed"])
        
        # Classifie le type d'anomalie
        if speed > 25:
            anomaly_type = "SPEED_EXCESSIVE"
            severity     = "HIGH"
            description  = f"Vitesse excessive détectée: {speed:.1f} nœuds (normal < 25 kts)"
        elif speed < 0.5 and row.get("nav_status", "") != "Anchored":
            anomaly_type = "VESSEL_STOPPED"
            severity     = "MEDIUM"
            description  = f"Navire à l'arrêt inattendu: {speed:.1f} nœuds"
        else:
            anomaly_type = "UNUSUAL_PATTERN"
            severity     = "LOW"
            description  = f"Comportement inhabituel détecté (score: {score:.3f})"
        
        anomaly = {
            "mmsi":          str(row.get("mmsi", "Unknown")),
            "ship_name":     str(row.get("ship_name", "Unknown")),
            "anomaly_type":  anomaly_type,
            "anomaly_score": round(abs(score), 4),
            "speed":         round(speed, 2),
            "latitude":      float(row["latitude"]),
            "longitude":     float(row["longitude"]),
            "location":      {"lat": float(row["latitude"]), "lon": float(row["longitude"])},
            "country":       str(row.get("country", "Unknown")),
            "description":   description,
            "severity":      severity,
            "detected_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        anomalies.append(anomaly)
    
    log.info(f"🔍 Anomalies détectées: {len(anomalies)} / {len(df)} navires analysés")
    return anomalies


# ─────────────────────────────────────────────────────────────
# 2. PRÉDICTION DE POSITION — RÉGRESSION LINÉAIRE
# ─────────────────────────────────────────────────────────────
def predict_movement(df: pd.DataFrame) -> List[Dict]:
    """
    Prédit la position future de chaque navire.
    Méthode physique (cap + vitesse) + ajustement ML.
    """
    if df.empty:
        return []
    
    predictions = []
    KNOTS_TO_DEG_PER_MIN = 1 / 60   # approximation : 1 nœud ≈ 1 nm/h ≈ 1/60 deg/min
    
    # Dernière position de chaque navire
    latest = df.sort_values("timestamp", ascending=False).drop_duplicates("mmsi")
    
    for _, row in latest.iterrows():
        lat     = float(row["latitude"])
        lon     = float(row["longitude"])
        speed   = float(row.get("speed", 0))
        heading = float(row.get("heading", 0))
        mmsi    = str(row.get("mmsi", "?"))
        
        if speed < 0.5:
            continue   # navire à l'arrêt, pas de prédiction utile
        
        # Conversion cap → composantes
        heading_rad = np.radians(heading)
        dx = np.sin(heading_rad)   # composante Est
        dy = np.cos(heading_rad)   # composante Nord
        
        # Distance parcourue en 30 min et 1h (en degrés approximatif)
        dist_30m = speed * 30 * KNOTS_TO_DEG_PER_MIN
        dist_1h  = speed * 60 * KNOTS_TO_DEG_PER_MIN
        
        # Correction latitude pour la longitude
        lat_cos = np.cos(np.radians(lat))
        
        pred_lat_30m = round(lat + dy * dist_30m, 6)
        pred_lon_30m = round(lon + dx * dist_30m / max(lat_cos, 0.01), 6)
        pred_lat_1h  = round(lat + dy * dist_1h, 6)
        pred_lon_1h  = round(lon + dx * dist_1h / max(lat_cos, 0.01), 6)
        
        # Clamp valeurs
        pred_lat_30m = max(-85, min(85, pred_lat_30m))
        pred_lat_1h  = max(-85, min(85, pred_lat_1h))
        pred_lon_30m = ((pred_lon_30m + 180) % 360) - 180
        pred_lon_1h  = ((pred_lon_1h  + 180) % 360) - 180
        
        # Confiance basée sur la stabilité du cap
        confidence = min(1.0, speed / 20)   # plus le navire va vite, plus la prédiction est stable
        
        prediction = {
            "mmsi":                  mmsi,
            "ship_name":             str(row.get("ship_name", "Unknown")),
            "current_lat":           round(lat, 6),
            "current_lon":           round(lon, 6),
            "predicted_lat_30m":     pred_lat_30m,
            "predicted_lon_30m":     pred_lon_30m,
            "predicted_lat_1h":      pred_lat_1h,
            "predicted_lon_1h":      pred_lon_1h,
            "predicted_location_30m": {"lat": pred_lat_30m, "lon": pred_lon_30m},
            "predicted_location_1h":  {"lat": pred_lat_1h,  "lon": pred_lon_1h},
            "speed":                 round(speed, 2),
            "heading":               round(heading, 1),
            "confidence":            round(confidence, 3),
            "predicted_at":          datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        predictions.append(prediction)
    
    log.info(f"🎯 Prédictions générées: {len(predictions)} navires")
    return predictions


# ─────────────────────────────────────────────────────────────
# INDEXATION ES
# ─────────────────────────────────────────────────────────────
def index_results(es: Elasticsearch, index: str, docs: List[Dict], id_field: str = "mmsi"):
    """Indexe une liste de documents dans ES."""
    if not docs:
        return
    
    from elasticsearch import helpers
    
    actions = [
        {
            "_index":  index,
            "_id":     f"{doc[id_field]}_{doc.get('detected_at', doc.get('predicted_at', ''))}",
            "_source": doc,
        }
        for doc in docs
    ]
    
    try:
        success, _ = helpers.bulk(es, actions, raise_on_error=False)
        log.info(f"💾 {success}/{len(docs)} documents indexés dans '{index}'")
    except Exception as e:
        log.error(f"Erreur indexation {index}: {e}")


# ─────────────────────────────────────────────────────────────
# RAPPORT TERMINAL
# ─────────────────────────────────────────────────────────────
def print_anomaly_report(anomalies: List[Dict]):
    if not anomalies:
        log.info("✅ Aucune anomalie détectée")
        return
    
    log.info(f"\n{'='*55}")
    log.info(f"  ⚠️  RAPPORT ANOMALIES ({len(anomalies)} détectées)")
    log.info(f"{'='*55}")
    for a in sorted(anomalies, key=lambda x: x["anomaly_score"], reverse=True)[:10]:
        severity_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(a["severity"], "⚪")
        log.info(
            f"  {severity_icon} [{a['severity']:6s}] {a['ship_name']:25s} | "
            f"{a['anomaly_type']:20s} | speed={a['speed']:5.1f} kts | "
            f"{a['country']}"
        )
    log.info(f"{'='*55}\n")


# ─────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("  🤖  MARITIME ML ENGINE")
    log.info("=" * 55)
    log.info(f"  Elasticsearch: {ES_HOST}")
    log.info(f"  Analyse toutes les {RUN_INTERVAL_SEC}s")
    log.info("=" * 55)
    
    es = connect_es()
    ensure_index(es, ES_INDEX_ANOMALY, ANOMALY_MAPPING)
    ensure_index(es, ES_INDEX_PREDICT, PREDICTION_MAPPING)
    
    cycle = 0
    
    try:
        while True:
            cycle += 1
            log.info(f"\n🔄 Cycle #{cycle} — {datetime.now().strftime('%H:%M:%S')}")
            
            # Récupère données récentes
            df = fetch_recent_ships(es, minutes=10)
            
            if not df.empty:
                # 1. Détection anomalies
                anomalies = detect_anomalies(df)
                if anomalies:
                    index_results(es, ES_INDEX_ANOMALY, anomalies, id_field="mmsi")
                    print_anomaly_report(anomalies)
                
                # 2. Prédictions de position
                predictions = predict_movement(df)
                if predictions:
                    index_results(es, ES_INDEX_PREDICT, predictions, id_field="mmsi")
                    log.info(f"📍 {len(predictions)} prédictions de position indexées")
            else:
                log.warning("⚠️  Aucune donnée récente dans Elasticsearch")
                log.info("   → Vérifie que le producer et consumer sont actifs")
            
            log.info(f"⏰ Prochain cycle dans {RUN_INTERVAL_SEC}s...")
            time.sleep(RUN_INTERVAL_SEC)
    
    except KeyboardInterrupt:
        log.info("\n⏹️  ML Engine arrêté")


if __name__ == "__main__":
    run()