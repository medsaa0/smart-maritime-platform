"""
====================================================
SETUP KIBANA — Index Pattern + Dashboard automatique
====================================================
Lance ce script UNE FOIS après démarrage de Kibana.
Il crée :
  - L'index pattern "ships*"
  - L'index pattern "anomalies*"
  - Un dashboard de base

Usage : python config/setup_kibana.py

Prérequis : pip install requests
====================================================
"""

import json
import time
import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kibana-setup")

KIBANA_URL = "http://localhost:5601"
ES_URL     = "http://localhost:9200"
HEADERS    = {"kbn-xsrf": "true", "Content-Type": "application/json"}


def wait_for_kibana(max_wait=120):
    """Attend que Kibana soit prêt."""
    log.info("⏳ Attente démarrage Kibana...")
    for i in range(max_wait // 5):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("status", {}).get("overall", {}).get("level") in ("available", "degraded"):
                    log.info("✅ Kibana prêt!")
                    return True
        except Exception:
            pass
        log.info(f"  ... tentative {i+1}/{max_wait//5}")
        time.sleep(5)
    return False


def create_index_pattern(pattern_id: str, pattern: str, time_field: str = "timestamp"):
    """Crée un index pattern dans Kibana."""
    url = f"{KIBANA_URL}/api/saved_objects/index-pattern/{pattern_id}"
    payload = {
        "attributes": {
            "title":         pattern,
            "timeFieldName": time_field,
        }
    }
    r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    if r.status_code in (200, 409):
        log.info(f"✅ Index pattern '{pattern}' créé (ou déjà existant)")
        return True
    else:
        log.error(f"❌ Erreur création index pattern '{pattern}': {r.status_code} — {r.text[:200]}")
        return False


def set_default_index_pattern(pattern_id: str):
    """Définit l'index pattern par défaut."""
    url = f"{KIBANA_URL}/api/kibana/settings"
    payload = {"changes": {"defaultIndex": pattern_id}}
    r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    if r.status_code == 200:
        log.info(f"✅ Index pattern par défaut défini: {pattern_id}")


def check_elasticsearch():
    """Vérifie qu'ES est accessible et a des données."""
    try:
        r = requests.get(f"{ES_URL}/_cat/indices?format=json", timeout=10)
        if r.status_code == 200:
            indices = [idx["index"] for idx in r.json() if not idx["index"].startswith(".")]
            log.info(f"✅ Elasticsearch OK | Indices: {indices}")
            return True
    except Exception as e:
        log.error(f"❌ Elasticsearch inaccessible: {e}")
    return False


def check_data_count():
    """Vérifie le nombre de documents dans chaque index."""
    for index in ["ships", "anomalies", "predictions"]:
        try:
            r = requests.get(f"{ES_URL}/{index}/_count", timeout=5)
            if r.status_code == 200:
                count = r.json().get("count", 0)
                log.info(f"   Index '{index}': {count} documents")
            else:
                log.info(f"   Index '{index}': non trouvé (normal si ML pas encore lancé)")
        except Exception:
            pass


def print_kibana_guide():
    """Affiche le guide pour créer le dashboard manuellement."""
    guide = """
╔══════════════════════════════════════════════════════════╗
║         GUIDE CRÉATION DASHBOARD KIBANA                 ║
╚══════════════════════════════════════════════════════════╝

1. Ouvre http://localhost:5601

2. CRÉER LA CARTE DES NAVIRES :
   → Menu : Maps (ou Visualize → Maps)
   → "+ Add layer" → Documents
   → Index: ships*
   → Geo field: location
   → Tooltip: ship_name, speed, country
   → Titre: "🗺️ Carte Navires en Temps Réel"

3. CRÉER LE GRAPHIQUE DE VITESSE :
   → Visualize → Line
   → Index: ships*
   → Y-axis: Average of "speed"
   → X-axis: Date Histogram "timestamp" (interval: 1 minute)
   → Titre: "📈 Vitesse Moyenne par Minute"

4. CRÉER LE CAMEMBERT PAR PAYS :
   → Visualize → Pie
   → Index: ships*
   → Buckets: Terms → field "country.keyword"
   → Size: 10
   → Titre: "🌍 Navires par Pays"

5. CRÉER LE COMPTEUR DE NAVIRES ACTIFS :
   → Visualize → Metric
   → Index: ships*
   → Unique Count of "mmsi.keyword"
   → Titre: "🚢 Navires Actifs"

6. CRÉER LE TABLEAU DES ANOMALIES :
   → Visualize → Data Table
   → Index: anomalies*
   → Columns: ship_name, anomaly_type, speed, severity, detected_at
   → Titre: "⚠️ Anomalies Détectées"

7. CRÉER LE DASHBOARD :
   → Dashboard → Create new
   → Add all visualizations
   → Auto-refresh: 30 secondes
   → Save: "Maritime Intelligence Dashboard"

══════════════════════════════════════════════════════════
"""
    print(guide)


def main():
    log.info("=" * 55)
    log.info("  🔧  KIBANA SETUP")
    log.info("=" * 55)
    
    # Check ES
    check_elasticsearch()
    
    # Wait Kibana
    if not wait_for_kibana():
        log.error("❌ Kibana n'a pas démarré à temps")
        return
    
    time.sleep(3)  # petit délai supplémentaire
    
    # Crée index patterns
    create_index_pattern("ships-pattern",      "ships*",      "timestamp")
    create_index_pattern("anomalies-pattern",  "anomalies*",  "detected_at")
    create_index_pattern("predictions-pattern","predictions*","predicted_at")
    
    # Définit ships comme défaut
    set_default_index_pattern("ships-pattern")
    
    # Check données
    log.info("\n📊 Vérification des données :")
    check_data_count()
    
    # Guide dashboard
    print_kibana_guide()
    
    log.info("=" * 55)
    log.info("✅ Setup Kibana terminé!")
    log.info(f"   🌐 Kibana: {KIBANA_URL}")
    log.info(f"   📡 ES:     {ES_URL}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()