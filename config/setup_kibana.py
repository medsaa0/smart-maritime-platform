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
    log.info("Attente demarrage Kibana...")
    for i in range(max_wait // 5):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("status", {}).get("overall", {}).get("level") in ("available", "degraded"):
                    log.info("Kibana pret")
                    return True
        except Exception:
            pass
        log.info(f"  ... tentative {i+1}/{max_wait//5}")
        time.sleep(5)
    return False


def create_index_pattern(pattern_id: str, pattern: str, time_field: str = "timestamp"):
    url = f"{KIBANA_URL}/api/saved_objects/index-pattern/{pattern_id}"
    payload = {
        "attributes": {
            "title":         pattern,
            "timeFieldName": time_field,
        }
    }
    r = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    if r.status_code in (200, 409):
        log.info(f"Index pattern '{pattern}' cree (ou deja existant)")
        return True
    else:
        log.error(f"Erreur creation index pattern '{pattern}': {r.status_code} — {r.text[:200]}")
        return False


def set_default_index_pattern(pattern_id: str):
    url     = f"{KIBANA_URL}/api/kibana/settings"
    payload = {"changes": {"defaultIndex": pattern_id}}
    r       = requests.post(url, headers=HEADERS, json=payload, timeout=15)
    if r.status_code == 200:
        log.info(f"Index pattern par defaut defini: {pattern_id}")


def check_elasticsearch():
    try:
        r = requests.get(f"{ES_URL}/_cat/indices?format=json", timeout=10)
        if r.status_code == 200:
            indices = [idx["index"] for idx in r.json() if not idx["index"].startswith(".")]
            log.info(f"Elasticsearch OK | Indices: {indices}")
            return True
    except Exception as e:
        log.error(f"Elasticsearch inaccessible: {e}")
    return False


def check_data_count():
    for index in ["ships", "anomalies", "predictions"]:
        try:
            r = requests.get(f"{ES_URL}/{index}/_count", timeout=5)
            if r.status_code == 200:
                count = r.json().get("count", 0)
                log.info(f"   Index '{index}': {count} documents")
            else:
                log.info(f"   Index '{index}': non trouve (normal si ML pas encore lance)")
        except Exception:
            pass


def print_kibana_guide():
    guide = """
==========================================================
         GUIDE CREATION DASHBOARD KIBANA
==========================================================

1. Ouvre http://localhost:5601

2. CREER LA CARTE DES NAVIRES :
   -> Menu : Maps (ou Visualize -> Maps)
   -> "+ Add layer" -> Documents
   -> Index: ships*
   -> Geo field: location
   -> Tooltip: ship_name, speed, country
   -> Titre: "Carte Navires en Temps Reel"

3. CREER LE GRAPHIQUE DE VITESSE :
   -> Visualize -> Line
   -> Index: ships*
   -> Y-axis: Average of "speed"
   -> X-axis: Date Histogram "timestamp" (interval: 1 minute)
   -> Titre: "Vitesse Moyenne par Minute"

4. CREER LE CAMEMBERT PAR PAYS :
   -> Visualize -> Pie
   -> Index: ships*
   -> Buckets: Terms -> field "country.keyword"
   -> Size: 10
   -> Titre: "Navires par Pays"

5. CREER LE COMPTEUR DE NAVIRES ACTIFS :
   -> Visualize -> Metric
   -> Index: ships*
   -> Unique Count of "mmsi.keyword"
   -> Titre: "Navires Actifs"

6. CREER LE TABLEAU DES ANOMALIES :
   -> Visualize -> Data Table
   -> Index: anomalies*
   -> Columns: ship_name, anomaly_type, speed, severity, detected_at
   -> Titre: "Anomalies Detectees"

7. CREER LE DASHBOARD :
   -> Dashboard -> Create new
   -> Add all visualizations
   -> Auto-refresh: 30 secondes
   -> Save: "Maritime Intelligence Dashboard"

==========================================================
"""
    print(guide)


def main():
    log.info(f"Kibana Setup | {KIBANA_URL} | ES: {ES_URL}")

    check_elasticsearch()

    if not wait_for_kibana():
        log.error("Kibana n a pas demarre a temps")
        return

    time.sleep(3)

    create_index_pattern("ships-pattern",       "ships*",       "timestamp")
    create_index_pattern("anomalies-pattern",   "anomalies*",   "detected_at")
    create_index_pattern("predictions-pattern", "predictions*", "predicted_at")

    set_default_index_pattern("ships-pattern")

    log.info("Verification des donnees:")
    check_data_count()

    print_kibana_guide()

    log.info("Setup Kibana termine")


if __name__ == "__main__":
    main()