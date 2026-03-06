"""
====================================================
CONSUMER AIS : KAFKA → ELASTICSEARCH
====================================================
"""

import json
import time
import logging
import os
import multiprocessing
import reverse_geocoder as rg          # import direct au niveau module
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ESConnectionError

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC             = "ais-raw"
KAFKA_GROUP_ID          = "maritime-consumer-group"
ES_HOST                 = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX                = "ships"
BATCH_SIZE              = 50
BATCH_TIMEOUT           = 5

# ─────────────────────────────────────────────────────────────
# LOGGING — niveau WARNING pour les libs tierces
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
# Silence les logs internes de reverse_geocoder et elasticsearch
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("reverse_geocoder").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log = logging.getLogger("consumer")

# ─────────────────────────────────────────────────────────────
# PAYS
# ─────────────────────────────────────────────────────────────
COUNTRY_CODES = {
    "FR": "France",          "ES": "Espagne",       "IT": "Italie",
    "DE": "Allemagne",       "GB": "Royaume-Uni",   "NL": "Pays-Bas",
    "BE": "Belgique",        "PT": "Portugal",      "GR": "Grece",
    "TR": "Turquie",         "EG": "Egypte",        "SA": "Arabie Saoudite",
    "AE": "Emirats Arabes",  "SG": "Singapour",     "CN": "Chine",
    "JP": "Japon",           "KR": "Coree du Sud",  "US": "Etats-Unis",
    "CA": "Canada",          "BR": "Bresil",        "AU": "Australie",
    "IN": "Inde",            "ZA": "Afrique du Sud","MA": "Maroc",
    "NG": "Nigeria",         "KE": "Kenya",         "DK": "Danemark",
    "NO": "Norvege",         "SE": "Suede",         "FI": "Finlande",
    "PL": "Pologne",         "RU": "Russie",        "UA": "Ukraine",
    "CY": "Chypre",          "MT": "Malte",         "HR": "Croatie",
    "PA": "Panama",          "LR": "Liberia",       "MH": "Iles Marshall",
    "BS": "Bahamas",         "BM": "Bermudes",      "KW": "Koweit",
    "QA": "Qatar",           "OM": "Oman",          "IR": "Iran",
    "PK": "Pakistan",        "BD": "Bangladesh",    "MM": "Myanmar",
    "TH": "Thailande",       "VN": "Vietnam",       "PH": "Philippines",
    "ID": "Indonesie",       "MY": "Malaisie",
}

def get_country(lat: float, lon: float) -> Dict[str, str]:
    """Géolocalisation inverse — retourne pays et région."""
    try:
        results = rg.search([(lat, lon)], verbose=False)
        if results:
            r  = results[0]
            cc = r.get("cc", "??")
            return {
                "country_code": cc,
                "country":      COUNTRY_CODES.get(cc, r.get("name", "Unknown")),
                "region":       r.get("admin1", "Unknown"),
            }
    except Exception:
        pass
    return {"country_code": "??", "country": "Unknown", "region": "Unknown"}


# ─────────────────────────────────────────────────────────────
# VALIDATION & NETTOYAGE
# ─────────────────────────────────────────────────────────────
def validate_and_clean(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mmsi = str(raw.get("mmsi", "")).strip()
        if not mmsi or len(mmsi) < 6:
            return None

        lat = float(raw.get("latitude", 0))
        lon = float(raw.get("longitude", 0))

        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return None
        if lat == 0.0 and lon == 0.0:
            return None

        speed   = float(raw.get("speed",   0))
        speed   = max(0.0, min(50.0, speed))
        heading = float(raw.get("heading", 0)) % 360

        # ── Timestamp : format ISO 8601 strict pour Elasticsearch ──
        ts_raw = raw.get("timestamp", "")
        try:
            if ts_raw:
                # Garde seulement les 19 premiers chars puis ajoute Z
                ts_clean = ts_raw[:19].replace(" ", "T")
                ts = ts_clean + "Z"                # ex: "2024-01-15T14:30:00Z"
            else:
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "mmsi":           mmsi,
            "ship_name":      str(raw.get("ship_name", "Unknown")).strip() or "Unknown",
            "ship_type":      int(raw.get("ship_type", 0)),
            "ship_type_name": str(raw.get("ship_type_name", "Unknown")),
            "latitude":       round(lat, 6),
            "longitude":      round(lon, 6),
            "location":       {"lat": round(lat, 6), "lon": round(lon, 6)},
            "speed":          round(speed, 2),
            "heading":        round(heading, 1),
            "course":         float(raw.get("course", heading)),
            "nav_status":     str(raw.get("status", "Unknown")),
            "timestamp":      ts,
            "source":         str(raw.get("source", "unknown")),
        }

    except (ValueError, TypeError) as e:
        log.debug(f"Validation échouée: {e}")
        return None


def enrich_with_geo(doc: Dict[str, Any]) -> Dict[str, Any]:
    geo = get_country(doc["latitude"], doc["longitude"])
    doc.update(geo)
    doc["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return doc


# ─────────────────────────────────────────────────────────────
# ELASTICSEARCH
# ─────────────────────────────────────────────────────────────
ES_MAPPING = {
    "mappings": {
        "properties": {
            "mmsi":           {"type": "keyword"},
            "ship_name":      {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "ship_type":      {"type": "integer"},
            "ship_type_name": {"type": "keyword"},
            "location":       {"type": "geo_point"},
            "latitude":       {"type": "float"},
            "longitude":      {"type": "float"},
            "speed":          {"type": "float"},
            "heading":        {"type": "float"},
            "course":         {"type": "float"},
            "nav_status":     {"type": "keyword"},
            "country_code":   {"type": "keyword"},
            "country":        {"type": "keyword"},
            "region":         {"type": "keyword"},
            "timestamp":      {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ssZ||strict_date_time||epoch_millis"},
            "ingested_at":    {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ssZ||strict_date_time||epoch_millis"},
            "source":         {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "5s",
    }
}

def connect_elasticsearch() -> Elasticsearch:
    for attempt in range(1, 21):
        try:
            es = Elasticsearch(
                ES_HOST,
                request_timeout=30,
                retry_on_timeout=True,
                max_retries=3,
            )
            info = es.info()
            log.info(f"✅ Elasticsearch connecté | version: {info['version']['number']}")
            return es
        except Exception as e:
            log.warning(f"⏳ ES pas encore prêt... tentative {attempt}/20: {e}")
            time.sleep(5)
    raise ConnectionError("❌ Impossible de connecter Elasticsearch")


def setup_index(es: Elasticsearch):
    """Recrée l'index avec le bon mapping si timestamp était mal typé."""
    if es.indices.exists(index=ES_INDEX):
        # Vérifie que le mapping timestamp est correct
        mapping = es.indices.get_mapping(index=ES_INDEX)
        ts_type = (mapping.get(ES_INDEX, {})
                   .get("mappings", {})
                   .get("properties", {})
                   .get("timestamp", {})
                   .get("type", ""))
        if ts_type != "date":
            log.warning("⚠️  Mapping timestamp incorrect — suppression et recréation de l'index")
            es.indices.delete(index=ES_INDEX)
            es.indices.create(index=ES_INDEX, body=ES_MAPPING)
            log.info(f"✅ Index '{ES_INDEX}' recréé avec mapping corrigé")
        else:
            log.info(f"✅ Index '{ES_INDEX}' OK")
    else:
        es.indices.create(index=ES_INDEX, body=ES_MAPPING)
        log.info(f"✅ Index '{ES_INDEX}' créé")


# ─────────────────────────────────────────────────────────────
# BULK INDEX
# ─────────────────────────────────────────────────────────────
def bulk_index(es: Elasticsearch, batch: list) -> int:
    if not batch:
        return 0

    actions = [
        {
            "_index": ES_INDEX,
            "_id":    f"{doc['mmsi']}_{doc['timestamp']}",
            "_source": doc,
        }
        for doc in batch
    ]

    try:
        success, errors = helpers.bulk(
            es, actions,
            raise_on_error=False,
            chunk_size=100,
        )
        if errors:
            log.warning(f"⚠️  {len(errors)} erreurs bulk — premier: {errors[0]}")
        return success
    except Exception as e:
        log.error(f"Erreur bulk: {e}")
        return 0


# ─────────────────────────────────────────────────────────────
# KAFKA CONSUMER
# ─────────────────────────────────────────────────────────────
def create_kafka_consumer() -> KafkaConsumer:
    for attempt in range(1, 21):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=False,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
                fetch_max_wait_ms=500,
            )
            log.info(f"✅ Kafka Consumer connecté | topic: {KAFKA_TOPIC} | group: {KAFKA_GROUP_ID}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka pas encore prêt... tentative {attempt}/20")
            time.sleep(5)
    raise ConnectionError("❌ Impossible de connecter Kafka")


# ─────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.received = 0
        self.valid    = 0
        self.indexed  = 0
        self.invalid  = 0
        self.start    = time.time()

    def log(self):
        elapsed = time.time() - self.start
        rate = self.indexed / elapsed if elapsed > 0 else 0
        log.info(
            f"📊 reçus={self.received} | valides={self.valid} | "
            f"indexés={self.indexed} | invalides={self.invalid} | "
            f"débit={rate:.1f} msg/s"
        )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("  🚢  MARITIME AIS CONSUMER")
    log.info("=" * 55)
    log.info(f"  Kafka : {KAFKA_BOOTSTRAP_SERVERS} → {KAFKA_TOPIC}")
    log.info(f"  ES    : {ES_HOST} → {ES_INDEX}")
    log.info("=" * 55)

    es       = connect_elasticsearch()
    setup_index(es)
    consumer = create_kafka_consumer()

    stats      = Stats()
    batch      = []
    last_flush = time.time()

    log.info("🔄 En attente de messages Kafka...")

    try:
        for message in consumer:
            raw = message.value
            stats.received += 1

            doc = validate_and_clean(raw)
            if doc is None:
                stats.invalid += 1
                continue

            stats.valid += 1
            doc = enrich_with_geo(doc)
            batch.append(doc)

            # Log chaque navire reçu
            log.info(
                f"🚢 {doc['ship_name']:25s} | "
                f"lat={doc['latitude']:8.3f} lon={doc['longitude']:9.3f} | "
                f"speed={doc['speed']:5.1f} kts | {doc.get('country','?')}"
            )

            now = time.time()
            if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_TIMEOUT:
                indexed = bulk_index(es, batch)
                stats.indexed += indexed
                log.info(f"💾 Batch indexé: {indexed}/{len(batch)} | Total: {stats.indexed}")
                consumer.commit()
                batch.clear()
                last_flush = now

            if stats.received % 200 == 0:
                stats.log()

    except KeyboardInterrupt:
        log.info("\n⏹️  Arrêt")
    except Exception as e:
        log.error(f"❌ Erreur critique: {e}", exc_info=True)
    finally:
        if batch:
            indexed = bulk_index(es, batch)
            stats.indexed += indexed
        consumer.commit()
        consumer.close()
        stats.log()
        log.info("👋 Consumer arrêté")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    # Silence worker process logs from reverse_geocoder
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("consumer").setLevel(logging.INFO)
    run()