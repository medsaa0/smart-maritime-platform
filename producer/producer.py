"""
====================================================
PRODUCER AIS RÉEL — AISstream.io → KAFKA
====================================================
Connexion WebSocket à AISstream.io
Données 100% réelles — aucune simulation
Flux mondial : tous les navires en temps réel

Prérequis :
    pip install kafka-python websocket-client

Clé API : configurée dans ce fichier
====================================================
"""

import json
import time
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AIS_API_KEY             = "79e483b0dd095b2183d5db9c669b407244f10e7c"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "ais-raw"
AIS_WS_URL              = "wss://stream.aisstream.io/v0/stream"

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("producer")

# ─────────────────────────────────────────────────────────────
# COMPTEUR GLOBAL
# ─────────────────────────────────────────────────────────────
stats = {
    "received":   0,
    "sent":       0,
    "errors":     0,
    "start_time": time.time()
}

# ─────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────────────────────
def create_kafka_producer() -> KafkaProducer:
    max_retries = 20
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                retry_backoff_ms=500,
                request_timeout_ms=30000,
                connections_max_idle_ms=540000,
                linger_ms=100,
                batch_size=65536,
            )
            log.info(f"✅ Kafka connecté → {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"⏳ Kafka pas encore prêt... tentative {attempt}/{max_retries}")
            time.sleep(5)

    raise ConnectionError("❌ Impossible de connecter Kafka")


# ─────────────────────────────────────────────────────────────
# PARSERS PAR TYPE DE MESSAGE AIS
# ─────────────────────────────────────────────────────────────
def parse_position_report(data: dict):
    """
    Type 1/2/3 — Position Report Class A
    Navires commerciaux : cargos, tankers, passagers
    """
    meta = data.get("MetaData", {})
    pos  = data.get("Message", {}).get("PositionReport", {})

    mmsi = str(meta.get("MMSI", "")).strip()
    if not mmsi:
        return None

    lat = pos.get("Latitude", 0.0)
    lon = pos.get("Longitude", 0.0)

    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None
    if lat == 0.0 and lon == 0.0:
        return None

    speed   = float(pos.get("Sog", 0))
    heading = float(pos.get("TrueHeading", pos.get("Cog", 0)))
    course  = float(pos.get("Cog", 0))

    if speed   >= 102.3: speed   = 0.0
    if heading >= 360:   heading = course

    nav_status_map = {
        0: "Under way using engine", 1: "At anchor",
        2: "Not under command",      3: "Restricted manoeuvrability",
        4: "Constrained by draught", 5: "Moored",
        6: "Aground",                7: "Engaged in fishing",
        8: "Under way sailing",      15: "Not defined",
    }
    nav_code   = pos.get("NavigationalStatus", 15)
    nav_status = nav_status_map.get(nav_code, "Unknown")

    return {
        "mmsi":           mmsi,
        "ship_name":      meta.get("ShipName", "Unknown").strip() or "Unknown",
        "ship_type":      0,
        "ship_type_name": "Unknown",
        "latitude":       round(lat, 6),
        "longitude":      round(lon, 6),
        "speed":          round(speed, 2),
        "heading":        round(heading % 360, 1),
        "course":         round(course % 360, 1),
        "status":         nav_status,
        "rot":            pos.get("RateOfTurn", 0),
        "timestamp":      meta.get("time_utc", datetime.now(timezone.utc).isoformat()),
        "source":         "aisstream.io",
        "msg_type":       "PositionReport",
    }


def parse_class_b(data: dict):
    """
    Type 18 — Standard Class B CS Position Report
    Petits navires, voiliers, bateaux de pêche
    """
    meta = data.get("MetaData", {})
    pos  = data.get("Message", {}).get("StandardClassBPositionReport", {})

    mmsi = str(meta.get("MMSI", "")).strip()
    if not mmsi:
        return None

    lat = pos.get("Latitude", 0.0)
    lon = pos.get("Longitude", 0.0)

    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None
    if lat == 0.0 and lon == 0.0:
        return None

    speed   = float(pos.get("Sog", 0))
    heading = float(pos.get("TrueHeading", pos.get("Cog", 0)))
    course  = float(pos.get("Cog", 0))

    if speed   >= 102.3: speed   = 0.0
    if heading >= 360:   heading = course

    return {
        "mmsi":           mmsi,
        "ship_name":      meta.get("ShipName", "Unknown").strip() or "Unknown",
        "ship_type":      0,
        "ship_type_name": "Class B",
        "latitude":       round(lat, 6),
        "longitude":      round(lon, 6),
        "speed":          round(speed, 2),
        "heading":        round(heading % 360, 1),
        "course":         round(course % 360, 1),
        "status":         "Under way",
        "rot":            0,
        "timestamp":      meta.get("time_utc", datetime.now(timezone.utc).isoformat()),
        "source":         "aisstream.io",
        "msg_type":       "ClassBPosition",
    }


def parse_static_data(data: dict):
    """
    Type 5 — Static and Voyage Related Data
    Nom, type de navire, destination, dimensions
    """
    meta   = data.get("MetaData", {})
    static = data.get("Message", {}).get("ShipStaticData", {})

    mmsi = str(meta.get("MMSI", "")).strip()
    if not mmsi:
        return None

    ship_type_map = {
        30: "Fishing",       31: "Towing",        32: "Towing",
        33: "Dredging",      34: "Diving",         35: "Military",
        36: "Sailing",       37: "Pleasure craft", 40: "High speed craft",
        50: "Pilot vessel",  51: "SAR vessel",     52: "Tug",
        53: "Port tender",   55: "Law enforcement",
        60: "Passenger",     61: "Passenger",      69: "Passenger",
        70: "Cargo",         71: "Cargo",          72: "Cargo",
        73: "Cargo",         74: "Cargo",          79: "Cargo",
        80: "Tanker",        81: "Tanker",         82: "Tanker",
        83: "Tanker",        84: "Tanker",         89: "Tanker",
        90: "Other",
    }

    type_code = static.get("Type", 0)
    dim       = static.get("Dimension", {})

    return {
        "mmsi":           mmsi,
        "ship_name":      static.get("Name", "Unknown").strip() or "Unknown",
        "ship_type":      type_code,
        "ship_type_name": ship_type_map.get(type_code, f"Type {type_code}"),
        "callsign":       static.get("CallSign", "").strip(),
        "destination":    static.get("Destination", "").strip(),
        "draught":        static.get("MaximumStaticDraught", 0),
        "length":         dim.get("A", 0) + dim.get("B", 0),
        "width":          dim.get("C", 0) + dim.get("D", 0),
        "imo":            str(static.get("ImoNumber", "")),
        "eta":            static.get("Eta", ""),
        "timestamp":      meta.get("time_utc", datetime.now(timezone.utc).isoformat()),
        "source":         "aisstream.io",
        "msg_type":       "ShipStaticData",
    }


# ─────────────────────────────────────────────────────────────
# DISPATCHER
# ─────────────────────────────────────────────────────────────
PARSERS = {
    "PositionReport":               parse_position_report,
    "StandardClassBPositionReport": parse_class_b,
    "ShipStaticData":               parse_static_data,
}

def dispatch(data: dict):
    msg_type = data.get("MessageType", "")
    parser   = PARSERS.get(msg_type)
    if parser:
        return parser(data)
    return None


# ─────────────────────────────────────────────────────────────
# WEBSOCKET CALLBACKS
# ─────────────────────────────────────────────────────────────
producer_ref = None   # référence globale pour les callbacks

def on_open(ws):
    log.info("🌐 WebSocket AISstream.io connecté")
    log.info("📡 Souscription flux mondial en cours...")

    subscribe = {
        "APIKey": AIS_API_KEY,
        "BoundingBoxes": [
            [[-90, -180], [90, 180]]     # monde entier
        ],
        "FilterMessageTypes": [
            "PositionReport",
            "StandardClassBPositionReport",
            "ShipStaticData",
        ]
    }
    ws.send(json.dumps(subscribe))
    log.info("✅ Souscription envoyée — réception des données réelles en cours...")


def on_message(ws, message):
    global stats, producer_ref

    try:
        data = json.loads(message)
        stats["received"] += 1

        doc = dispatch(data)
        if doc is None:
            return

        mmsi = doc.get("mmsi", "")
        if not mmsi:
            return

        producer_ref.send(KAFKA_TOPIC, key=mmsi, value=doc)
        stats["sent"] += 1

        # Log toutes les 100 réceptions
        if stats["sent"] % 100 == 0:
            elapsed = time.time() - stats["start_time"]
            rate    = stats["sent"] / elapsed if elapsed > 0 else 0
            log.info(
                f"📊 Envoyés={stats['sent']:6d} | "
                f"Débit={rate:.1f} msg/s | "
                f"Dernier: {doc.get('ship_name','?'):25s} | "
                f"lat={doc.get('latitude', 0):8.3f} "
                f"lon={doc.get('longitude', 0):9.3f} | "
                f"speed={doc.get('speed', 0):5.1f} kts"
            )

    except json.JSONDecodeError:
        stats["errors"] += 1
    except Exception as e:
        log.error(f"Erreur traitement: {e}")
        stats["errors"] += 1


def on_error(ws, error):
    log.error(f"❌ WebSocket erreur: {error}")


def on_close(ws, close_status_code, close_msg):
    log.warning(f"🔌 WebSocket fermé — code={close_status_code}")


# ─────────────────────────────────────────────────────────────
# RECONNEXION AUTOMATIQUE
# ─────────────────────────────────────────────────────────────
def run_forever():
    """Lance le WebSocket avec reconnexion automatique."""
    while True:
        try:
            log.info("🔄 Connexion à AISstream.io...")
            ws = websocket.WebSocketApp(
                AIS_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
            )
        except Exception as e:
            log.error(f"Erreur WebSocket: {e}")

        log.info("⏳ Reconnexion dans 10 secondes...")
        time.sleep(10)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  🚢  MARITIME AIS PRODUCER — 100% DONNÉES RÉELLES")
    log.info("=" * 60)
    log.info(f"  Source   : AISstream.io (flux mondial)")
    log.info(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS} → topic '{KAFKA_TOPIC}'")
    log.info(f"  Messages : PositionReport + ClassB + StaticData")
    log.info("=" * 60)

    producer_ref = create_kafka_producer()

    try:
        run_forever()
    except KeyboardInterrupt:
        elapsed = time.time() - stats["start_time"]
        log.info(f"\n⏹️  Arrêt après {elapsed:.0f}s")
        log.info(f"   Reçus  : {stats['received']}")
        log.info(f"   Envoyés: {stats['sent']}")
        log.info(f"   Erreurs: {stats['errors']}")
        producer_ref.flush()
        producer_ref.close()