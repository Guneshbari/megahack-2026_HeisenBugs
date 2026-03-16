import json
import logging
from configparser import ConfigParser
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaConsumer # type: ignore
import psycopg2
from psycopg2.extras import Json

from shared_constants import DB_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('kafka_to_postgres')

# DB_CONFIG imported from shared_constants

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'sentinel-events'
KAFKA_GROUP_ID = 'postgres-ingester-group'

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def setup_database(conn):
    """Ensure tables exist."""
    with conn.cursor() as cur:
        # Create events table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                system_id VARCHAR(100),
                hostname VARCHAR(255),
                log_channel VARCHAR(100),
                event_record_id BIGINT,
                provider_name VARCHAR(255),
                event_id INTEGER,
                level INTEGER,
                task INTEGER,
                opcode INTEGER,
                keywords VARCHAR(50),
                process_id INTEGER,
                thread_id INTEGER,
                severity VARCHAR(20),
                fault_type VARCHAR(50),
                diagnostic_context JSONB,
                event_hash VARCHAR(64) UNIQUE,
                raw_xml TEXT,
                cpu_usage_percent NUMERIC(5,2),
                memory_usage_percent NUMERIC(5,2),
                disk_free_percent NUMERIC(5,2),
                ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_events_system_id ON events(system_id);
            CREATE INDEX IF NOT EXISTS idx_events_ingested_at ON events(ingested_at DESC);
        """)

        # Create system_heartbeats table for live tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_heartbeats (
                system_id VARCHAR(100) PRIMARY KEY,
                hostname VARCHAR(255),
                cpu_usage_percent NUMERIC(5,2),
                memory_usage_percent NUMERIC(5,2),
                disk_free_percent NUMERIC(5,2),
                os_version VARCHAR(255),
                agent_version VARCHAR(50),
                ip_address VARCHAR(50),
                uptime_seconds BIGINT,
                last_seen TIMESTAMP WITH TIME ZONE
            );
        """)
        conn.commit()
    logger.info("Database schema verified.")

def process_message(conn, msg: Dict[str, Any]):
    """Process a single Kafka payload."""
    system_id = msg.get('system_id', 'unknown')
    hostname = msg.get('hostname', 'unknown')
    sys_info = msg.get('system_info', {})
    
    # 1. Always update the heartbeat table to maintain "live" online status
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO system_heartbeats (
                system_id, hostname, cpu_usage_percent, memory_usage_percent, 
                disk_free_percent, os_version, agent_version, ip_address, 
                uptime_seconds, last_seen
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (system_id) DO UPDATE SET
                hostname = EXCLUDED.hostname,
                cpu_usage_percent = EXCLUDED.cpu_usage_percent,
                memory_usage_percent = EXCLUDED.memory_usage_percent,
                disk_free_percent = EXCLUDED.disk_free_percent,
                os_version = EXCLUDED.os_version,
                agent_version = EXCLUDED.agent_version,
                ip_address = EXCLUDED.ip_address,
                uptime_seconds = EXCLUDED.uptime_seconds,
                last_seen = EXCLUDED.last_seen;
        """, (
            system_id,
            hostname,
            float(sys_info.get('cpu_usage_percent', 0.0) if 'cpu_usage_percent' in sys_info else (msg['events'][0]['cpu_usage_percent'] if msg.get('events') else 0)),
            float(sys_info.get('memory_usage_percent', 0.0) if 'memory_usage_percent' in sys_info else (msg['events'][0]['memory_usage_percent'] if msg.get('events') else 0)),
            float(sys_info.get('disk_free_percent', 0.0) if 'disk_free_percent' in sys_info else (msg['events'][0]['disk_free_percent'] if msg.get('events') else 0)),
            sys_info.get('os_version', 'Unknown'),
            sys_info.get('agent_version', 'Unknown'),
            sys_info.get('ip_address', 'Unknown'),
            sys_info.get('uptime_seconds', 0),
            datetime.now(timezone.utc)
        ))
    
    # 2. Insert any actual Windows events
    events = msg.get('events', [])
    if events:
        with conn.cursor() as cur:
            for ev in events:
                try:
                    cur.execute("""
                        INSERT INTO events (
                            system_id, hostname, log_channel, event_record_id, provider_name,
                            event_id, level, task, opcode, keywords, process_id, thread_id,
                            severity, fault_type, diagnostic_context, event_hash, raw_xml,
                            cpu_usage_percent, memory_usage_percent, disk_free_percent
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_hash) DO NOTHING;
                    """, (
                        system_id,
                        hostname,
                        ev.get('log_channel'),
                        ev.get('event_record_id'),
                        ev.get('provider_name'),
                        ev.get('event_id'),
                        ev.get('level'),
                        ev.get('task'),
                        ev.get('opcode'),
                        ev.get('keywords'),
                        ev.get('process_id'),
                        ev.get('thread_id'),
                        ev.get('severity'),
                        ev.get('fault_type'),
                        Json(ev.get('diagnostic_context', {})),
                        ev.get('event_hash'),
                        ev.get('raw_xml'),
                        ev.get('cpu_usage_percent'),
                        ev.get('memory_usage_percent'),
                        ev.get('disk_free_percent')
                    ))
                except Exception as e:
                    logger.error(f"Failed to insert event {ev.get('event_hash')}: {e}")
    
    conn.commit()

def run_consumer():
    conn = get_db_connection()
    setup_database(conn)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info(f"Listening to Kafka topic '{KAFKA_TOPIC}' acting as syncing bridge...")

    try:
        for message in consumer:
            payload = message.value
            process_message(conn, payload)
            events_count = len(payload.get('events', []))
            
            logger.info(f"✓ Processed payload from {payload.get('hostname')} | Heartbeat updated | Events ingested: {events_count}")
            
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    run_consumer()