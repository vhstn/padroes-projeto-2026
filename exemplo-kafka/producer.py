"""Produtor simples de eventos de pedidos para o Apache Kafka."""

import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "pedidos-criados"

PEDIDOS = [
    {"id_pedido": "PED-1001", "cliente": "Ana", "valor": 149.90},
    {"id_pedido": "PED-1002", "cliente": "Bruno", "valor": 89.50},
    {"id_pedido": "PED-1003", "cliente": "Carla", "valor": 320.00},
    {"id_pedido": "PED-1004", "cliente": "Diego", "valor": 54.00},
    {"id_pedido": "PED-1005", "cliente": "Eduarda", "valor": 210.75},
]


def main() -> None:
    """Envia uma lista de pedidos simulados para o tópico Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Conectado ao Kafka em {BOOTSTRAP_SERVERS}")
    print(f"Publicando {len(PEDIDOS)} eventos no tópico '{TOPIC_NAME}'...\n")

    for pedido in PEDIDOS:
        evento = {
            **pedido,
            "criado_em": datetime.now(timezone.utc).isoformat(),
        }
        future = producer.send(
            TOPIC_NAME,
            key=pedido["id_pedido"],
            value=evento,
        )
        metadata = future.get(timeout=10)
        print(
            f"[PRODUCER] Enviado {pedido['id_pedido']} ({pedido['cliente']}) -> "
            f"tópico={metadata.topic}, partição={metadata.partition}, offset={metadata.offset}"
        )
        time.sleep(0.3)

    producer.flush()
    producer.close()
    print("\nEnvio concluído com sucesso.")


if __name__ == "__main__":
    main()
