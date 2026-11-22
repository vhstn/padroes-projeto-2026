"""Consumidor simples de eventos de pedidos no Apache Kafka."""

import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "pedidos-criados"
GROUP_ID = "grupo-notificacoes"


def main() -> None:
    """Lê os eventos publicados no tópico Kafka e simula o envio de notificações."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=5000,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"Conectado ao Kafka em {BOOTSTRAP_SERVERS} (grupo='{GROUP_ID}')")
    print(f"Aguardando mensagens do tópico '{TOPIC_NAME}'...\n")

    total = 0
    for message in consumer:
        total += 1
        pedido = message.value
        print(
            f"[CONSUMER] Recebido {message.key} | partição={message.partition} | "
            f"offset={message.offset} | cliente={pedido['cliente']} | "
            f"valor=R$ {pedido['valor']:.2f}"
        )

    consumer.close()
    print(f"\nConsumo finalizado. Total de mensagens processadas: {total}")


if __name__ == "__main__":
    main()
