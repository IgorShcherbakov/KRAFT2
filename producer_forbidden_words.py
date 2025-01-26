import logging
from confluent_kafka import Producer, serialization

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """
    Проверить статус доставки сообщения
    """
    if err is not None:
        logger.info(f"Ошибка доставки сообщения: {err}")
    else:
        deserializer = serialization.StringDeserializer()
        deserializer_msg = deserializer(msg.value())
        logger.info(f"Сообщение «{deserializer_msg}» доставлено в {msg.topic()} [{msg.partition()}]")


def send_message(word: str) -> None:
    """
    Отправить сообщение о запрещенном слове в топик "forbidden_words"
    """

    serializer = serialization.StringSerializer()
    # конфигурация продюсера – адрес сервера
    conf = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all",  # Для синхронной репликации
        "retries": 3,  # Количество попыток при сбоях
    }
    # создание продюсера
    producer = Producer(conf)
    # попытка сериализации сообщения
    try:
        serialized_msg = serializer(word)
    except Exception as e:
        logger.error(f"Ошибка сериализации: {str(e)}")
    # отправка сообщения
    producer.produce(
        topic="forbidden_words",
        key="fw",
        value=serialized_msg,
        callback=delivery_report,
    )
    # ожидание завершения отправки всех сообщений
    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    send_message("сейчас")
    send_message("На")
