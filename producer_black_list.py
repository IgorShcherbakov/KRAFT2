import logging
from confluent_kafka import Producer
from black_list import BlackList, BlackListSerializer, BlackListDeserializer

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """
    Проверить статус доставки сообщения
    """
    if err is not None:
        logger.info(f"Ошибка доставки сообщения: {err}")
    else:
        deserializer = BlackListDeserializer()
        deserializer_msg = deserializer(msg.value())
        logger.info(f"Сообщение «{deserializer_msg.__str__()}» доставлено в {msg.topic()} [{msg.partition()}]")


def send_message(source_user_id: int, target_user_id: int) -> None:
    """
    Отправить сообщение о блокировке пользователя в топик "black_list"
    """

    serializer = BlackListSerializer()
    # конфигурация продюсера – адрес сервера
    conf = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all",  # Для синхронной репликации
        "retries": 3,  # Количество попыток при сбоях
    }
    # создание продюсера
    producer = Producer(conf)
    # генерация сообщения
    msg = BlackList(source_user_id, target_user_id)
    # попытка сериализации сообщения
    try:
        serialized_msg = serializer(msg)
    except Exception as e:
        logger.error(f"Ошибка сериализации: {str(e)}")
    # отправка сообщения
    producer.produce(
        topic="black_list",
        key="bu",
        value=serialized_msg,
        callback=delivery_report,
    )
    # ожидание завершения отправки всех сообщений
    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    send_message(1, 2)

    send_message(2, 3)

    send_message(3, 1)
