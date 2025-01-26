import logging
import time
from datetime import datetime, date
from confluent_kafka import Producer
from message import Message, MessageSerializer, MessageDeserializer

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """
    Проверить статус доставки сообщения
    """
    if err is not None:
        logger.info(f"Ошибка доставки сообщения: {err}")
    else:
        deserializer = MessageDeserializer()
        deserializer_msg = deserializer(msg.value())
        logger.info(f"Сообщение «{deserializer_msg.__str__()}» доставлено в {msg.topic()} [{msg.partition()}]")


def start_sending_messages(timeout: int = 5) -> None:
    """
    Начать отправку сообщений в топик "messages" с указанным интервалом
    """
    counter: int = 0
    serializer = MessageSerializer()
    # конфигурация продюсера – адрес сервера
    conf = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all",  # Для синхронной репликации
        "retries": 3,  # Количество попыток при сбоях
    }
    # создание продюсера
    producer = Producer(conf)
    while True:
        counter += 1
        # моделируем ситуацию с 3 пользователями, каждый по очереди пишем всем
        for source_user_id in range(3):
            for target_user_id in range(3):
                if source_user_id == target_user_id:
                    continue
                else:
                    # генерация сообщения
                    msg = Message(counter, source_user_id + 1, target_user_id + 1, f"{date.today()}", f"На часах сейчас - {datetime.now()}")
                    # попытка сериализации сообщения
                    try:
                        serialized_msg = serializer(msg)
                    except Exception as e:
                        logger.error(f"Ошибка сериализации: {str(e)}")
                        continue
                    # отправка сообщения
                    producer.produce(
                        topic="messages",
                        key="msg",
                        value=serialized_msg,
                        callback=delivery_report,
                    )
                    # ожидание завершения отправки всех сообщений
                    producer.flush()
            
            
        logger.info(f"Ждем {timeout} секунд...")
        time.sleep(timeout)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    start_sending_messages(60)
