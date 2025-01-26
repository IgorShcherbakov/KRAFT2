import os
import faust
import logging
from confluent_kafka import serialization
from message import Message, MessageSerializer, MessageDeserializer
from black_list import BlackListDeserializer

logger = logging.getLogger(__name__)


# Настройка логирования
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# Конфигурация Faust-приложения
app = faust.App(
    "sfa",
    broker="localhost:9094",
    value_serializer="raw", # Работа с байтами (default: "json")
    store="memory://"
    #store="rocksdb://",
)

# таблица для хранения заблокированных пользователей
black_list_table = app.Table(
    "black_list",
    partitions=3,    # Количество партиций
    default=list,     # Функция или тип для пропущенных ключей
)

# таблица для хранения запрещенных слов
forbidden_words_table = app.Table(
    "forbidden_words",
    partitions=3,    # Количество партиций
    default=list,     # Функция или тип для пропущенных ключей
)

message_serializer = MessageSerializer()
message_deserializer = MessageDeserializer()
blacklist_deserializer = BlackListDeserializer()
forbidden_word_deserializer = serialization.StringDeserializer()

# топик сообщений для входных данных
messages_topic = app.topic("messages", key_type=str, value_type=bytes)
# топик заблокированных пользователей для входных данных
black_list_topic = app.topic("black_list", key_type=str, value_type=bytes)
# топик запрещенных слов для входных данных
forbidden_words_topic = app.topic("forbidden_words", key_type=str, value_type=bytes)

# топик обработанных сообщений для выходных данных
filtered_messages_topic = app.topic("filtered_messages", key_type=str, value_type=bytes)

async def censor_message(value: Message):
    """
    Функция для цензурирования сообщения
    """
    # проходим по списку запрещенных слов и маскируем их если они есть
    for forbidden_word in forbidden_words_table["forbidden_words"]:
        value.body = value.body.replace(forbidden_word, "*"*len(forbidden_word))
    logger.info(f"Сообщение после цензуры: {value.body}")
    # сериализуем цезурированное сообщение
    msg = message_serializer(value)
    # отправляем обработаннные данные в выходной топик
    await filtered_messages_topic.send(value=msg)


# Функция, реализующая потоковую обработку сообщений
@app.agent(messages_topic, sink=[censor_message])
async def process_messages(messages):
    async for message in messages.filter(lambda msg: message_deserializer(msg).sender_id not in black_list_table[message_deserializer(msg).recipient_id]):
        try:
            msg = message_deserializer(message)
            logger.info(f"Получено сообщение: {msg.__str__()}")
            yield msg
        except Exception as e:
            logger.exception(e)

# Функция, реализующая потоковую обработку заблокированных пользователей       
@app.agent(black_list_topic)
async def process_blocked_users(blacklist):
    async for bl in blacklist:
        try:
            msg = blacklist_deserializer(bl)
        except Exception as e:
            logger.exception(f"Ошибка десериализации: {str(e)}")
        
        # logger.info(f"Текущее значение {blacklist_table[msg.source_user_id]} ключа {msg.source_user_id}")
        
        # получаем текущий список заблокированных пользователей
        blocked_users = black_list_table[msg.source_user_id]

        # добавляем в список заблокированных указанного пользователя если его там нет
        if msg.target_user_id not in blocked_users:
            blocked_users.append(msg.target_user_id)
            logger.info(f"Пользователь {msg.source_user_id} заблокировал пользователя {msg.target_user_id}")

        # сохраняем новое значение
        black_list_table[msg.source_user_id] = blocked_users
        logger.info(f"Новый черный список пользователя {msg.source_user_id}: {black_list_table[msg.source_user_id]}")

# Функция, реализующая потоковую обработку запрещенных слов
@app.agent(forbidden_words_topic)
async def process_forbidden_words(forbidden_words):
    async for forbidden_word in forbidden_words:
        try:
            msg = forbidden_word_deserializer(forbidden_word)
        except Exception as e:
            logger.exception(f"Ошибка десериализации: {str(e)}")

        # получаем текущий список запрещенных слов
        forbidden_words = forbidden_words_table["forbidden_words"]

        # добавляем в список запрещенных слов указанное слово если его там нет
        if msg not in forbidden_words:
            forbidden_words.append(msg)
            logger.info(f"Добавляем новое запрещенное слово: {msg}")

        forbidden_words_table["forbidden_words"] = forbidden_words
        logger.info(f"Новый список запрещенных слов: {forbidden_words_table['forbidden_words']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    os.system("faust -A kafka_streams worker -l INFO")