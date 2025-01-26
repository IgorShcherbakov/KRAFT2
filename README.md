# KRAFT2
Итоговый проект второго модуля (Разработка упрощённого сервиса обмена сообщениями).

## Установка
1. Клонировать репозиторий
2. Создать виртуальное окружение
```bash
python -m venv venv
```
3. Активировать виртуальное окружение
```bash
venv\Scripts\Activate.ps1
```
4. Установить зависимости
```bash
pip install -r requirements.txt
```
5. Запустить Kafka в Docker
```bash
docker-compose up -d
```
6. Подключиться к контейнеру и создать топики
```bash
kafka-topics.sh --create --topic messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
kafka-topics.sh --create --topic black_list --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
kafka-topics.sh --create --topic forbidden_words --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
kafka-topics.sh --create --topic filtered_messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
```

### Примеры использования
# запуск kafka_streams.py (faust приложение)
```bash
python kafka_streams.py
# так же есть альтернативный вариант (выполнить команту в терминале)
faust -A kafka_streams worker -l INFO
```

# запуск producer_message.py (моделируем поток сообщений)
```bash
python producer_message.py
```

# запуск producer_black_list.py (моделируем занесение пользователей в черный список)
```bash
python producer_black_list.py
```

# запуск producer_forbidden_words.py (моделируем добавление запрещенных слов)
```bash
python producer_forbidden_words.py
```

1. После запуска kafka_streams.py видим в консоли информацию о топиках, партициях и прочее.
2. После запуска producer_message.py видим что в консоль начинает приходить информация о отправленных сообщения, например, "Получено сообщение: id: 1, sender_id: 1, recipient_id: 2, subject: 2025-01-26, body: На часах сейчас - 2025-01-26 21:36:52.915074".
3. После запуска producer_black_list.py видим что часть сообщений перестает приходить (заблокированные пользователи начинают отсеиваться).
4. После запуска producer_forbidden_words.py видим что текст сообщений меняется (запрещенные слова заменены на "*").
5. Кроме консоли можно увидеть результат в UI (проверить топик "filtered_messages")