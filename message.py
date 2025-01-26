from confluent_kafka.serialization import Serializer, Deserializer


class Message:
    """Класс для объекта "сообщение".
    
    Attributes:
        id (int): идентификатор сообщения
        sender_id (int): отправитель сообщения
        recipient_id (int): получатель сообщения
        subject (str): тема сообщения
        body (str): текст сообщения
        
    Example:
        >>> msg = Message(1, 2, 3, "2023-01-01", "Тестовое сообщение")
        >>> print(msg.id)
        1
    """
    def __init__(self, id: int, sender_id: int, recipient_id: int, subject: str, body: str):
        self.id = id
        self.sender_id = sender_id
        self.recipient_id = recipient_id
        self.subject = subject
        self.body = body

    def __str__(self) -> str:
        return f"id: {self.id}, sender_id: {self.sender_id}, recipient_id: {self.recipient_id}, subject: {self.subject}, body: {self.body}"


class MessageSerializer(Serializer):
    """
    Сериализатор для объектов Message
    """
    def __call__(self, obj: Message, ctx=None):
        id_bytes = obj.id.to_bytes(4, byteorder="big")
        sender_id_bytes = obj.sender_id.to_bytes(4, byteorder="big")
        recipient_id_bytes = obj.recipient_id.to_bytes(4, byteorder="big")

        subject_bytes = obj.subject.encode("utf-8")
        subject_size = len(subject_bytes)

        body_bytes = obj.body.encode("utf-8")
        body_size = len(body_bytes)

        result = id_bytes
        result += sender_id_bytes
        result += recipient_id_bytes

        result += subject_size.to_bytes(4, byteorder="big")
        result += subject_bytes

        result += body_size.to_bytes(4, byteorder="big")
        result += body_bytes

        return result


class MessageDeserializer(Deserializer):
    """
    Десериализатор для объектов Message
    """
    def __call__(self, value: bytes, ctx=None):
        if value is None:
            return None

        id_bytes = value[0:4]
        id_value = int.from_bytes(id_bytes, byteorder="big")

        sender_id_bytes = value[4:8]
        sender_id_value = int.from_bytes(sender_id_bytes, byteorder="big")

        recipient_id_bytes = value[8:12]
        recipient_id_value = int.from_bytes(recipient_id_bytes, byteorder="big")

        subject_size = int.from_bytes(value[12:16], byteorder="big")
        subject_bytes = value[16:16 + subject_size]
        subject_value = subject_bytes.decode("utf-8")

        body_size = int.from_bytes(value[16 + subject_size:20 + subject_size], byteorder="big")
        body_bytes = value[20 + subject_size:20 + subject_size + body_size]
        body_value = body_bytes.decode("utf-8")

        return Message(id_value, sender_id_value, recipient_id_value, subject_value, body_value)
