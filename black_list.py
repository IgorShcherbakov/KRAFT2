from confluent_kafka.serialization import Serializer, Deserializer


class BlackList:
    """Класс для объекта "черный список".

    Attributes:
        source_user_id (int): пользователь который заблокировал
        target_user_id (int): пользователь которого заблокировали
    """
    def __init__(self, source_user_id: int, target_user_id: int):
        self.source_user_id = source_user_id
        self.target_user_id = target_user_id

    def __str__(self) -> str:
        return f"source_user_id: {self.source_user_id}, target_user_id: {self.target_user_id}"
    

class BlackListSerializer(Serializer):
    """
    Сериализатор для объектов BlackList
    """
    def __call__(self, obj: BlackList, ctx=None):
        source_user_id_bytes = obj.source_user_id.to_bytes(4, byteorder="big")
        target_user_id_bytes = obj.target_user_id.to_bytes(4, byteorder="big")

        result = source_user_id_bytes
        result += target_user_id_bytes

        return result


class BlackListDeserializer(Deserializer):
    """
    Десериализатор для объектов BlackList
    """
    def __call__(self, value: bytes, ctx=None):
        if value is None:
            return None

        source_user_id_bytes = value[0:4]
        source_user_id_value = int.from_bytes(source_user_id_bytes, byteorder="big")

        target_user_id_bytes = value[4:8]
        target_user_id_value = int.from_bytes(target_user_id_bytes, byteorder="big")

        return BlackList(source_user_id_value, target_user_id_value)