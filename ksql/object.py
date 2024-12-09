from ksql.type import KsqlObjectType as Type


class KsqlObject:

    def __init__(self, ksql_type: str, object_name: str = None, topic: str = None, query_id: str = None):
        self._ksql_type = ksql_type
        if object_name:
            if object_name.startswith('`') and object_name.endswith('`'):
                self._name = object_name
            else:
                self._name = object_name.upper()
        else:
            self._name = None

        if topic:
            self.topic = topic
        elif ksql_type in [Type.CREATE_TABLE, Type.CREATE_STREAM]:
            self.topic = object_name
        else:
            self.topic = None
        self._query_id = query_id

    def __str__(self):
        if Type.is_insert(self._ksql_type):
            return f'INSERT INTO {self._name}'
        elif Type.is_cas(self._ksql_type):
            obj = 'TABLE' if self._ksql_type == Type.CTAS else 'STREAM'
            return f'CREATE {obj} {self._name} AS SELECT'
        elif Type.is_create(self._ksql_type):
            return f'CREATE {self._ksql_type} {self._name}'
        else:
            return self._ksql_type

    @property
    def type(self):
        return self._ksql_type

    @property
    def name(self):
        return self._name

    @property
    def query_id(self):
        return self._query_id

    def set_topic(self, topic: str) -> None:
        self.topic = topic

    def as_dict(self) -> dict:
        return {
            'type': self._ksql_type,
            'name': self._name,
            'topic': self.topic,
            'query_id': self._query_id
        }
