from ksql.object import KsqlObject
from ksql.parser import read_kslq_script, parse_statement
from ksql.type import KsqlObjectType as Type


class KsqlScript:

    def __init__(self, path: str):
        self.statements = read_kslq_script(path)
        self.elements = []
        for i in self.statements:
            self._add_element(i)

    def _add_element(self, statement: str) -> None:
        element = parse_statement(statement)
        if Type.is_insert(element.type):
            topic = self.get_element(element.name).topic
            element.set_topic(topic)
        self.elements.append(element)

    def get_element(self, name: str) -> KsqlObject | None:
        for e in self.elements:
            if e.name == name:
                return e

    def as_dict_list(self) -> list[dict]:
        return [i.as_dict() for i in self.elements]

    def get_all_topics(self, sort: bool = False, unique: bool = False) -> list[str]:
        all_topics = [i.topic for i in self.elements if i.topic]
        if unique:
            all_topics = list(set(all_topics))
        return sorted(all_topics) if sort else all_topics

    def get_all_queries(self) -> list[str]:
        return [i.query_id for i in self.elements if i.query_id]

    def get_all_names(self, sort: bool = False) -> list[str]:
        all_names = set([i.name for i in self.elements if i.name])
        return sorted(list(all_names)) if sort else list(all_names)

    def count_persistent_queries(self) -> int:
        return len([e for e in self.elements if Type.is_materialization(e.type)])

    def get_drop_statements(self, delete_topics: bool) -> list[str]:
        drop_list = []

        for element in self.elements:
            if Type.is_insert(element.type):
                s = f"TERMINATE {element.query_id};"
            elif not Type.is_create(element.type):
                continue
            else:
                s = f"DROP {Type.cas_type(element.type) or element.type} IF EXISTS {element.name};"
                if Type.is_cas(element.type) and delete_topics:
                    s = s[:-1] + " DELETE TOPIC;"
            drop_list.append(s)

        return drop_list
