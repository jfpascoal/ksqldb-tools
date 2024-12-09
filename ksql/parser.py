import re

from ksql.object import KsqlObject
from ksql.type import KsqlObjectType as Type

COMMENT = re.compile(r"--.+")
MULTI_SPACE = re.compile(r"\s+")
CTAS_PAT = re.compile(r"CREATE\s+(OR REPLACE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)\s+"
                      + r"(WITH\s?\(.+?\)\s?)?AS\s+SELECT.+",
                      re.IGNORECASE | re.DOTALL)
CSAS_PAT = re.compile(r"CREATE\s+(OR REPLACE\s+)?STREAM\s+(IF\s+NOT\s+EXISTS\s+)?(\S+)\s+"
                      + r"(WITH\s?\(.+?\)\s?)?AS\s+SELECT.+",
                      re.IGNORECASE | re.DOTALL)
CREATE_STREAM_PAT = re.compile(r"CREATE\s+(OR REPLACE\s+)?(SOURCE\s+)?STREAM\s+(IF\s+NOT\s+EXISTS\s+)?"
                               + r"([^\s(]+).+?(WITH\s?\(.+?\)).?", re.IGNORECASE | re.DOTALL)
CREATE_TABLE_PAT = re.compile(r"CREATE\s+(OR REPLACE\s+)?(SOURCE\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?"
                              + r"([^\s(]+).+?(WITH\s?\(.+?\)).?", re.IGNORECASE | re.DOTALL)
INSERT_PAT = re.compile(r"INSERT\s+INTO\s+(\S+)\s+(WITH\s?\(.+?\)\s?)?SELECT.+", re.IGNORECASE | re.DOTALL)
SET_PAT = re.compile(r"SET.+", re.IGNORECASE | re.DOTALL)
TOPIC_PAT = re.compile(r"KAFKA_TOPIC\s?=\s?'(\S+?)'.?", re.IGNORECASE | re.DOTALL)
QUERY_ID_PAT = re.compile(r"QUERY_ID\s?=\s?'(\S+?)'.?", re.IGNORECASE | re.DOTALL)


def read_kslq_script(script_path: str) -> list[str]:
    statements = []
    with open(script_path) as f:
        script = f.read()
        elements = script.split(';')

        for e in elements:
            e = re.sub(COMMENT, '', e)
            e = re.sub(MULTI_SPACE, ' ', e)
            if e.strip():
                statements.append(e.strip())
        return statements


def parse_statement(statement: str) -> KsqlObject:
    for pat, func in [
        (CTAS_PAT, parse_ctas),
        (CSAS_PAT, parse_csas),
        (CREATE_TABLE_PAT, parse_create_table),
        (CREATE_STREAM_PAT, parse_create_stream),
        (INSERT_PAT, parse_insert)
    ]:
        match = re.fullmatch(pat, statement)
        if match:
            return func(match)
    return KsqlObject(ksql_type=Type.SET)


def parse_ctas(match: re.Match) -> KsqlObject:
    name = match.group(3)
    topic = re.search(TOPIC_PAT, match.group(4)).group(1) if match.group(3) else None
    return KsqlObject(ksql_type=Type.CTAS, object_name=name, topic=topic)


def parse_csas(match: re.Match) -> KsqlObject:
    name = match.group(3)
    topic = re.search(TOPIC_PAT, match.group(4)).group(1) if match.group(3) else None
    return KsqlObject(ksql_type=Type.CSAS, object_name=name, topic=topic)


def parse_create_stream(match: re.Match) -> KsqlObject:
    name = match.group(4)
    topic = re.search(TOPIC_PAT, match.group(5)).group(1)
    return KsqlObject(ksql_type=Type.CREATE_STREAM, object_name=name, topic=topic)


def parse_create_table(match: re.Match) -> KsqlObject:
    name = match.group(4)
    topic = re.search(TOPIC_PAT, match.group(5)).group(1)
    return KsqlObject(ksql_type=Type.CREATE_TABLE, object_name=name, topic=topic)


def parse_insert(match: re.Match) -> KsqlObject:
    name = match.group(1)
    query_id = re.search(QUERY_ID_PAT, match.group(2)).group(1) if match.group(2) else None
    return KsqlObject(ksql_type=Type.INSERT, object_name=name, query_id=query_id)
