class KsqlObjectType:
    CSAS = 'CSAS'
    CTAS = 'CTAS'
    CREATE_STREAM = 'STREAM'
    CREATE_TABLE = 'TABLE'
    INSERT = 'INSERT'
    SET = 'SET'

    TYPE_MAP = {
        CSAS: CREATE_STREAM,
        CTAS: CREATE_TABLE
    }

    @classmethod
    def get_types(cls) -> dict:
        return dict(
            [(k, v) for k, v in cls.__dict__.items()
             if not k.startswith('__') and isinstance(v, str)]
        )

    @classmethod
    def type_of(cls, typ: str) -> str:
        types = cls.get_types()
        return object.__getattribute__(cls, types[typ]) if typ in types.values() else None

    @classmethod
    def is_cas(cls, typ: str) -> bool:
        return typ in [cls.CSAS, cls.CTAS]

    @classmethod
    def is_create(cls, typ: str) -> bool:
        return cls.is_cas(typ) or typ in [cls.CREATE_TABLE, cls.CREATE_STREAM]

    @classmethod
    def is_insert(cls, typ: str) -> bool:
        return typ == cls.INSERT

    @classmethod
    def is_materialization(cls, typ: str) -> bool:
        return cls.is_cas(typ) or cls.is_insert(typ)

    @classmethod
    def cas_type(cls, typ: str) -> str:
        return cls.TYPE_MAP.get(typ)
