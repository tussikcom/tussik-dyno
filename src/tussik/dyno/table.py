import inspect
import logging

from .attributes import DynoEnum, DynoAttrBase, DynoAttribAutoIncrement, DynoAttrMap, DynoAttrList

logger = logging.getLogger()


class DynoMeta(type):
    def __repr__(cls):
        if hasattr(cls, '_class_repr'):
            return getattr(cls, '_class_repr')()
        else:
            return super(DynoMeta, cls).__repr__()

    def __str__(cls):
        if hasattr(cls, '_class_str'):
            return getattr(cls, '_class_str')()
        else:
            return super(DynoMeta, cls).__str__()


class DynoTableLink:
    __slots__ = ["table", "schema", "globalindex"]

    def __init__(self,
                 table: type["DynoTable"],
                 schema: type["DynoSchema"] | None = None,
                 globalindex: type['DynoGlobalIndex'] | None = None):
        self.table = table
        self.schema = schema
        self.globalindex = globalindex

    def __repr__(self):
        return f"DynoTableLink: table={self.table.TableName}, schema={self.schema or ''}, gsi={self.globalindex or ''}"


class DynoKey:
    __slots__ = ["_pk", "_sk", "_pk_type", "_sk_type"]

    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None
                 ):
        self._pk: str = pk or "pk"
        self._sk: str = sk or "sk"
        self._pk_type: DynoEnum = pk_type or DynoEnum.String
        self._sk_type: DynoEnum = sk_type or DynoEnum.String

    @property
    def pk(self) -> str:
        return self._pk

    @property
    def sk(self) -> str:
        return self._sk

    @property
    def pk_type(self) -> DynoEnum:
        return self._pk_type

    @property
    def sk_type(self) -> DynoEnum:
        return self._sk_type

    def __repr__(self):
        return f"DynoKey: {self._pk}, {self._sk}"


class DynoKeyFormat:
    __slots__ = ["pk", "sk", "req"]

    def __init__(self, pk: None | str = None, sk: None | str = None, req: None | set[str] = None):
        self.pk: str = pk
        self.sk: str = sk
        self.req: set[str] = req or set[str]()

    def __repr__(self):
        return f"DynoKeyFormat: pk={self.pk}, sk={self.sk}, req={' '.join(self.req)}"

    def write(self, key: DynoKey, values: None | dict[str, any]) -> None | dict[str, dict[str, any]]:
        pval = self.format_pk(values)
        if pval is None:
            return None
        sval = self.format_sk(values)
        if sval is None:
            return None
        results = {
            key.pk: {key.pk_type.value: pval},
            key.sk: {key.sk_type.value: sval},
        }
        return results

    def format_pk(self, values: None | dict[str, any] = None) -> None | str:
        if self.pk is not None:
            try:
                value = self.pk.format(**(values or dict()))
                if "None" in value:
                    for check in self.req:
                        if check not in values:
                            return None
                        if values[check] is None:
                            return None
                return value
            except Exception as e:
                pass
        return None

    def format_sk(self, values: None | dict[str, any] = None) -> None | str:
        if self.sk is not None:
            try:
                value = self.sk.format(**(values or dict()))
                if "None" in value:
                    for check in self.req:
                        if check not in values:
                            return None
                        if values[check] is None:
                            return None
                return value
            except Exception as e:
                pass
        return None


class DynoGlobalIndexFormat:
    __slots__ = ["name", "pk", "sk", "req"]

    def __init__(self, name: str, pk: None | str = None, sk: None | str = None, req: None | set[str] = None):
        self.name = name
        self.pk: str = pk
        self.sk: str = sk
        self.req: set[str] = req or set[str]()

    def __repr__(self):
        return f"DynoGlobalIndexFormat.{self.name}: pk={self.pk}, sk={self.sk}, req={' '.join(self.req)}"

    def write(self, key: DynoKey, values: None | dict[str, any]) -> None | dict[str, dict[str, any]]:
        pval = self.format_pk(values)
        if pval is None:
            return None
        sval = self.format_sk(values)
        if sval is None:
            return None
        results = {
            key.pk: {key.pk_type.value: pval},
            key.sk: {key.sk_type.value: sval},
        }
        return results

    def format_pk(self, values: None | dict[str, any] = None) -> None | str:
        if self.pk is not None:
            try:
                value = self.pk.format(**(values or dict()))
                if "None" in value:
                    for check in self.req:
                        if check not in values:
                            return None
                        if values[check] is None:
                            return None
                return value
            except Exception as e:
                pass
        return None

    def format_sk(self, values: None | dict[str, any] = None) -> None | str:
        if self.sk is not None:
            try:
                value = self.sk.format(**(values or dict()))
                if "None" in value:
                    for check in self.req:
                        if check not in values:
                            return None
                        if values[check] is None:
                            return None
                return value
            except Exception as e:
                pass
        return None


class DynoGlobalIndex:
    __slots__ = ["_name", "_pk", "_sk", "_pk_type", "_sk_type", "read_unit", "write_unit", "unique"]

    def __init__(self,
                 name: str,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None,
                 read_unit: None | int = None, write_unit: None | int = None,
                 unique: None | bool = None
                 ):
        self._name = name
        self._pk: str = pk or f"{self._name}_pk"
        self._sk: str = sk or f"{self._name}_sk"
        self._pk_type: DynoEnum = pk_type or DynoEnum.String
        self._sk_type: DynoEnum = sk_type or DynoEnum.String
        self.read_unit: int = read_unit or 1
        self.write_unit: int = write_unit or 1
        self.unique: bool = unique or True

    @property
    def name(self) -> str:
        return self._name

    @property
    def pk(self) -> str:
        return self._pk

    @property
    def sk(self) -> str:
        return self._sk

    @property
    def pk_type(self) -> DynoEnum:
        return self._pk_type

    @property
    def sk_type(self) -> DynoEnum:
        return self._sk_type

    def __repr__(self):
        return f"DynoGlobalIndex.{self._name}: {self._pk}, {self._sk}"


class DynoSchema(metaclass=DynoMeta):
    Key: DynoKeyFormat = ...
    Indexes: list[DynoGlobalIndexFormat] = list[DynoGlobalIndexFormat]()

    @classmethod
    def _class_repr(cls):
        return f"DynoSchema: {cls.__name__}"

    @classmethod
    def _class_str(cls):
        return cls.__name__

    @classmethod
    def get_schema_name(cls) -> str:
        return cls.__name__

    @classmethod
    def get_globalindexes(cls) -> dict[str, DynoGlobalIndexFormat]:
        results = dict[str, DynoGlobalIndexFormat]()
        for item in cls.Indexes:
            if isinstance(item, DynoGlobalIndexFormat):
                results[item.name] = item
        return results

    @classmethod
    def get_globalindex(cls, name: str) -> None | DynoGlobalIndexFormat:
        for item in cls.Indexes:
            if isinstance(item, DynoGlobalIndexFormat) and item.name == name:
                return item
        return None

    @classmethod
    def get_attributes(cls, nested: bool = False) -> dict[str, DynoAttrBase]:
        results = dict[str, DynoAttrBase]()
        for name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoAttrBase):
                results[name] = cls_attr
                if not nested:
                    continue

                if isinstance(cls_attr, DynoAttrMap):
                    child = cls_attr.get_attributes()
                    for cn, cb in child.items():
                        results[f"{name}.{cn}"] = cb

                if isinstance(cls_attr, DynoAttrList):
                    child = cls_attr.get_attributes()
                    for cn, cb in child.items():
                        results[f"{name}.{cn}"] = cb

        return results

    @classmethod
    def get_autoincrement(cls, name: str) -> None | DynoAttribAutoIncrement:
        for autoinc_name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoAttribAutoIncrement):
                if autoinc_name == name:
                    return cls_attr
        return None


class DynoAllow:
    __slots__ = ['datatype', 'attrib']

    def __init__(self, datatype: DynoEnum, attrib: None | DynoAttrBase = None):
        self.datatype = datatype
        self.attrib: None | DynoAttrBase = attrib

    def __repr__(self):
        value = f" -> {self.attrib!r}" if self.attrib else ""
        return f"DynoAllow({self.datatype.name}){value}"


class DynoTable(metaclass=DynoMeta):
    TableName: str = ...
    SchemaFieldName: None | str = "schema"
    Key: DynoKey = ...
    Indexes: list[DynoGlobalIndex] = list[DynoGlobalIndex]()
    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    @classmethod
    def isvalid(cls) -> bool:
        if not isinstance(cls.Key, DynoKey) or cls.Key.pk is None or cls.Key.sk is None:
            raise ValueError(f"Table key is invalid")

        key_list = list[str]()
        key_list.append(cls.Key.pk)
        if cls.Key.sk in key_list:
            raise ValueError(f"Table key sk must be unique")

        gsi_list = list[str]()
        for gsi in cls.Indexes:
            if not isinstance(gsi, DynoGlobalIndex):
                raise ValueError(f"Table global index {gsi.name} item is invalid")
            if gsi.name in gsi_list:
                raise ValueError(f"Table global index {gsi.name} name must be unique")

            if gsi.pk in key_list:
                raise ValueError(f"Table global index {gsi.name} pk must be unique")
            if gsi.sk in key_list:
                raise ValueError(f"Table global index {gsi.name} sk must be unique")

            gsi_list.append(gsi.name)

        if not isinstance(cls.TableName, str) or len(cls.TableName) == 0:
            raise ValueError(f"Table table name is required")

        schema_list = list[str]()
        for name, item in cls.get_schemas().items():
            if not issubclass(item, DynoSchema):
                raise ValueError(f"Table schema {name} is invalid")
            if name in schema_list:
                raise ValueError(f"Table schema {name} must be unique")

            if not isinstance(item.Key, DynoKeyFormat) or item.Key.pk is None or item.Key.sk is None:
                raise ValueError(f"Table schema {name} key is invalid")

            for gsi in item.Indexes:
                if not isinstance(gsi, DynoGlobalIndexFormat):
                    raise ValueError(f"Table schema {name} global index is invalid")
                if gsi.name not in gsi_list:
                    raise ValueError(f"Table schema {name} global index {gsi.name} is unknown")

            schema_list.append(name)

        return True

    @classmethod
    def _class_repr(cls):
        return f"DynoTable: {cls.TableName}"

    @classmethod
    def _class_str(cls):
        return cls.TableName

    @classmethod
    def get_link(cls, schema: type[DynoSchema] | None = None, globalindex: None | str = None) -> DynoTableLink:
        return DynoTableLink(cls, schema, globalindex)

    @classmethod
    def auto_increment(cls,
                       data: dict[str, any],
                       schema: type[DynoSchema],
                       name: str,
                       reset: None | bool = None) -> None | dict:
        reset = reset if isinstance(reset, bool) else False

        #
        # get the schema
        #
        check = cls.get_schema(schema.get_schema_name())
        if check is None:
            raise Exception(f"Schema {schema} must be part of the table class")

        #
        # locate the auto-increment field
        #
        autoinc = schema.get_autoincrement(name)
        if autoinc is None:
            raise Exception(f"Unknown auto-increment: {schema}.{name}")

        #
        # prepare the key
        #
        pk = schema.Key.format_pk(data)
        sk = schema.Key.format_pk(data)
        if pk is None or sk is None:
            raise Exception(f"Invalid key for {schema}.{name}")

        names = {
            "#n1": name,
            "#n2": cls.Key.pk,
            "#n3": cls.Key.sk
        }
        values = {
            ":v1": {"N": str(autoinc.start)},
            ":v2": {"N": str(autoinc.step)},
            ":v3": {cls.Key.pk_type.value: pk},
            ":v4": {cls.Key.sk_type.value: sk},
        }

        params = {
            "TableName": cls.TableName,
            "Key": {
                cls.Key.pk: {cls.Key.pk_type.value: pk},
                cls.Key.sk: {cls.Key.sk_type.value: sk}
            },
            "UpdateExpression": "SET #n1 = :v1 + :v2" if reset else "SET #n1 = if_not_exists(#n1, :v1) + :v2",
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
            "ReturnValues": "UPDATED_NEW",
            "ReturnConsumedCapacity": "TOTAL",
            "ConditionExpression": f"#n2 = :v3 AND #n3 = :v4",
        }
        return params

    @classmethod
    def write_table_create(cls) -> dict:
        params = dict[str, any]()

        params["TableName"] = cls.TableName
        params["TableClass"] = "STANDARD" if cls.TableClassStandard else "STANDARD_INFREQUENT_ACCESS"
        params["DeletionProtectionEnabled"] = cls.DeletionProtection
        params["BillingMode"] = "PAY_PER_REQUEST" if cls.PayPerRequest else "PROVISIONED"

        #
        # Attributes
        #
        results = list[dict[str, any]]()
        results.append({"AttributeName": cls.Key.pk, "AttributeType": cls.Key.pk_type.value})
        results.append({"AttributeName": cls.Key.sk, "AttributeType": cls.Key.sk_type.value})
        for name, gsi in cls.get_globalindexes().items():
            results.append({"AttributeName": gsi.pk, "AttributeType": gsi.pk_type.value})
            results.append({"AttributeName": gsi.sk, "AttributeType": gsi.sk_type.value})
        params["AttributeDefinitions"] = results

        #
        # Key
        #
        params["KeySchema"] = [
            {"AttributeName": cls.Key.pk, "KeyType": "HASH"},
            {"AttributeName": cls.Key.sk, "KeyType": "RANGE"}
        ]

        #
        # Provisioning
        #
        params['ProvisionedThroughput'] = {
            "ReadCapacityUnits": cls.ReadCapacityUnits,
            "WriteCapacityUnits": cls.WriteCapacityUnits
        }

        #
        # Global Indexes
        #
        params['GlobalSecondaryIndexes'] = list()
        for name, gsi in cls.get_globalindexes().items():
            params['GlobalSecondaryIndexes'].append({
                "IndexName": name,
                "KeySchema": [
                    {"AttributeName": gsi.pk, "KeyType": "HASH"},
                    {"AttributeName": gsi.sk, "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"},  # TODO: add to GSI class
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": gsi.read_unit,
                    "WriteCapacityUnits": gsi.write_unit,
                }
            })

        return params

    @classmethod
    def get_globalindexes(cls) -> dict[str, DynoGlobalIndex]:
        indexes = dict[str, DynoGlobalIndex]()
        for item in cls.Indexes:
            if isinstance(item, DynoGlobalIndex):
                indexes[item.name] = item
        return indexes

    @classmethod
    def get_globalindex(cls, name: str) -> None | DynoGlobalIndex:
        for item in cls.Indexes:
            if isinstance(item, DynoGlobalIndex) and item.name == name:
                return item
        return None

    @classmethod
    def get_schemas(cls) -> dict[str, type[DynoSchema]]:
        results = dict[str, type[DynoSchema]]()
        for name, cls_attr in cls.__dict__.items():
            if inspect.isclass(cls_attr) and issubclass(cls_attr, DynoSchema):
                results[name] = cls_attr
        return results

    @classmethod
    def get_schema(cls, name: str) -> type[DynoSchema] | None:
        for n, cls_attr in cls.__dict__.items():
            if inspect.isclass(cls_attr) and issubclass(cls_attr, DynoSchema) and n == name:
                return cls_attr
        return None

    @classmethod
    def allow_list(cls, schema: type[DynoSchema], globalindex: None | str = None) -> dict[str, DynoAllow]:
        result = dict[str, DynoAllow]()

        # auto include the key
        result[cls.Key.pk] = DynoAllow(cls.Key.pk_type)
        result[cls.Key.sk] = DynoAllow(cls.Key.sk_type)

        # optionally include global index
        for name, gsi in cls.get_globalindexes().items():
            if isinstance(globalindex, str) and name != globalindex:
                continue
            result[gsi.pk] = DynoAllow(gsi.pk_type)
            result[gsi.sk] = DynoAllow(gsi.sk_type)

        # include attributes for either all schemas or the provided schema
        for name, value in cls.get_schemas().items():
            n1 = value.get_schema_name()
            n2 = schema.get_schema_name()
            if issubclass(schema, DynoSchema) and n1 != n2:
                continue

            attributes = value.get_attributes(nested=False)
            for attr_name, base in attributes.items():
                result[attr_name] = DynoAllow(DynoEnum(base.code), base)

        return result

    @classmethod
    def write_value(cls,
                    data: dict[str, any],
                    schema: type[DynoSchema],
                    globalindex: None | str = None,
                    include_readonly: None | bool = None
                    ) -> dict[str, any]:
        allow = cls.allow_list(schema, globalindex)
        result = dict[str, any]()
        include_readonly = include_readonly if isinstance(include_readonly, bool) else False

        #
        # tagged record with schema field value
        #
        if isinstance(cls.SchemaFieldName, str):
            result[cls.SchemaFieldName] = schema.get_schema_name()

        #
        # write attributes
        #
        for name, item in allow.items():
            if isinstance(item.attrib, DynoAttrBase):
                if item.attrib.readonly and not include_readonly:
                    # respect readonly when asked to
                    continue
                if not item.attrib.always and name not in data:
                    # if the entry is unknown and the member is optional, skip it
                    continue

                try:
                    value = data.get(name)
                    ret = item.attrib.write_value(value)
                    result[name] = ret
                except Exception as e:
                    logger.exception(f"DynoTable.write_value(include_readonly={include_readonly}): {e!r}")

        if schema is None:
            return result

        #
        # primary key
        #
        result[cls.Key.pk] = schema.Key.format_pk(result)
        result[cls.Key.sk] = schema.Key.format_sk(result)

        #
        # global indexes
        #
        for name, gsi in cls.get_globalindexes().items():
            if isinstance(globalindex, str) and name != globalindex:
                continue
            fmt = schema.get_globalindex(name)
            pk = None
            sk = None

            if fmt is not None:
                pk = fmt.format_pk(result)
                sk = fmt.format_sk(result)

            if pk is not None and sk is not None:
                result[gsi.pk] = pk
                result[gsi.sk] = sk

        return result
