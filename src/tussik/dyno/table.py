import inspect
import logging
from typing import Set, Any, Dict, Type, Optional

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
                 schema: type["DynoSchema"] = None,
                 globalindex: None | type['DynoGlobalIndex'] = None):
        self.table = table
        self.schema = schema
        self.globalindex = globalindex

    def __repr__(self):
        return f"DynoTableLink: table={self.table.TableName}, schema={self.schema or ''}, gsi={self.globalindex or ''}"


class DynoKey:
    __slots__ = ["pk", "sk", "pk_type", "sk_type"]

    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None
                 ):
        self.pk: str = pk or "pk"
        self.sk: str = sk or "sk"
        self.pk_type: DynoEnum = pk_type or DynoEnum.String
        self.sk_type: DynoEnum = sk_type or DynoEnum.String

    def __repr__(self):
        return f"DynoKey: {self.pk}, {self.sk}"


class DynoKeyFormat:
    __slots__ = ["pk", "sk", "req"]

    def __init__(self, pk: None | str = None, sk: None | str = None, req: None | Set[str] = None):
        self.pk: str = pk
        self.sk: str = sk
        self.req: Set[str] = req or set[str]()

    def __repr__(self):
        return f"DynoKeyFormat: pk={self.pk}, sk={self.sk}, req={' '.join(self.req)}"

    def write(self, key: DynoKey, values: None | Dict[str, Any]) -> None | Dict[str, Dict[str, Any]]:
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

    def format_pk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.pk is not None:
            try:
                return self.pk.format(**(values or dict()))
            except Exception as e:
                pass
        return None

    def format_sk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.sk is not None:
            try:
                return self.sk.format(**(values or dict()))
            except Exception as e:
                pass
        return None


class DynoGlobalIndex:
    __slots__ = ["pk", "sk", "pk_type", "sk_type", "read_unit", "write_unit", "unique"]

    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None,
                 read_unit: None | int = None, write_unit: None | int = None,
                 unique: None | bool = None
                 ):
        self.pk: str = pk or "pk"
        self.sk: str = sk or "sk"
        self.pk_type: DynoEnum = pk_type or DynoEnum.String
        self.sk_type: DynoEnum = sk_type or DynoEnum.String
        self.read_unit: int = read_unit or 1
        self.write_unit: int = write_unit or 1
        self.unique: bool = unique or True


class DynoSchema(metaclass=DynoMeta):
    Key: DynoKeyFormat = ...

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
    def get_globalindexes(cls) -> Dict[str, DynoKeyFormat]:
        results = dict[str, DynoKeyFormat]()
        for name, cls_attr in cls.__dict__.items():
            if name != "Key" and isinstance(cls_attr, DynoKeyFormat):
                results[name] = cls_attr
        return results

    @classmethod
    def get_globalindex(cls, name: str) -> None | DynoKeyFormat:
        if name == "Key":
            return None
        for key, cls_attr in cls.__dict__.items():
            if key == name and isinstance(cls_attr, DynoKeyFormat):
                return cls_attr
        return None

    @classmethod
    def get_attributes(cls, nested: bool = False) -> Dict[str, DynoAttrBase]:
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
            if isinstance(cls_attr, DynoAttribAutoIncrement) and autoinc_name == name:
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
    SchemaFieldName: None | str = None
    Key: DynoKey = ...
    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    @classmethod
    def _class_repr(cls):
        return f"DynoTable: {cls.TableName}"

    @classmethod
    def _class_str(cls):
        return cls.TableName

    @classmethod
    def get_link(cls, schema: type[DynoSchema] = None, globalindex: None | str = None) -> DynoTableLink:
        return DynoTableLink(cls, schema, globalindex)

    @classmethod
    def auto_increment(cls,
                       data: dict[str, Any],
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
        params = dict[str, Any]()

        params["TableName"] = cls.TableName
        params["TableClass"] = "STANDARD" if cls.TableClassStandard else "STANDARD_INFREQUENT_ACCESS"
        params["DeletionProtectionEnabled"] = cls.DeletionProtection
        params["BillingMode"] = "PAY_PER_REQUEST" if cls.PayPerRequest else "PROVISIONED"

        #
        # Attributes
        #
        results = list[dict[str, Any]]()
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
    def get_globalindexes(cls) -> Dict[str, DynoGlobalIndex]:
        indexes = dict[str, DynoGlobalIndex]()
        for name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoGlobalIndex):
                indexes[name] = cls_attr
        return indexes

    @classmethod
    def get_globalindex(cls, name: str) -> None | DynoGlobalIndex:
        for key, cls_attr in cls.__dict__.items():
            if name == key and isinstance(cls_attr, DynoGlobalIndex):
                return cls_attr
        return None

    @classmethod
    def get_schemas(cls) -> Dict[str, Type[DynoSchema]]:
        results = dict[str, Type[DynoSchema]]()
        for name, cls_attr in cls.__dict__.items():
            if inspect.isclass(cls_attr) and issubclass(cls_attr, DynoSchema):
                results[name] = cls_attr
        return results

    @classmethod
    def get_schema(cls, name: str) -> Optional[Type[DynoSchema]]:
        for n, cls_attr in cls.__dict__.items():
            if inspect.isclass(cls_attr) and issubclass(cls_attr, DynoSchema) and n == name:
                return cls_attr
        return None

    @classmethod
    def allow_list(cls, schema: type[DynoSchema], globalindex: None | str = None) -> Dict[str, DynoAllow]:
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
                    data: dict[str, Any],
                    schema: type[DynoSchema],
                    globalindex: None | str = None,
                    include_readonly: None | bool = None
                    ) -> dict[str, Any]:
        allow = cls.allow_list(schema, globalindex)
        result = dict[str, Any]()
        include_readonly = include_readonly if isinstance(include_readonly, bool) else False

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
            if fmt is not None:
                result[gsi.pk] = fmt.format_pk(result)
                result[gsi.sk] = fmt.format_sk(result)

        return result
