import inspect
import logging
from typing import Set, Any, Dict, Type, Optional

from .attributes import DynoEnum, DynoAttrBase, DynoAttribAutoIncrement

logger = logging.getLogger()


class DynoTableLink:
    def __init__(self, table: Type["DynoTable"], schema: None | str = None, globalindex: None | str = None):
        self.table = table
        self.schema = schema
        self.globalindex = globalindex


class DynoKey:

    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None
                 ):
        self.pk = pk or "pk"
        self.sk = sk or "sk"
        self.pk_type = pk_type or DynoEnum.String
        self.sk_type = sk_type or DynoEnum.String

    def read(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        result = dict[str, Any]()
        value = None
        if self.pk in data:
            rec = data[self.pk]
            if isinstance(rec, dict):
                value = rec.get(self.pk_type.value)
        result[self.pk] = value

        value = None
        if self.sk in data:
            rec = data[self.sk]
            if isinstance(rec, dict):
                value = rec.get(self.sk_type.value)
        result[self.sk] = value
        return result


class DynoKeyFormat:
    __slots__ = ["pk", "sk", "req"]

    def __init__(self, pk: None | str = None, sk: None | str = None, req: None | Set[str] = None):
        self.pk = pk
        self.sk = sk
        self.req = req or set[str]()

    def format_pk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.pk is not None:
            for item in self.req:
                if item not in values:
                    return None
            return self.pk.format(**(values or dict()))
        return None

    def format_sk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.sk is not None:
            for item in self.req:
                if item not in values:
                    return None
            return self.sk.format(**(values or dict()))
        return None


class DynoGlobalIndex:
    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoEnum = None, sk_type: None | DynoEnum = None,
                 read_unit: None | int = None, write_unit: None | int = None
                 ):
        self.pk = pk or "pk"
        self.sk = sk or "sk"
        self.pk_type = pk_type or DynoEnum.String
        self.sk_type = sk_type or DynoEnum.String
        self.read_unit = read_unit or 1
        self.write_unit = write_unit or 1

    def read(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        result = dict[str, Any]()
        value = None
        if self.pk in data:
            rec = data[self.pk]
            if isinstance(rec, dict):
                value = rec.get(self.pk_type.value)
        result[self.pk] = value

        value = None
        if self.sk in data:
            rec = data[self.sk]
            if isinstance(rec, dict):
                value = rec.get(self.sk_type.value)
        result[self.sk] = value
        return result


class DynoSchema:
    Key: DynoKeyFormat = ...
    SchemaFieldValue: None | str = None
    GlobalIndexes = dict[str, DynoKeyFormat]()
    SchemaTypeFieldName: None | str = None

    def __repr__(self):
        return f"DynoSchema"

    @classmethod
    def get_attributes(cls) -> Dict[str, DynoAttrBase]:
        results = dict[str, DynoAttrBase]()
        for name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoAttrBase):
                results[name] = cls_attr
        return results

    @classmethod
    def get_autoincrement(cls, name: str) -> None | DynoAttribAutoIncrement:
        for autoinc_name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoAttribAutoIncrement) and autoinc_name == name:
                return cls_attr
        return None

    @classmethod
    def write(cls, data: Dict[str, Any], include_readonly: None | bool = None) -> Dict[str, dict]:
        include_readonly = include_readonly if isinstance(include_readonly, bool) else False
        result = dict[str, dict[str, Any]]()

        for name, item in cls.get_attributes().items():
            if item.readonly and not include_readonly:
                # respect readonly when asked to
                continue
            if not item.always and name not in data:
                # if the entry is unknown and the member is optional, skip it
                continue
            value = data.get(name)
            try:
                ret = item.write(value)
                result[name] = ret
            except Exception as e:
                logger.exception(f"DynoSchema.write(): {e!r}")

        return result

    @classmethod
    def read(cls, data: dict) -> dict:
        result = dict()

        # for name, item in cls.get_attributes().items():
        #     value = data.get(name)

        return result


class DynoTable:
    TableName: str = ...
    SchemaFieldName: None | str = None
    Key: DynoKey = ...
    GlobalIndexes = dict[str, DynoGlobalIndex]()
    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    def __repr__(self):
        return f"DynoTable( {self.TableName} )"

    @classmethod
    def get_link(cls, schema: None | str = None, globalindex: None | str = None) -> DynoTableLink:
        return DynoTableLink(cls, schema, globalindex)

    @classmethod
    def auto_increment(cls, data: Dict[str, Any], schema: str, name: str, reset: None | bool = None) -> None | dict:
        reset = reset if isinstance(reset, bool) else False

        #
        # get the schema
        #
        tbl = cls.get_schema(schema)
        if tbl is None:
            raise Exception(f"Unknown schema: {schema}")

        #
        # locate the auto-increment field
        #
        autoinc = tbl.get_autoincrement(name)
        if autoinc is None:
            raise Exception(f"Unknown auto-increment: {schema}.{name}")

        #
        # prepare the key
        #
        pk = tbl.Key.format_pk(data)
        sk = tbl.Key.format_pk(data)
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
    def table_create_keys(cls) -> list:
        results = list[dict[str, Any]]()
        results.append({"AttributeName": cls.Key.pk, "KeyType": "HASH"})
        results.append({"AttributeName": cls.Key.sk, "KeyType": "RANGE"})
        return results

    @classmethod
    def table_create_attributes(cls) -> list:
        results = list[dict[str, Any]]()

        results.append({"AttributeName": cls.Key.pk, "AttributeType": cls.Key.pk_type.value})
        results.append({"AttributeName": cls.Key.sk, "AttributeType": cls.Key.sk_type.value})

        for name, gsi in cls.GlobalIndexes.items():
            results.append({"AttributeName": gsi.pk, "AttributeType": gsi.pk_type.value})
            results.append({"AttributeName": gsi.sk, "AttributeType": gsi.sk_type.value})

        return results

    @classmethod
    def table_create_globalindexes(cls) -> list:
        results = list[dict[str, Any]]()
        for name, gsi in cls.GlobalIndexes.items():
            results.append({
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
        return results

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
    def read(cls, data: Dict[str, Dict[str, Any]],
             schema: None | str = None, globalindex: None | str = None,
             nested: None | bool = None) -> Dict[str, Any]:
        nested = nested if isinstance(nested, bool) else True

        #
        # schema specific read
        #
        tbl = cls.get_schema(schema) if isinstance(schema, str) else None
        if tbl is not None:
            result = dict[str, Any]()

            # include key values where available
            if isinstance(globalindex, str):
                gsi = cls.GlobalIndexes.get(globalindex)
                if gsi is not None:
                    result |= gsi.read(data)
            result |= cls.Key.read(data)

            # include schema field where available
            if isinstance(cls.SchemaFieldName, str) and len(cls.SchemaFieldName) > 0:
                result[cls.SchemaFieldName] = data.get(cls.SchemaFieldName, dict()).get(DynoEnum.String.value)

            # walk attributes
            attributes = tbl.get_attributes()
            for k, v in data.items():
                if k in result:
                    continue
                try:
                    if k in attributes:
                        base = attributes[k]
                        datatype = next(iter(v))
                        value = v.get(datatype)
                        result[k] = base.read(datatype, value)
                except Exception as e:
                    logger.exception(f"DynoTable.read() of {k} : {e!r}")

            # TODO: look for elements not covered
            return result

        #
        # generic read
        #
        result = dict[str, Any]()
        for k1, v1 in data.items():
            if isinstance(v1, dict):
                dt = next(iter(v1))
                val = v1[dt]

                if dt == DynoEnum.Null:
                    continue
                elif dt == DynoEnum.Map:
                    if not nested:
                        continue
                    result[k1] = dict[str, [dict[str, Any]]]()
                    for k2, v2 in val.items():
                        rec = cls.read(v2)
                        result[k1][k2] = rec

                elif dt == DynoEnum.List:
                    if not nested:
                        continue
                    result[k1] = list[dict[str, Any]]()
                    for item in val:
                        rec = cls.read(item)
                        result[k1].append(rec)

                elif dt == DynoEnum.Number:
                    v = float(val)
                    if v.is_integer():
                        v = int(v)
                    result[k1] = v

                else:
                    result[k1] = val

        return result

    @classmethod
    def write_key(cls, data: Dict[str, Any], schema: str,
                  include_readonly: None | bool = None) -> None | Dict[str, dict]:
        tbl = cls.get_schema(schema)
        if tbl is None:
            raise Exception("not found")
        result = tbl.write(data, include_readonly)

        # schema field in action
        if isinstance(cls.SchemaFieldName, str) and len(cls.SchemaFieldName) > 0:
            if isinstance(tbl.SchemaFieldValue, str) and len(tbl.SchemaFieldValue) > 0:
                result[cls.SchemaFieldName] = {"S": tbl.SchemaFieldValue}

        cleaned = cls.read(result, False)
        pk = tbl.Key.format_pk(cleaned)
        sk = tbl.Key.format_sk(cleaned)
        if pk is None and sk is None:
            return None

        result[cls.Key.pk] = {cls.Key.pk_type.value: pk}
        result[cls.Key.sk] = {cls.Key.sk_type.value: sk}

        for name, gsi in cls.GlobalIndexes.items():
            fmt = tbl.GlobalIndexes.get(name)
            if fmt is None:
                continue
            pk = fmt.format_pk(cleaned)
            sk = fmt.format_sk(cleaned)
            if pk is None and sk is None:
                continue
            result[gsi.pk] = {gsi.pk_type.value: pk}
            result[gsi.sk] = {gsi.sk_type.value: sk}

        return result

    @classmethod
    def write_globalindex(cls, data: Dict[str, Any], schema: str, globalindex: str,
                          include_readonly: None | bool = None) -> None | Dict[str, dict]:
        tbl = cls.get_schema(schema)
        if tbl is None:
            raise Exception("schema not found")
        gsi = cls.GlobalIndexes.get(globalindex)
        if gsi is None:
            raise Exception("table global index not found")
        fmt = tbl.GlobalIndexes.get(globalindex)
        if fmt is None:
            raise Exception("schema global index not found")
        result = tbl.write(data, include_readonly)

        # schema field in action
        if isinstance(cls.SchemaFieldName, str) and len(cls.SchemaFieldName) > 0:
            if isinstance(tbl.SchemaFieldValue, str) and len(tbl.SchemaFieldValue) > 0:
                result[cls.SchemaFieldName] = {"S": tbl.SchemaFieldValue}

        cleaned = cls.read(result, False)
        pk = fmt.format_pk(cleaned)
        sk = fmt.format_sk(cleaned)
        if pk is None and sk is None:
            return None

        gsi = cls.GlobalIndexes.get(globalindex)
        result[gsi.pk] = {gsi.pk_type.value: pk}
        result[gsi.sk] = {gsi.sk_type.value: sk}
        return result
