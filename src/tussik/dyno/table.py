import inspect
import logging
from typing import Set, Any, Dict, Type, Optional

from .fields import DynoTypeEnum, DynoTypeBase

logger = logging.getLogger()


class DynoKey:

    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoTypeEnum = None, sk_type: None | DynoTypeEnum = None
                 ):
        self.pk = pk or "pk"
        self.sk = sk or "sk"
        self.pk_type = pk_type or DynoTypeEnum.String
        self.sk_type = sk_type or DynoTypeEnum.String


class DynoKeyFormat:
    __slots__ = ["pk", "sk", "req"]

    def __init__(self, pk: None | str = None, sk: None | str = None, req: None | Set[str] = None):
        self.pk = pk
        self.sk = sk
        self.req = req or set[str]()

    def format_pk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.pk is not None and len(values) > 0:
            for item in self.req:
                if item not in values:
                    return None
            return self.pk.format(**(values or dict()))
        return None

    def format_sk(self, values: None | Dict[str, Any] = None) -> None | str:
        if self.sk is not None and len(values) > 0:
            for item in self.req:
                if item not in values:
                    return None
            return self.sk.format(**(values or dict()))
        return None


class DynoGlobalIndex:
    def __init__(self,
                 pk: None | str = None, sk: None | str = None,
                 pk_type: None | DynoTypeEnum = None, sk_type: None | DynoTypeEnum = None,
                 read_unit: None | int = None, write_unit: None | int = None
                 ):
        self.pk = pk or "pk"
        self.sk = sk or "sk"
        self.pk_type = pk_type or DynoTypeEnum.String
        self.sk_type = sk_type or DynoTypeEnum.String
        self.read_unit = read_unit or 1
        self.write_unit = write_unit or 1


class DynoSchema:
    Key: DynoKeyFormat = ...
    GlobalIndexes = dict[str, DynoKeyFormat]()

    def __repr__(self):
        return f"DynoSchema"

    @classmethod
    def get_attributes(cls) -> Dict[str, DynoTypeBase]:
        results = dict[str, DynoTypeBase]()
        for name, cls_attr in cls.__dict__.items():
            if isinstance(cls_attr, DynoTypeBase):
                results[name] = cls_attr
        return results

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


class DynoTypeTable:
    TableName: str = ...
    Key: DynoKey = ...
    GlobalIndexes = dict[str, DynoGlobalIndex]()
    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    def __repr__(self):
        return f"DynoTypeTable( {self.TableName} )"

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
    def dyno2data(cls, data: Dict[str, Dict[str, Any]], nested: None | bool = None) -> Dict[str, Any]:
        nested = nested if isinstance(nested, bool) else True
        result = dict[str, Any]()

        #
        # TODO: change to a generic "write" using metadata
        #

        for k1, v1 in data.items():
            if isinstance(v1, dict):
                dt = next(iter(v1))
                val = v1[dt]

                if dt == DynoTypeEnum.Null:
                    continue
                elif dt == DynoTypeEnum.Map:
                    if not nested:
                        continue
                    result[k1] = dict[str, [dict[str, Any]]]()
                    for k2, v2 in val.items():
                        rec = cls.dyno2data(v2)
                        result[k1][k2] = rec

                elif dt == DynoTypeEnum.List:
                    if not nested:
                        continue
                    result[k1] = list[dict[str, Any]]()
                    for item in val:
                        rec = cls.dyno2data(item)
                        result[k1].append(rec)

                elif dt == DynoTypeEnum.Number:
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

        cleaned = cls.dyno2data(result, False)
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

        cleaned = cls.dyno2data(result, False)
        pk = fmt.format_pk(cleaned)
        sk = fmt.format_sk(cleaned)
        if pk is None and sk is None:
            return None

        gsi = cls.GlobalIndexes.get(globalindex)
        result[gsi.pk] = {gsi.pk_type.value: pk}
        result[gsi.sk] = {gsi.sk_type.value: sk}
        return result
