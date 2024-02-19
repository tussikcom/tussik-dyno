import logging
from enum import Enum, StrEnum
from typing import Self

from .filtering import DynoFilter, DynoFilterKey, DynoAttributeState
from .table import DynoTable, DynoTableLink, DynoSchema

logger = logging.getLogger()


class DynoQuerySelectEnum(Enum):
    all = "ALL_ATTRIBUTES"
    projected = "ALL_PROJECTED_ATTRIBUTES"
    specific = "SPECIFIC_ATTRIBUTES"
    count = "COUNT"


class DynoQueryOperator(StrEnum):
    eq = "="
    ne = "<>"
    gt = ">"
    lt = "<"
    ge = ">="
    le = "<="
    contains = "contains"
    not_contains = "not_contains"
    begins_with = "begins_with"
    exists = "exists"
    not_exists = "not_exists"

    @classmethod
    def write(cls, value: str | Self) -> str:
        if isinstance(value, str):
            value = DynoQueryOperator(value)
        return str(value.value)


class DynoQuery:
    __slots__ = [
        "_schema_obj", "_key_obj", "_key_fmt", "_key", "_consistent",
        '_limit', '_data', '_link', "_asc", "_select", "_select_attributes",
        "_filter", "_filter_key", "_filter_key_globalindex", "_start_key"
    ]

    def __init__(self,
                 table: type[DynoTable],
                 schema: type[DynoSchema] | None = None,
                 globalindex: None | str = None,
                 limit: None | int = None):
        if isinstance(globalindex, str):
            if table.get_globalindex(globalindex) is None:
                globalindex = None
            elif schema is not None and table.get_globalindex(globalindex) is None:
                globalindex = None

        self._link = table.get_link(schema, globalindex)
        self._limit: None | int = max(1, limit) if isinstance(limit, int) else None

        self._key: None | dict = None

        self._start_key = None
        self._asc: bool = True
        self._consistent: bool = False

        self._filter: DynoFilter = DynoFilter()
        self._filter_key: DynoFilterKey = DynoFilterKey()
        self._filter_key_globalindex: dict[str, DynoFilterKey] = dict[str, DynoFilterKey]()

        self._select = DynoQuerySelectEnum.projected if isinstance(globalindex, str) else DynoQuerySelectEnum.all
        self._select_attributes = set[str]()

        for name, gsi in table.get_globalindexes().items():
            self._filter_key_globalindex[name] = DynoFilterKey()

        if schema is None:
            self._schema_obj = None
            self._key_obj = None
            self._key_fmt = None
        else:
            self._schema_obj = table.get_schema(schema.get_schema_name())
            if isinstance(self._link.globalindex, str):
                self._key_obj = self._link.table.get_globalindex(self._link.globalindex)
                self._key_fmt = self._schema_obj.get_globalindex(self._link.globalindex)
            elif self._schema_obj is not None:
                self._key_obj = self._link.table.Key
                self._key_fmt = self._schema_obj.Key
            else:
                self._key_obj = None
                self._key_fmt = None

            self._filter_key.pk = self._schema_obj.Key.format_pk(dict())
            for name, gsi in self._schema_obj.get_globalindexes().items():
                self._filter_key_globalindex[name].pk = gsi.format_pk(dict())

    def __repr__(self):
        if self._link.schema is None:
            return f"DynoQuery.{self._link.table.TableName}"
        if self._link.globalindex:
            return f"DynoQuery.{self._link.table.TableName}.{self._link.globalindex}"
        return f"DynoQuery.{self._link.table.TableName}.{self._link.schema.get_schema_name()}"

    @property
    def TableName(self) -> str:
        return self._link.table.TableName

    @property
    def Key(self) -> None | DynoFilterKey:
        if isinstance(self._link.globalindex, str):
            return self._filter_key_globalindex.get(self._link.globalindex)
        else:
            return self._filter_key

    @property
    def Attrib(self) -> DynoFilter:
        return self._filter

    def apply_key(self, data: dict[str, any] | None = None) -> None:
        if self._link.schema is None:
            return
        if not isinstance(data, dict):
            data = dict()

        if isinstance(self._link.globalindex, str):
            fmt = self._link.schema.get_globalindex(self._link.globalindex)
        else:
            fmt = self._link.schema.Key

        pk = fmt.format_pk(data)
        sk = fmt.format_sk(data)

        if pk:
            self.Key.pk = pk

        if sk:
            self.Key.op("=", sk)

    def get_link(self) -> DynoTableLink:
        return self._link

    @property
    def limit(self) -> None | int:
        return self._limit

    @limit.setter
    def limit(self, max_results: None | int = None):
        if max_results is None or max_results <= 0:
            self._limit = None
        else:
            self._limit = max(1, max_results)

    def set_consistent_read(self, consistent: None | bool = None):
        self._consistent = consistent if isinstance(consistent, bool) else False

    def set_order(self, asc: None | bool = None) -> None:
        self._asc = asc if isinstance(asc, bool) else True

    def set_select(self, option: DynoQuerySelectEnum):
        self._select = option

    def set_select_attributes(self, names: set[str]):
        for name in names:
            self._select_attributes.add(name)

    @property
    def ismore(self) -> bool:
        return self._start_key is not None

    def set_startkey(self, startkey: None | dict[str, dict[str, any]]) -> Self:
        self._start_key = dict()

        if isinstance(startkey, dict):
            for k, v in startkey.items():
                if isinstance(v, dict):
                    dt = next(iter(v))
                    val = v.get(dt)
                    if k == self._link.table.Key.pk and dt == self._link.table.Key.pk_type.value:
                        self._start_key[k] = {dt: val}
                    elif k == self._link.table.Key.sk and dt == self._link.table.Key.sk_type.value:
                        self._start_key[k] = {dt: val}

        if len(self._start_key) == 0:
            self._start_key = None
        return self

    @property
    def ok(self) -> bool:
        if self.Key.pk is None:
            return False
        return True

    def build(self, for_scan: bool = False) -> None | dict[str, any]:
        params = dict()
        state = DynoAttributeState()

        #
        # Table and Index
        #
        params['TableName'] = self._link.table.TableName
        if isinstance(self._link.globalindex, str):
            params['IndexName'] = self._link.globalindex

        #
        # General Filtering
        #
        try:
            filter_statements = list[str]()
            if not self._filter.is_empty():
                filter_statements.append(self._filter.write(state))
            if self._link.schema and isinstance(self._link.table.SchemaFieldName, str):
                n1 = state.alias(self._link.table.SchemaFieldName)
                v1 = state.add(self._link.schema.get_schema_name())
                filter_statements.append(f"( {n1} = {v1} ) ")
            if len(filter_statements) > 0:
                params['FilterExpression'] = " AND ".join(filter_statements)
        except Exception as e:
            logger.exception(f"DynoQuery.build: general filter")
            raise e

        #
        # Key Filtering
        #
        key_statements = list[str]()
        try:
            if not for_scan:
                if isinstance(self._link.globalindex, str):
                    for name, gsi_filter in self._filter_key_globalindex.items():
                        gsi = self._link.table.get_globalindex(name)
                        s1 = gsi_filter.write(gsi, state)
                        if s1 is not None:
                            key_statements.append(s1)
                elif not self._filter_key.is_empty():
                    s1 = self._filter_key.write(self._link.table.Key, state)
                    if s1 is not None:
                        key_statements.append(s1)
        except Exception as e:
            logger.exception(f"DynoQuery.build: key filter")
            raise e
        if not for_scan:
            if len(key_statements) == 0:
                raise Exception(f"DynoQuery.build - key condition is required")
            params['KeyConditionExpression'] = " AND ".join(key_statements)

        #
        # Selection configuration
        #
        try:
            if len(self._select_attributes) > 0:
                # provided attributes means select specific
                params['ProjectionExpression'] = ", ".join(self._select_attributes)
                params['Select'] = DynoQuerySelectEnum.specific
            else:
                if not isinstance(self._link.globalindex, str) and self._select == DynoQuerySelectEnum.projected:
                    # not a global index with projected, the closest value is "all"
                    params['Select'] = DynoQuerySelectEnum.all.value
                else:
                    params['Select'] = self._select.value
        except Exception as e:
            logger.exception(f"DynoQuery.build: selection configuration")
            raise e

        #
        # Name and Value State
        #
        try:
            params |= state.write()
        except Exception as e:
            logger.exception(f"DynoQuery.build: name and value states")
            raise e

        #
        # Parameters
        #
        if not for_scan:
            params['ScanIndexForward'] = self._asc
        if isinstance(self._limit, int):
            params['Limit'] = self._limit
        if not for_scan:
            params['ConsistentRead'] = self._consistent

        #
        # Pagination
        #
        if isinstance(self._start_key, str):
            params['ExclusiveStartKey'] = {
                self._key_obj.sk: {"S": str(self._start_key)}
            }
        elif isinstance(self._start_key, (int, float)):
            params['ExclusiveStartKey'] = {
                self._key_obj.sk: {"N": str(self._start_key)}
            }
        elif isinstance(self._start_key, bytes):
            params['ExclusiveStartKey'] = {
                self._key_obj.sk: {"B": self._start_key}
            }

        #
        # Measures
        #
        params["ReturnConsumedCapacity"] = "INDEXES"
        return params
