import logging
from enum import Enum
from typing import Set, Any, Dict, Type, Self

from .filtering import DynoFilter, DynoFilterKey, DynoFilterState
from .table import DynoTable, DynoTableLink

logger = logging.getLogger()


class DynoQuerySelectEnum(Enum):
    all = "ALL_ATTRIBUTES"
    projected = "ALL_PROJECTED_ATTRIBUTES"
    specific = "SPECIFIC_ATTRIBUTES"
    count = "COUNT"


class DynoQuery:
    __slots__ = [
        "_schema_obj", "_key_obj", "_key_fmt", "_key", "_consistent",
        '_limit', '_data', '_link', "_asc", "_select", "_select_attributes",
        "_filter", "_filter_key", "_filter_key_globalindex", "_start_key"
    ]

    def __init__(self, table: Type[DynoTable], schema: str, globalindex: None | str = None):
        self._link = table.get_link(schema, globalindex)

        self._key: None | dict = None

        self._start_key = None
        self._limit: None | int = None
        self._asc: bool = True
        self._consistent: bool = False

        self._filter: DynoFilter = DynoFilter()
        self._filter_key: DynoFilterKey = DynoFilterKey()
        self._filter_key_globalindex: Dict[str, DynoFilterKey] = dict[str, DynoFilterKey]()

        self._select = DynoQuerySelectEnum.projected if isinstance(globalindex, str) else DynoQuerySelectEnum.all
        self._select_attributes = set[str]()

        for name, gsi in table.GlobalIndexes.items():
            self._filter_key_globalindex[name] = DynoFilterKey()

        self._schema_obj = table.get_schema(schema)
        if isinstance(self._link.globalindex, str):
            self._key_obj = self._link.table.GlobalIndexes.get(self._link.globalindex)
            self._key_fmt = self._schema_obj.GlobalIndexes.get(self._link.globalindex)
        elif self._schema_obj is not None:
            self._key_obj = self._link.table.Key
            self._key_fmt = self._schema_obj.Key
        else:
            self._key_obj = None
            self._key_fmt = None

        self._filter_key.pk(self._schema_obj.Key.format_pk(dict()))
        for name, gsi in self._schema_obj.GlobalIndexes.items():
            self._filter_key_globalindex[name].pk(gsi.format_pk(dict()))

    @property
    def TableName(self) -> str:
        return self._link.table.TableName

    def get_link(self) -> DynoTableLink:
        return self._link

    def set_limit(self, max_results: None | int = None):
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

    def set_select_attributes(self, names: Set[str]):
        for name in names:
            self._select_attributes.add(name)

    def FilterExpression(self) -> DynoFilter:
        return self._filter

    def StartKey(self, startkey: None | Dict[str, Dict[str, Any]]) -> Self:
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

    def FilterKey(self) -> DynoFilterKey:
        return self._filter_key

    def FilterGlobalIndex(self, globalindex: str) -> None | DynoFilterKey:
        return self._filter_key_globalindex.get(globalindex)

    def write(self) -> None | Dict[str, Any]:
        params = dict()

        params['TableName'] = self._link.table.TableName
        if isinstance(self._link.globalindex, str):
            params['IndexName'] = self._link.globalindex

        state = DynoFilterState()

        if not self._filter.is_empty():
            params['FilterExpression'] = self._filter.write(state)

        statements = list[str]()
        if not self._filter_key.is_empty():
            s1 = self._filter_key.write(self._link.table.Key, state)
            if s1 is not None:
                statements.append(s1)
        if isinstance(self._link.globalindex, str):
            for name, gsi_filter in self._filter_key_globalindex.items():
                gsi = self._link.table.GlobalIndexes.get(name)
                s1 = gsi_filter.write(gsi, state)
                if s1 is not None:
                    statements.append(s1)
        if len(statements) > 0:
            params['KeyConditionExpression'] = " AND ".join(statements)

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

        params |= state.write()

        params['ScanIndexForward'] = self._asc
        if isinstance(self._limit, int):
            params['Limit'] = self._limit
        params['ConsistentRead'] = self._consistent

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

        params["ReturnConsumedCapacity"] = "INDEXES"
        return params
