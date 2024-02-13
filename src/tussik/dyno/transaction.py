import logging
from typing import Dict, Any

from tussik.dyno import DynoUpdate, DynoTable, DynoSchema
from tussik.dyno.query import DynoQuery

logger = logging.getLogger()


class DynoTransact:
    __slots__ = ['_items']

    def __init__(self):
        self._items: list[dict]()

    def append(self, call: dict):
        self._items.append(call)

    def append_autoincrement(self, data: Dict[str, Any], table: type[DynoTable], schema: type[DynoSchema],
                             name: str, reset: bool = False) -> bool:
        params = table.auto_increment(data, schema, name, reset)
        self._items.append(params) # TODO: complete param
        return True

    def append_query(self, query: DynoQuery) -> bool:
        value = query.build()
        if value is not None:
            self._items.append(value) # TODO: complete param
        return True

    def append_update(self, update: DynoUpdate) -> bool:
        value = update.build()
        if value is not None:
            self._items.append(value) # TODO: complete param
        return True

    def build(self) -> list[dict]:
        # TODO: complete params
        return self._items
