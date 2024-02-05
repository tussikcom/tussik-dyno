import logging
from typing import Type, Dict, Any

from tussik.dyno import DynoUpdate, DynoTable
from tussik.dyno.query import DynoQuery

logger = logging.getLogger()


class DynoTransact:
    __slots__ = ['_items']

    def __init__(self):
        self._items: list[dict]()

    def append(self, call: dict):
        self._items.append(call)

    def append_autoincrement(self, data: Dict[str, Any], table: Type[DynoTable], schema: str,
                             name: str, reset: bool = False) -> bool:
        params = table.auto_increment(data, schema, name, reset)
        self._items.append(params)
        return True

    def append_query(self, query: DynoQuery) -> bool:
        value = query.write()
        if value is not None:
            self._items.append(value)
        return True

    def append_update(self, update: DynoUpdate) -> bool:
        value = update.write()
        if value is not None:
            self._items.append(value)
        return True

    def write(self) -> list[dict]:
        return self._items
