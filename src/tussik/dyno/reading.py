import logging
from typing import Any, List, Dict

from tussik.dyno import DynoEnum

logger = logging.getLogger()


class DynoReader:
    __slots__ = ['_dataset', '_cache']

    def __init__(self, data: Any):
        self._dataset: None | list | dict = None
        if isinstance(data, list):
            self._dataset = self._read_list(data)
        elif isinstance(data, dict):
            self._dataset = self._read_dict(data)
        self._cache = dict[str, Any]()

    def __repr__(self):
        return f'DynoReader with {len(self._dataset)} elements'

    @property
    def dataset(self) -> None | list | dict:
        return self._dataset

    def _cache_get(self, key: str) -> Any:
        return self._cache.get(key)

    def _cache_set(self, key: str, value: Any) -> None:
        if value is not None:
            self._cache[key] = value

    def _read_list(self, data: list) -> List[dict]:
        dataset = list[dict]()
        for item in data:
            value = self._read_dict(item)
            dataset.append(value)
        return dataset

    def _read_dict(self, data: dict) -> dict:
        dataset = dict[str, Any]()

        for name, value in data.items():
            if isinstance(value, dict) and len(value) == 1:
                key = next(iter(value))
                dt = DynoEnum(key)
                if isinstance(dt, DynoEnum):
                    # THIS IS ENCODED
                    if dt == DynoEnum.Map:
                        dataset[name] = self._read_dict(value[key])
                    elif dt == DynoEnum.List:
                        dataset[name] = self._read_list(value[key])
                    else:
                        dataset[name] = value[key]
                    continue

            if isinstance(value, dict):
                dataset[name] = self._read_dict(value)
            elif isinstance(value, list):
                dataset[name] = self._read_list(value)
            else:
                dataset[name] = value

        return dataset

    def allow_list(self, table: type["DynoTable"], schema: type["DynoSchema"] | None = None) -> Dict[str, DynoEnum]:
        if issubclass(table, type("DynoTable")):
            raise ValueError("table must be DynoTable Type")
        if schema is not None and issubclass(schema, type("DynoSchema")):
            raise ValueError("schema must be DynoSchema Type")

        result = dict[str, DynoEnum]()
        result[table.Key.pk] = table.Key.pk_type
        result[table.Key.sk] = table.Key.sk_type
        for name, gsi in table.get_globalindexes().items():
            result[gsi.pk] = gsi.pk_type
            result[gsi.sk] = gsi.sk_type

        n1 = schema.get_schema_name() if schema else None
        for name, tbl in table.get_schemas().items():
            if schema is not None:
                n2 = tbl.get_schema_name()
                if n1 != n2:
                    continue
            attributes = tbl.get_attributes(nested=True)
            for attr_name, base in attributes.items():
                result[attr_name] = DynoEnum(base.code)
        return result

    def decode(self, table: type["DynoTable"], schema: type['DynoSchema'] | None = None) -> None | dict | list:
        if issubclass(table, type("DynoTable")):
            raise ValueError("table must be DynoTable Type")
        if schema is not None and issubclass(schema, type("DynoSchema")):
            raise ValueError("schema must be DynoSchema Type")

        if self._dataset is None:
            return None

        key = f"{table.__class__.__name__}[{schema}]"
        value = self._cache_get(key)
        if value is not None:
            return value

        allowlist = self.allow_list(table, schema)
        if isinstance(self._dataset, list):
            value = self._decode_list(allowlist, self._dataset)
        elif isinstance(self._dataset, dict):
            value = self._decode_dict(allowlist, self._dataset)
        else:
            value = None

        self._cache_set(key, value)
        return value

    def _decode_list(self, allowlist: Dict[str, DynoEnum], data: Any, prefix: None | str = None) -> list:
        dataset = list()
        for item in data:
            if isinstance(item, list):
                value = self._decode_list(allowlist, item, prefix)
                dataset.append(value)
            elif isinstance(item, dict):
                value = self._decode_dict(allowlist, item, prefix)
                dataset.append(value)
            else:
                dataset.append(item)
        return dataset

    def _decode_dict(self, allowlist: Dict[str, DynoEnum], data: Any, prefix: None | str = None) -> dict:
        dataset = dict()

        for name, item in data.items():
            new_prefix = f"{prefix}.{name}" if isinstance(prefix, str) else name
            if new_prefix not in allowlist:
                continue

            if isinstance(item, list):
                value = self._decode_list(allowlist, item, new_prefix)
                dataset[name] = value
            elif isinstance(item, dict):
                value = self._decode_dict(allowlist, item, new_prefix)
                dataset[name] = value
            else:
                dataset[name] = item
        return dataset

    def encode(self, table: type["DynoTable"], schema: type["DynoSchema"] | None = None) -> None | dict | list:
        if issubclass(table, type("DynoTable")):
            raise ValueError("table must be DynoTable Type")
        if schema is not None and issubclass(schema, type("DynoSchema")):
            raise ValueError("schema must be DynoSchema Type")

        if self._dataset is None:
            return None

        key = f"{table.__class__.__name__}[{schema}]"
        value = self._cache_get(key)
        if value is not None:
            return value

        allowlist = self.allow_list(table, schema)

        if isinstance(self._dataset, list):
            value = self._encode_list(allowlist, self._dataset)
        elif isinstance(self._dataset, dict):
            value = self._encode_dict(allowlist, self._dataset)
        else:
            value = None

        self._cache_set(key, value)
        return value

    def _encode_list(self, allowlist: Dict[str, DynoEnum], data: Any, prefix: None | str = None) -> list:
        dataset = list()
        for item in data:
            if isinstance(item, list):
                value = self._encode_list(allowlist, item, prefix)
                dataset.append(value)
            elif isinstance(item, dict):
                value = self._encode_dict(allowlist, item, prefix)
                dataset.append(value)
            else:
                dataset.append(item)
        return dataset

    def _encode_dict(self, allowlist: Dict[str, DynoEnum], data: Any, prefix: None | str = None) -> dict:
        dataset = dict()

        for name, item in data.items():
            new_prefix = f"{prefix}.{name}" if isinstance(prefix, str) else name
            if new_prefix not in allowlist:
                continue
            dt = allowlist[new_prefix]

            if isinstance(item, list):
                value = self._encode_list(allowlist, item, new_prefix)
                dataset[name] = {dt.value: value}
            elif isinstance(item, dict):
                value = self._encode_dict(allowlist, item, new_prefix)
                dataset[name] = {dt.value: value}
            elif item is None:
                dataset[name] = {DynoEnum.Null.value: True}
            elif dt.value == DynoEnum.Number:
                dataset[name] = {dt.value: str(item)}
            else:
                dataset[name] = {dt.value: item}
        return dataset
