import copy
import logging
from enum import Enum
from typing import Self

from .attributes import DynoEnum
from .table import DynoKey, DynoGlobalIndex

logger = logging.getLogger()


class DynoOpEnum(str, Enum):
    eq = "="
    ne = "<>"
    gt = ">"
    lt = "<"
    ge = ">="
    le = "<="

    @classmethod
    def write(cls, value: str | Self) -> str:
        if isinstance(value, str):
            value = DynoOpEnum(value)
        return str(value.value)


class DynoAttributeState:
    __slots__ = ["_names", "_values", "_count_name", "_count_value", "_value_context"]

    def __init__(self, state: Self | None = None):
        if isinstance(state, DynoAttributeState):
            self._count_name = state._count_name
            self._count_value = state._count_value
            self._names = copy.deepcopy(state._names)
            self._values = copy.deepcopy(state._values)
            self._value_context = copy.deepcopy(state._value_context)
        else:
            self._count_name = 0
            self._count_value = 0
            self._names = dict[str, str]()
            self._values = dict[str, dict[str, any]]()
            self._value_context = dict[str, str]()

    def __repr__(self):
        return f"DynoAttributeState with {self._count_name} names, and {self._count_value} values"

    def write(self) -> dict[str, dict]:
        params = dict[str, dict]()
        names = dict[str, str]()
        for name, alias in self._names.items():
            names[alias] = name

        if len(names) > 0:
            params['ExpressionAttributeNames'] = names

        if len(self._values) > 0:
            params['ExpressionAttributeValues'] = self._values
        return params

    def name_exists(self, name: str) -> bool:
        return name in self._names

    def alias(self, name: str) -> str:
        if name not in self._names:
            self._count_name += 1
            self._names[name] = f"#n{self._count_name}"
        return self._names[name]

    def _add_value(self, value: any, alias: str):
        if isinstance(value, str):
            if alias in self._values:
                existing = self._values[alias]
                key = next(iter(existing))
                val = existing[key]
                if isinstance(val, list):
                    val.append(value)
                else:
                    val = [val, value]
                self._values[alias] = {DynoEnum.StringList.value: val}
            else:
                self._values[alias] = {DynoEnum.String.value: value}
            return alias

        if isinstance(value, bool):
            self._values[alias] = {DynoEnum.Boolean.value: value}
            return alias

        if isinstance(value, (int, float)):
            if alias in self._values:
                existing = self._values[alias]
                key = next(iter(existing))
                val = existing[key]
                if isinstance(val, list):
                    val.append(value)
                else:
                    val = [value, val]
                self._values[alias] = {DynoEnum.NumberList.value: val}
            else:
                self._values[alias] = {DynoEnum.Number.value: str(value)}
            return alias

        if isinstance(value, bytes):
            self._values[alias] = {DynoEnum.Bytes.value: value}
            return alias

        if isinstance(value, list):
            if isinstance(value[0], str):
                revalue = [str(x) for x in value if isinstance(x, str)]
                if alias in self._values:
                    existing = self._values[alias]
                    key = next(iter(existing))
                    val = existing[key]
                    revalue += val
                self._values[alias] = {DynoEnum.StringList.value: revalue}
                return alias

            if isinstance(value[0], (int, float)):
                revalue = [str(x) for x in value if isinstance(x, (int, float))]
                if alias in self._values:
                    existing = self._values[alias]
                    key = next(iter(existing))
                    val = existing[key]
                    revalue += val
                self._values[alias] = {DynoEnum.StringList.value: revalue}
                return alias

            if isinstance(value[0], bytes):
                revalue = [x for x in value if isinstance(x, bytes)]
                if alias in self._values:
                    existing = self._values[alias]
                    key = next(iter(existing))
                    val = existing[key]
                    revalue += val
                self._values[alias] = {DynoEnum.NumberList.value: revalue}
                return alias

        self._values[alias] = {DynoEnum.Null.value: True}

    def add(self, value: any, name: None | str = None) -> str:
        alias = None

        if isinstance(name, str):
            alias = self._value_context.get(name)

        if not isinstance(alias, str):
            self._count_value += 1
            alias = f":v{self._count_value}"

        if isinstance(name, str):
            self._value_context[name] = alias

        self._add_value(value, alias)
        return alias


class DynoFilter:
    __slots__ = ['_stack']

    def __init__(self):
        self._stack = list()

    def __repr__(self):
        return f"DynoFilter with {len(self._stack)} filters"

    def is_empty(self) -> bool:
        return len(self._stack) == 0

    def write(self, state: DynoAttributeState) -> str:
        statement = list[str]()

        for item in self._stack:
            if item['type'] == "scope":
                value = item['filter'].write_encode(state)
                if len(statement) > 0:
                    flat = " AND ".join(statement)
                    statement = list[str]()
                    statement.append(f"( {flat} ) {item['op']} ( {value} )")
                else:
                    statement.append(f"( {value} )")

            elif item['type'] == "function":
                names = list[str]()
                for x in item['path']:
                    names.append(state.alias(x))
                n1 = ".".join(names)
                fn = state.alias(item['op'])

                if "comparator" in item:
                    v1 = state.add(item['value'])
                    c = item['comparator']
                    statement.append(f"( {fn} ( {n1} ) {c} {v1} )")
                elif "value" in item:
                    v1 = state.add(item['value'])
                    statement.append(f"( {fn} ( {n1}, {v1} ) )")

            elif item['type'] == "in":
                n1 = state.alias(item['attr'])
                values = item['value']
                value_list = list[str]()
                for x in values:
                    value_list.append(state.add(x))
                statement.append(f"( {n1} IN ({', '.join(value_list)}) )")

            elif item['type'] == "between":
                n1 = state.alias(item['attr'])
                values = item['value']
                v1 = state.add(values[0])
                v2 = state.add(values[1])
                statement.append(f"( {n1} BETWEEN {v1} AND {v2} )")

            elif item['type'] == "value":
                n1 = state.alias(item['attr'])
                v1 = state.add(item['value'])
                statement.append(f"( {n1} {item['op']} {v1} )")

        result = " AND ".join(statement)
        return result

    def reset(self):
        self._stack = list()

    def op(self, attr: str, op: str | DynoOpEnum, value: any) -> Self:

        self._stack.append({
            "type": "value",
            "attr": attr,
            "op": DynoOpEnum.write(op),
            "value": value
        })
        return self

    def op_in(self, attr: str, value: list[any]) -> Self:
        self._stack.append({
            "type": "in",
            "attr": attr,
            "op": "IN",
            "value": value
        })
        return self

    def op_between(self, attr: str, value1: any, value2: any) -> Self:
        self._stack.append({
            "type": "between",
            "attr": attr,
            "op": "BETWEEN",
            "value": [value1, value2]
        })
        return self

    def AND(self, criteria: "DynoFilter") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "AND",
            "filter": criteria
        })
        return self

    def __and__(self, criteria: "DynoFilter") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "AND",
            "filter": criteria
        })
        return self

    def OR(self, criteria: "DynoFilter") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "OR",
            "filter": criteria
        })
        return self

    def __or__(self, criteria: "DynoFilter") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "OR",
            "filter": criteria
        })
        return self

    def NOT(self, criteria: "DynoFilter") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "NOT",
            "filter": criteria
        })
        return self

    def Exists(self, path: str | list[str]) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "op": "attribute_exists"
        })
        return self

    def NotExist(self, path: str | list[str]) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "op": "attribute_not_exists"
        })
        return self

    def AttrType(self, path: str | list[str], datatype: DynoEnum) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": datatype.value,
            "op": "attribute_type"
        })
        return self

    def StartsWith(self, path: str | list[str], value: any) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": value,
            "op": "begins_with"
        })
        return self

    def Contains(self, path: str | list[str], value: any) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": value,
            "op": "contains"
        })
        return self

    def AttrSize(self, path: str | list[str], value: any, op: str | DynoOpEnum = DynoOpEnum.eq) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "comparator": DynoOpEnum.write(op),
            "value": value,
            "op": "size"
        })
        return self


class DynoFilterKey:
    __slots__ = ['_stack', "_pk_value"]

    def __init__(self):
        self._stack = list()
        self._pk_value = None

    def __repr__(self):
        return f"DynoFilterKey with {self._pk_value} filters"

    def is_empty(self) -> bool:
        return len(self._stack) == 0 and self._pk_value is None

    @property
    def pk(self) -> any:
        return self._pk_value

    @pk.setter
    def pk(self, value: any):
        if isinstance(value, (str, int, float, bytes)):
            self._pk_value = value
        else:
            self._pk_value = None

    def write(self, key: DynoKey | DynoGlobalIndex, state: DynoAttributeState) -> None | str:
        statement = list[str]()

        if self._pk_value is not None:
            n1 = state.alias(key.pk)
            v1 = state.add(self._pk_value)
            statement.append(f"( {n1} = {v1} )")

        keyname = key.sk

        for item in self._stack:
            if item['type'] == "scope":
                value = item['filter'].write_encode(keyname, state)
                if len(statement) > 0:
                    flat = " AND ".join(statement)
                    statement = list[str]()
                    statement.append(f"( {flat} ) {item['op']} ( {value} )")
                else:
                    statement.append(f"( {value} )")

            elif item['type'] == "function":
                fn = state.alias(item['op'])

                if "comparator" in item:
                    v1 = state.add(item['value'])
                    c = item['comparator']
                    statement.append(f"( {fn} {c} {v1} )")
                elif "value" in item:
                    n1 = state.alias(keyname)
                    v1 = state.add(item['value'])
                    statement.append(f"( {fn} ( {n1}, {v1} ) )")

            elif item['type'] == "in":
                n1 = state.alias(keyname)
                values = item['value']
                value_list = list[str]()
                for x in values:
                    value_list.append(state.add(x))
                statement.append(f"( {n1} IN ({', '.join(value_list)}) )")

            elif item['type'] == "between":
                n1 = state.alias(keyname)
                values = item['value']
                v1 = state.add(values[0])
                v2 = state.add(values[1])
                statement.append(f"( {n1} BETWEEN {v1} AND {v2} )")

            elif item['type'] == "value":
                n1 = state.alias(keyname)
                v1 = state.add(item['value'])
                statement.append(f"( {n1} {item['op']} {v1} )")

        result = " AND ".join(statement)
        if len(result) == 0:
            return None
        return result

    def reset(self):
        self._stack = list()

    def op(self, op: str | DynoOpEnum, value: any) -> Self:
        self._stack.append({
            "type": "value",
            "op": DynoOpEnum.write(op),
            "value": value
        })
        return self

    def op_in(self, value: list[any]) -> Self:
        self._stack.append({
            "type": "in",
            "op": "IN",
            "value": value
        })
        return self

    def op_between(self, value1: any, value2: any) -> Self:
        self._stack.append({
            "type": "between",
            "op": "BETWEEN",
            "value": [value1, value2]
        })
        return self

    def AND(self, criteria: "DynoFilterKey") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "AND",
            "filter": criteria
        })
        return self

    def __and__(self, criteria: "DynoFilterKey") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "AND",
            "filter": criteria
        })
        return self

    def OR(self, criteria: "DynoFilterKey") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "OR",
            "filter": criteria
        })
        return self

    def __or__(self, criteria: "DynoFilterKey") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "OR",
            "filter": criteria
        })
        return self

    def NOT(self, criteria: "DynoFilterKey") -> Self:
        self._stack.append({
            "type": "scope",
            "op": "NOT",
            "filter": criteria
        })
        return self

    def StartsWith(self, value: any) -> Self:
        self._stack.append({
            "type": "function",
            "value": value,
            "op": "begins_with"
        })
        return self

    def Contains(self, value: any) -> Self:
        self._stack.append({
            "type": "function",
            "value": value,
            "op": "contains"
        })
        return self

    def AttrSize(self, value: any, op: DynoOpEnum = DynoOpEnum.eq) -> Self:
        self._stack.append({
            "type": "function",
            "comparator": DynoOpEnum.write(op),
            "value": value,
            "op": "Size"
        })
        return self
