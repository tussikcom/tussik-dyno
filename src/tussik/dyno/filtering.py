import logging
from enum import Enum
from typing import Any, Self, List

from .attributes import DynoEnum
from .table import DynoKey

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


class DynoFilterState:
    __slots__ = ["_names", "_values", "_count_name", "_count_value"]

    def __init__(self):
        self._count_name = 0
        self._count_value = 0
        self._names = dict[str, str]()
        self._values = dict[str, dict[str, Any]]()

    def __repr__(self):
        return f"DynoFilterState with {self._count_name} names, and {self._count_value} values"

    def write(self) -> dict[str, dict]:
        params = dict[str, dict]()
        names = dict[str, str]()
        for name, alias in self._names.items():
            names[alias] = name
        params['ExpressionAttributeNames'] = names
        params['ExpressionAttributeValues'] = self._values
        return params

    def alias(self, name: str) -> str:
        if name not in self._names:
            self._count_name += 1
            self._names[name] = f"#n{self._count_name}"
        return self._names[name]

    def add(self, value: Any) -> str:
        if isinstance(value, str):
            self._count_value += 1
            alias = f":v{self._count_value}"
            self._values[alias] = {DynoEnum.String.value: value}
            return alias

        if isinstance(value, (int, float)):
            self._count_value += 1
            alias = f":v{self._count_value}"
            self._values[alias] = {DynoEnum.Null.value: str(value)}
            return alias

        if isinstance(value, bool):
            self._count_value += 1
            alias = f":v{self._count_value}"
            self._values[alias] = {DynoEnum.Boolean.value: value}
            return alias

        if isinstance(value, bytes):
            self._count_value += 1
            alias = f":v{self._count_value}"
            self._values[alias] = {DynoEnum.Bytes.value: value}
            return alias

        if isinstance(value, list):
            if isinstance(value[0], (int, float, str)):
                self._count_value += 1
                alias = f":v{self._count_value}"
                revalue = [str(x) for x in value if isinstance(x, (int, float, str))]
                self._values[alias] = {DynoEnum.StringList.value: revalue}
                return alias

            if isinstance(value[0], bytes):
                self._count_value += 1
                alias = f":v{self._count_value}"
                revalue = [x for x in value if isinstance(x, bytes)]
                self._values[alias] = {DynoEnum.NumberList.value: revalue}
                return alias

        self._count_value += 1
        alias = f":v{self._count_value}"
        self._values[alias] = {DynoEnum.Null.value: True}
        return alias


class DynoFilter:
    __slots__ = ['_stack']

    def __init__(self):
        self._stack = list()

    def __repr__(self):
        return f"DynoFilter with {len(self._stack)} filters"

    def is_empty(self) -> bool:
        return len(self._stack) == 0

    def write(self, state: DynoFilterState) -> str:
        statement = list[str]()

        for item in self._stack:
            if item['type'] == "scope":
                value = item['filter'].write(state)
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

    def op(self, attr: str, op: str | DynoOpEnum, value: Any) -> Self:

        self._stack.append({
            "type": "value",
            "attr": attr,
            "op": DynoOpEnum.write(op),
            "value": value
        })
        return self

    def op_in(self, attr: str, value: list[Any]) -> Self:
        self._stack.append({
            "type": "in",
            "attr": attr,
            "op": "IN",
            "value": value
        })
        return self

    def op_between(self, attr: str, value1: Any, value2: Any) -> Self:
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

    def Exists(self, path: str | List[str]) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "op": "attribute_exists"
        })
        return self

    def NotExist(self, path: str | List[str]) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "op": "attribute_not_exists"
        })
        return self

    def AttrType(self, path: str | List[str], datatype: DynoEnum) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": datatype.value,
            "op": "attribute_type"
        })
        return self

    def StartsWith(self, path: str | List[str], value: Any) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": value,
            "op": "begins_with"
        })
        return self

    def Contains(self, path: str | List[str], value: Any) -> Self:
        self._stack.append({
            "type": "function",
            "path": path if isinstance(path, list) else [path],
            "value": value,
            "op": "contains"
        })
        return self

    def AttrSize(self, path: str | List[str], value: Any, op: str | DynoOpEnum = DynoOpEnum.eq) -> Self:
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

    def pk(self, value: Any):
        if isinstance(value, (str, int, float, bytes)):
            self._pk_value = value
        else:
            self._pk_value = None

    def write(self, key: DynoKey, state: DynoFilterState) -> None | str:
        statement = list[str]()

        if self._pk_value is not None:
            n1 = state.alias(key.pk)
            v1 = state.add(self._pk_value)
            statement.append(f"( {n1} = {v1} )")

        keyname = key.sk

        for item in self._stack:
            if item['type'] == "scope":
                value = item['filter'].write(keyname, state)
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

    def op(self, op: DynoOpEnum, value: Any) -> Self:
        self._stack.append({
            "type": "value",
            "op": DynoOpEnum.write(op),
            "value": value
        })
        return self

    def op_in(self, value: list[Any]) -> Self:
        self._stack.append({
            "type": "in",
            "op": "IN",
            "value": value
        })
        return self

    def op_between(self, value1: Any, value2: Any) -> Self:
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

    def StartsWith(self, value: Any) -> Self:
        self._stack.append({
            "type": "function",
            "value": value,
            "op": "begins_with"
        })
        return self

    def Contains(self, value: Any) -> Self:
        self._stack.append({
            "type": "function",
            "value": value,
            "op": "contains"
        })
        return self

    def AttrSize(self, value: Any, op: DynoOpEnum = DynoOpEnum.eq) -> Self:
        self._stack.append({
            "type": "function",
            "comparator": DynoOpEnum.write(op),
            "value": value,
            "op": "Size"
        })
        return self
