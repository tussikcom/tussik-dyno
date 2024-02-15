import logging
from enum import Enum
from typing import Set, Any, Dict, Self

from .attributes import DynoEnum
from .table import DynoTable, DynoTableLink, DynoGlobalIndex, DynoKey, DynoSchema

logger = logging.getLogger()


class DynoUpdateState:
    __slots__ = ["_names", "_values", "_count_name", "_count_value"]

    def __init__(self):
        self._count_name = 0
        self._count_value = 0
        self._names = dict[str, str]()
        self._values = dict[str, dict[str, Any]]()

    def __repr__(self):
        return f"DynoUpdateState with {self._count_name} names, and {self._count_value} values"

    def write(self) -> dict[str, dict]:
        params = dict[str, dict]()
        names = dict[str, str]()
        for name, alias in self._names.items():
            names[alias] = name
        params['ExpressionAttributeNames'] = names
        params['ExpressionAttributeValues'] = self._values
        return params

    def name_exists(self, name: str) -> bool:
        return name in self._names

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


class DynoExpressionEnum(str, Enum):
    Add = "ADD"
    Set = "SET"
    Remove = "REMOVE"
    Delete = "DELETE"

    @classmethod
    def write(cls, value: str | Self) -> str:
        if isinstance(value, str):
            value = DynoExpressionEnum(value)
        return str(value.value)


class DynoUpdate:
    __slots__ = [
        "_schema_obj", "_key_obj", "_key_fmt", "_key", "_state",
        '_expression_set', '_expression_add', '_expression_remove', '_expression_delete',
        '_attrib_names', '_attrib_values', '_counter', '_condition_exp', "_link"
    ]

    def __init__(self, table: type[DynoTable], schema: type[DynoSchema], globalindex: None | str = None):
        self._link = table.get_link(schema, globalindex)
        self._state = DynoUpdateState()

        self._condition_exp = list[str]()
        self._expression_set = list[str]()
        self._expression_add = list[str]()
        self._expression_remove = list[str]()
        self._expression_delete = list[str]()

        self._key: None | dict = None

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

    @property
    def TableName(self) -> str:
        return self._link.table.TableName

    def get_link(self) -> DynoTableLink:
        return self._link

    def _read_key(self, data: Dict[str, Dict[str, Any]], key: DynoKey | DynoGlobalIndex) -> Dict[str, Any]:
        result = dict[str, Any]()
        value = None
        if key.pk in data:
            rec = data[key.pk]
            if isinstance(rec, dict):
                value = rec.get(key.pk_type.value)
        result[key.pk] = value

        value = None
        if key.sk in data:
            rec = data[key.sk]
            if isinstance(rec, dict):
                value = rec.get(key.sk_type.value)
        result[key.sk] = value
        return result

    def build(self) -> None | Dict[str, Any]:
        params = dict[str, Any]()
        result = dict[str, dict[str, Any]]()

        #
        # not setup for success
        #
        if self._schema_obj is None or self._key_obj is None or self._key_fmt is None:
            logger.error(f"DynoUpdate.write: no schema, key or formatting found")
            return None

        #
        # tables, indexes, and keys
        #
        params['TableName'] = self._link.table.TableName
        if isinstance(self._link.globalindex, str):
            params['Indexname'] = self._link.globalindex
        if self._schema_obj:
            value = self._schema_obj.Key.write(self._link.table.Key, data)
            if value is not None:
                result |= value
            for name, gsi in self._link.table.get_globalindexes().items():
                fmt = self._schema_obj.get_globalindex(name)
                if fmt is not None:
                    #
                    # TODO: fix this
                    #
                    value = fmt.write(gsi, data)
                    if value is not None:
                        result |= value

        #
        # add all always include read only attributes
        #
        for name, base in self._schema_obj.get_attributes().items():
            if base.always and not base.readonly and not self._state.name_exists(name):
                value = base.write_encode(None)
                if isinstance(value, dict):
                    n1 = self._state.alias(name)
                    v1 = self._state.add(next(iter(value.values())))
                    self._expression_set.append(f"{n1} = {v1}")

        #
        # changes
        #
        params['Key'] = self._key_fmt.write_encode(self._key_obj, data)
        params['ReturnValues'] = "UPDATED_NEW"
        params['ReturnConsumedCapacity'] = "TOTAL"
        params |= self._state.write()

        text = ""
        if len(self._expression_set) > 0:
            text += f"{DynoExpressionEnum.Set.value} {','.join(self._expression_set)} "
        if len(self._expression_add) > 0:
            text += f"{DynoExpressionEnum.Add.value} {','.join(self._expression_set)} "
        if len(self._expression_remove) > 0:
            text += f"{DynoExpressionEnum.Remove.value} {','.join(self._expression_set)} "
        if len(self._expression_delete) > 0:
            text += f"{DynoExpressionEnum.Delete.value} {','.join(self._expression_set)} "
        params['UpdateExpression'] = text

        #
        # constraints
        #
        # TODO: change from not_exist to a match statement such as "#n1 = :v1"
        cond_list = list[str]()
        if isinstance(self._link.globalindex, str):
            for name, gsi in self._link.table.get_globalindexes().items():
                if gsi.unique and gsi.pk in result:
                    n1 = self._state.alias(gsi.pk)
                    v1 = self._state.add("")  # TODO:
                    cond_list.append(f"{n1} = {v1}")
                if gsi.unique and gsi.sk in result:
                    n1 = self._state.alias(gsi.sk)
                    v1 = self._state.add("")  # TODO:
                    cond_list.append(f"{n1} = {v1}")
        else:
            n1 = self._state.alias(self._link.table.Key.pk)
            n2 = self._state.alias(self._link.table.Key.sk)
            cond_list.append(f"attribute_not_exists({n1})")
            cond_list.append(f"attribute_not_exists({n2})")
        params['ConditionExpression'] = " AND ".join(cond_list)

        return params

    def add_value(self, value: None | bool | int | float | str | bytes | dict) -> str:
        key = self._state.add(value)
        return key

    def add_name(self, name: str) -> str:
        key = self._state.alias(name)
        return key

    def ok(self) -> bool:
        n = 0
        n += len(self._expression_set)
        n += len(self._expression_add)
        n += len(self._expression_remove)
        n += len(self._expression_delete)
        return n > 0

    # def add_key(self, data: Dict[str, Any]) -> None:
    #     #
    #     # TODO: is this method needed?
    #     #
    #     if self._schema_obj is None:
    #         self._key = None
    #         return
    #
    #     pk = self._key_fmt.format_pk(data)
    #     sk = self._key_fmt.format_sk(data)
    #     if pk is None or sk is None:
    #         self._key = None
    #         return
    #
    #     pk_n = self.add_name(self._key_obj.pk)
    #     pk_v = self.add_name(self._key_obj.pk)
    #     sk_n = self.add_name(self._key_obj.sk)
    #     sk_v = self.add_name(self._key_obj.sk)
    #
    #     self._key = {
    #         pk_n: {self._key_obj.pk_type.value: pk},
    #         sk_n: {self._key_obj.sk_type.value: sk},
    #     }
    #
    #     self._condition_exp.append(f"{pk_n} = {pk_v}")
    #     self._condition_exp.append(f"{sk_n} = {sk_v}")
    #
    # def get_conditional_expression(self) -> str:
    #     return " AND ".join(self._condition_exp)

    def apply_auto_increment(self, fieldname: str, step: int = 1):
        n1 = self.add_name(fieldname)
        v1 = self.add_value(step)
        v2 = self.add_value(0)
        stmt = f"{n1} = if_not_exists({n1}, {v1}) + {v2}"
        self._expression_set.append(stmt)

    def apply_custom(self, expression: str | DynoExpressionEnum, statement: str) -> None:
        value = DynoExpressionEnum.write(expression)
        if value == DynoExpressionEnum.Add:
            self._expression_add.append(statement)
        elif value == DynoExpressionEnum.Set:
            self._expression_set.append(statement)
        elif value == DynoExpressionEnum.Delete:
            self._expression_delete.append(statement)
        elif value == DynoExpressionEnum.Remove:
            self._expression_remove.append(statement)

    def apply_add(self, dataset: Dict[str, Any]) -> None:
        key = next(iter(dataset))
        if isinstance(dataset[key], dict):
            data = dataset
        else:
            data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract(DynoExpressionEnum.Add.value, data)

    def apply_set(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract(DynoExpressionEnum.Set.value, data)

    def apply_remove(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract(DynoExpressionEnum.Remove.value, data)

    def apply_delete(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract(DynoExpressionEnum.Delete.value, data)

    def _extract(self, action: str, dataset: Dict[str, dict], prefix: None | str = None) -> None:
        avail = self._schema_obj.get_attributes(nested=True)  # TODO: cache
        for k, v in dataset.items():
            key = k if prefix is None else f"{prefix}.{k}"

            # TODO: also allow pk/sk
            if key not in avail or avail[key].readonly:
                continue

            datatype = next(iter(v))

            if datatype == DynoEnum.Map.value:
                self._extract(action, v, k)
                continue

            if action == DynoExpressionEnum.Set.value:
                n1 = self._state.alias(key)
                v1 = self._state.add(v)
                self._expression_set.append(f"{n1} = {v1}")
            elif action == DynoExpressionEnum.Add.value:
                n1 = self._state.alias(key)
                v1 = self._state.add(v)
                self._expression_set.append(f"{n1} = {v1}")
            elif action == DynoExpressionEnum.Remove.value:
                n1 = self._state.alias(key)
                if datatype == DynoEnum.Null.value:
                    self._expression_remove.append(n1)
                else:
                    v1 = self._state.add(v)
                    self._expression_remove.append(f"{n1} = {v1}")
            elif action == DynoExpressionEnum.Delete.value:
                n1 = self._state.alias(key)
                self._expression_delete.append(n1)
