import logging
from enum import Enum
from typing import Self

from .attributes import DynoEnum
from .filtering import DynoAttributeState
from .table import DynoTable, DynoTableLink, DynoSchema

logger = logging.getLogger()


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
        "_schema_obj", "_key_obj", "_key_fmt", "_key", "_state", "_state_context",
        '_expression_set', '_expression_add', '_expression_remove', '_expression_delete',
        '_condition_exp', "_link", "_pk", "_sk"
    ]

    def __init__(self, table: type[DynoTable], schema: type[DynoSchema]):
        self._link = table.get_link(schema)
        self._state = DynoAttributeState()
        self._state_context = dict[DynoExpressionEnum, dict[str, str]]()
        self._state_context[DynoExpressionEnum.Add] = dict[str, str]()
        self._state_context[DynoExpressionEnum.Set] = dict[str, str]()
        self._state_context[DynoExpressionEnum.Delete] = dict[str, str]()
        self._state_context[DynoExpressionEnum.Remove] = dict[str, str]()

        #
        # update conditions
        #
        self._condition_exp = list[str]()
        self._expression_set = list[str]()
        self._expression_add = list[str]()
        self._expression_remove = list[str]()
        self._expression_delete = list[str]()

        #
        # keys and filters
        #
        self._pk: any = None
        self._sk: any = None

        #
        # schema
        #
        self._schema_obj = None
        self._key_obj = None
        self._key_fmt = None
        if schema:
            self._schema_obj = table.get_schema(schema.get_schema_name())
            if self._schema_obj is not None:
                self._key_obj = self._link.table.Key
                self._key_fmt = self._schema_obj.Key

                self._pk = self._key_fmt.format_pk()
                self._sk = self._key_fmt.format_sk()
            else:
                self._key_obj = None
                self._key_fmt = None

    def __repr__(self):
        prefix = f"DynoUpdate.{self._link.table.TableName}.{self._link.schema.get_schema_name()}"

        msg = list[str]()
        msg.append(f"{len(self._expression_set)} sets")
        msg.append(f"{len(self._expression_add)} adds")
        msg.append(f"{len(self._expression_delete)} deletes")
        msg.append(f"{len(self._expression_remove)} removes")

        if isinstance(self._link.globalindex, str):
            return f"{prefix}.{self._link.globalindex}: {', '.join(msg)}"
        else:
            return f"{prefix}: {', '.join(msg)}"

    @property
    def TableName(self) -> str:
        return self._link.table.TableName

    def get_link(self) -> DynoTableLink:
        return self._link

    def _context_check(self, ee: DynoExpressionEnum, name: str) -> None | str:
        return self._state_context[ee].get(name)

    def _context_name(self, ee: DynoExpressionEnum, name: str) -> None | str:
        alias = self._state.alias(name)
        self._state_context[ee][name] = alias
        return alias

    def _context_value(self, ee: DynoExpressionEnum, value: any, name: str) -> None | str:
        ctx_name = f"{ee.value}.{name}"
        return self._state.add(value, ctx_name)

    def apply_key(self, data: dict[str, any] | None = None) -> None:
        if self._link.schema is None:
            return

        fmt = self._link.schema.Key
        self._pk = fmt.format_pk(data or dict())
        self._sk = fmt.format_sk(data or dict())

    def add_value(self, value: None | bool | int | float | str | bytes | dict) -> str:
        key = self._state.add(value)
        return key

    def add_name(self, name: str) -> str:
        key = self._state.alias(name)
        return key

    @property
    def ok(self) -> bool:
        n = 0
        n += len(self._expression_set)
        n += len(self._expression_add)
        n += len(self._expression_remove)
        n += len(self._expression_delete)
        return n > 0

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

    def apply_add(self, dataset: dict[str, any]) -> None:
        if isinstance(dataset, dict):
            self._extract(DynoExpressionEnum.Add.value, dataset)

    def apply_set(self, dataset: set[str] | dict[str, any]) -> None:
        if isinstance(dataset, set):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
            self._extract(DynoExpressionEnum.Set.value, data)
        elif isinstance(dataset, dict):
            self._extract(DynoExpressionEnum.Set.value, dataset)

    def apply_remove(self, dataset: set[str] | dict[str, any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
            self._extract(DynoExpressionEnum.Remove.value, data)
        elif isinstance(dataset, dict):
            self._extract(DynoExpressionEnum.Remove.value, dataset)

    def apply_delete(self, dataset: set[str] | dict[str, any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {DynoEnum.Null.value: True}
            self._extract(DynoExpressionEnum.Delete.value, data)
        elif isinstance(dataset, dict):
            self._extract(DynoExpressionEnum.Delete.value, dataset)

    def _extract(self, action: str, dataset: dict[str, dict], prefix: None | str = None) -> None:
        avail = self._schema_obj.get_attributes(nested=True)  # TODO: cache
        for k, v in dataset.items():
            key = k if prefix is None else f"{prefix}.{k}"

            # TODO: also allow pk/sk
            if key not in avail or avail[key].readonly:
                continue

            if isinstance(v, dict):
                self._extract(action, v, key)
            elif action == DynoExpressionEnum.Set.value:
                key_exists = self._context_check(DynoExpressionEnum.Set, key)
                n1 = self._context_name(DynoExpressionEnum.Set, key)
                v1 = self._context_value(DynoExpressionEnum.Set, v, key)
                if not key_exists:
                    self._expression_set.append(f"{n1} = {v1}")
            elif action == DynoExpressionEnum.Add.value:

                if isinstance(v, (int, float)):
                    key_exists = self._context_check(DynoExpressionEnum.Set, key)
                    n1 = self._context_name(DynoExpressionEnum.Set, key)
                    v1 = self._context_value(DynoExpressionEnum.Set, abs(v), key)
                    if not key_exists:
                        self._expression_set.append(f"{n1} = {n1} {'-' if v < 0.0 else '+'} {v1}")
                else:
                    key_exists = self._context_check(DynoExpressionEnum.Add, key)
                    n1 = self._context_name(DynoExpressionEnum.Add, key)
                    v1 = self._context_value(DynoExpressionEnum.Add, v, key)
                    if not key_exists:
                        self._expression_add.append(f"{n1} {v1}")
            elif action == DynoExpressionEnum.Remove.value:
                key_exists = self._context_check(DynoExpressionEnum.Remove, key)
                n1 = self._context_name(DynoExpressionEnum.Remove, key)
                if v is None:
                    if not key_exists:
                        self._expression_remove.append(n1)  # remove attribute
                else:
                    v1 = self._context_value(DynoExpressionEnum.Remove, v, key)
                    if not key_exists:
                        self._expression_remove.append(f"{n1} {v1}")  # remove value in attribute
            elif action == DynoExpressionEnum.Delete.value:
                key_exists = self._context_check(DynoExpressionEnum.Delete, key)
                n1 = self._context_name(DynoExpressionEnum.Delete, key)
                if v is None:
                    if not key_exists:
                        self._expression_delete.append(n1)  # remove attribute
                else:
                    v1 = self._context_value(DynoExpressionEnum.Delete, v, key)
                    if not key_exists:
                        self._expression_delete.append(f"{n1} {v1}")  # remove value in attribute

    def build(self) -> dict[str, any] | None:
        params = dict[str, any]()
        state = DynoAttributeState(self._state)  # copy of current state

        #
        # Table and Key
        #
        params['TableName'] = self._link.table.TableName
        params['Key'] = {
            self._link.table.Key.pk: {self._link.table.Key.pk_type.value: self._pk},
            self._link.table.Key.sk: {self._link.table.Key.sk_type.value: self._sk}
        }

        #
        # add all always include read only attributes
        #
        for name, base in self._schema_obj.get_attributes().items():
            if base.always and base.replace and not state.name_exists(name):
                value = base.write_encode(None)
                if isinstance(value, dict):
                    n1 = state.alias(name)
                    v1 = state.add(next(iter(value.values())))
                    self._expression_set.append(f"{n1} = {v1}")

        #
        # Response Instructions
        #
        params['ReturnValues'] = "UPDATED_NEW"
        params['ReturnConsumedCapacity'] = "TOTAL"

        #
        # name and value state
        #
        params |= state.write()

        #
        # update expression
        #
        text = ""
        if len(self._expression_set) > 0:
            text += f"{DynoExpressionEnum.Set.value} {', '.join(self._expression_set)} "
        if len(self._expression_add) > 0:
            text += f"{DynoExpressionEnum.Add.value} {', '.join(self._expression_add)} "
        if len(self._expression_remove) > 0:
            text += f"{DynoExpressionEnum.Remove.value} {', '.join(self._expression_remove)} "
        if len(self._expression_delete) > 0:
            text += f"{DynoExpressionEnum.Delete.value} {', '.join(self._expression_delete)} "
        params['UpdateExpression'] = text

        return params
