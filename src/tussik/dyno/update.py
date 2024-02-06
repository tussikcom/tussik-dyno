import copy
import logging
from typing import Set, Any, Dict, Type

from .attributes import DynoEnum
from .table import DynoTable, DynoTableLink

logger = logging.getLogger()


class DynoUpdate:
    __slots__ = [
        "_schema_obj", "_key_obj", "_key_fmt", "_key",
        '_expression_set', '_expression_add', '_expression_remove', '_expression_delete',
        '_attrib_names', '_attrib_values', '_counter', '_condition_exp', "_link"
    ]

    def __init__(self, table: Type[DynoTable], schema: str, globalindex: None | str = None):
        self._link = table.get_link(schema, globalindex)
        self._counter = 0
        self._condition_exp = list[str]()
        self._expression_set = list[str]()
        self._expression_add = list[str]()
        self._expression_remove = list[str]()
        self._expression_delete = list[str]()
        self._attrib_names = dict[str, str]()
        self._attrib_values = dict[str, dict[str, str | bool]]()
        self._key: None | dict = None

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

    @property
    def TableName(self) -> str:
        return self._link.table.TableName

    def get_link(self) -> DynoTableLink:
        return self._link

    def write(self, data: Dict[str, Any]) -> None | Dict[str, Any]:
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
            for name, gsi in self._link.table.GlobalIndexes.items():
                fmt = self._schema_obj.GlobalIndexes.get(name)
                if fmt is not None:
                    value = fmt.write(gsi, data)
                    if value is not None:
                        result |= value

        #
        # TODO: because param-A was set, that alters GSI param-B as SET entries, or delete GSI param-B
        # TODO: because param-A is null, that alters GSI param-B as DELETE entries
        # TODO: always params get updated as SET entries
        #
        pass

        #
        # changes
        #
        params['Key'] = self._key_fmt.write(self._key_obj, data)
        params['ExpressionAttributeValues'] = self.get_values()
        params['ExpressionAttributeNames'] = self.get_names()
        params['ReturnValues'] = "UPDATED_NEW"
        params['ReturnConsumedCapacity'] = "TOTAL"
        params['UpdateExpression'] = self.get_expression()
        current_count = self._counter

        #
        # constraints
        #
        # TODO: change from not_exist to a match statement such as "#n1 = :v1"
        cond_list = list[str]()
        counter = 0
        if isinstance(self._link.globalindex, str):
            for name, gsi in self._link.table.GlobalIndexes.items():
                if gsi.unique and gsi.pk in result:
                    counter += 1
                    params['ExpressionAttributeNames'][f"#k{counter}"] = gsi.pk
                    params['ExpressionAttributeValues'][f":k{counter}"] = {gsi.pk_type.value: ""}
                    cond_list.append(f"#k{counter} = :k{counter}")
                if gsi.unique and gsi.sk in result:
                    counter += 1
                    params['ExpressionAttributeNames'][f"#k{counter}"] = gsi.sk
                    params['ExpressionAttributeValues'][f":k{counter}"] = {gsi.pk_type.value: ""}
                    cond_list.append(f"#k{counter} = :k{counter}")
        else:
            cond_list.append(f"attribute_not_exists({self._link.table.Key.pk})")
            cond_list.append(f"attribute_not_exists({self._link.table.Key.sk})")
        params['ConditionExpression'] = " AND ".join(cond_list)

        return params

    def add_value(self, value: None | bool | int | float | str | bytes | dict) -> str:
        self._counter += 1
        key = f":v{self._counter}"

        #
        # TODO: list, map, stringlist, numberlist, bytelist
        # TODO: common use with similar other function
        #

        if isinstance(value, dict):
            self._attrib_values[key] = value
        elif isinstance(value, bool):
            self._attrib_values[key] = {DynoEnum.Boolean.value: value}
        elif isinstance(value, int):
            self._attrib_values[key] = {DynoEnum.Number.value: str(value)}
        elif isinstance(value, float):
            self._attrib_values[key] = {DynoEnum.Number.value: str(value)}
        elif isinstance(value, str):
            self._attrib_values[key] = {DynoEnum.String.value: str(value)}
        elif isinstance(value, bytes):
            self._attrib_values[key] = {DynoEnum.Bytes.value: str(value)}
        else:
            self._attrib_values[key] = {DynoEnum.Null.value: True}

        return key

    def add_name(self, name: str) -> str:
        self._counter += 1
        key = f"#n{self._counter}"
        self._attrib_names[key] = name
        return key

    def ok(self) -> bool:
        n = 0
        n += len(self._expression_set)
        n += len(self._expression_add)
        n += len(self._expression_remove)
        n += len(self._expression_delete)
        return n > 0

    def get_key(self) -> None | dict:
        return self._key

    def add_key(self, data: Dict[str, Any]) -> None:
        if self._schema_obj is None:
            self._key = None
            return

        pk = self._key_fmt.format_pk(data)
        sk = self._key_fmt.format_sk(data)
        if pk is None or sk is None:
            self._key = None
            return

        pk_n = self.add_name(self._key_obj.pk)
        pk_v = self.add_name(self._key_obj.pk)
        sk_n = self.add_name(self._key_obj.sk)
        sk_v = self.add_name(self._key_obj.sk)

        self._key = {
            pk_n: {self._key_obj.pk_type.value: pk},
            sk_n: {self._key_obj.sk_type.value: sk},
        }

        self._condition_exp.append(f"{pk_n} = {pk_v}")
        self._condition_exp.append(f"{sk_n} = {sk_v}")

    def get_conditional_expression(self) -> str:
        return " AND ".join(self._condition_exp)

    def get_expression(self) -> str:
        text = ""

        if len(self._expression_set) > 0:
            text += f"SET {','.join(self._expression_set)} "

        if len(self._expression_add) > 0:
            text += f"ADD {','.join(self._expression_set)} "

        if len(self._expression_remove) > 0:
            text += f"REMOVE {','.join(self._expression_set)} "

        if len(self._expression_delete) > 0:
            text += f"DELETE {','.join(self._expression_set)} "

        return text

    def get_names(self) -> Dict[str, str]:
        return copy.deepcopy(self._attrib_names)

    def get_values(self) -> Dict[str, dict[str, str | bool]]:
        return copy.deepcopy(self._attrib_values)

    def apply_auto_increment(self, fieldname: str, step: int = 1):
        alias_n = self.add_name(fieldname)
        alias_1 = self.add_value(step)
        alias_0 = self.add_value(0)
        stmt = f"{alias_n} = if_not_exists({alias_n}, {alias_0}) + {alias_1}"
        self._expression_set.append(stmt)

    def custom_set(self, statement: str) -> None:
        self._expression_set.append(statement)

    def custom_add(self, statement: str) -> None:
        self._expression_add.append(statement)

    def custom_delete(self, statement: str) -> None:
        self._expression_delete.append(statement)

    def custom_remove(self, statement: str) -> None:
        self._expression_remove.append(statement)

    def apply_add(self, dataset: Dict[str, Any]) -> None:
        key = next(iter(dataset))
        if isinstance(dataset[key], dict):
            data = dataset
        else:
            data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract("ADD", data)

    def apply_set(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {"NULL": True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract("SET", data)

    def apply_remove(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {"NULL": True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract("REMOVE", data)

    def apply_delete(self, dataset: Set[str] | Dict[str, Any]) -> None:
        if isinstance(dataset, list):
            data = dict()
            for item in dataset:
                data[item] = {"NULL": True}
        else:
            key = next(iter(dataset))
            if isinstance(dataset[key], dict):
                data = dataset
            else:
                data = self._link.table.read(dataset, self._link.schema, self._link.globalindex)
        self._extract("DELETE", data)

    def _extract(self, action: str, dataset: Dict[str, dict], prefix: None | str = None) -> None:
        for k, v in dataset.items():
            key = k if prefix is None else f"{prefix}{k}"
            if key in self._attrib_names:
                continue

            datatype = next(iter(v))

            if datatype == "M":
                self._extract(action, v, k)
                continue

            self._counter += 1
            idx = self._counter

            if action == "SET":
                self._expression_set.append(f"#n{idx} = :v{idx}")
                self._attrib_names[f"#n{idx}"] = key
                self._attrib_values[f":v{idx}"] = v
            elif action == "ADD":
                self._expression_add.append(f"#n{idx} = :v{idx}")
                self._attrib_names[f"#n{idx}"] = key
                self._attrib_values[f":v{idx}"] = v
            elif action == "REMOVE":
                if datatype == "NULL":
                    self._expression_remove.append(f"#n{idx}")
                else:
                    self._expression_remove.append(f"#n{idx} = :v{idx}")
                self._attrib_names[f"#n{idx}"] = key
                self._attrib_values[f":v{idx}"] = v
            elif action == "DELETE":
                self._expression_delete.append(f"#n{idx}")
                self._attrib_names[f"#n{idx}"] = key
