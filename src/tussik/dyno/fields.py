import datetime
import logging
import uuid
from enum import Enum
from typing import Set, Any, Dict, Type

logger = logging.getLogger()


class DynoEnum(str, Enum):
    String = "S"
    Number = "N"
    Bytes = "B"
    Null = "NULL"
    Boolean = "BOOL"
    StringList = "SS"
    NumberList = "NS"
    ByteList = "BS"
    Map = "M"
    List = "L"


class DynoTypeBase:
    code: str = ...
    readonly: bool = False
    always: bool = True

    def __init__(self, always: None | bool = None, readonly: None | bool = None):
        DynoTypeBase.always = always if isinstance(always, bool) else True
        DynoTypeBase.readonly = readonly if isinstance(readonly, bool) else False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} code:{self.code}"

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {self.code: True}
        if isinstance(value, bool) and self.code == DynoEnum.Boolean:
            return {self.code: value}
        if isinstance(value, int) and self.code == DynoEnum.Number:
            return {self.code: value}
        if isinstance(value, float) and self.code == DynoEnum.Number:
            return {self.code: value}
        if isinstance(value, bytes) and self.code == DynoEnum.Bytes:
            return {self.code: value}
        if isinstance(value, str) and self.code == DynoEnum.String:
            return {self.code: value}
        if isinstance(value, list) and self.code == DynoEnum.StringList:
            result = list[str]()
            for item in value:
                if isinstance(item, str):
                    result.append(item)
            return {self.code: value}
        if isinstance(value, list) and self.code == DynoEnum.NumberList:
            result = list[int | float]()
            for item in value:
                if isinstance(item, (int, float)) and item:
                    result.append(item)
            return {self.code: result}
        myname = self.__class__.__name__
        raise ValueError(f"{myname}.read: Unsupported value-type {type(value)}")

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None

        if datatype != self.code:
            myname = self.__class__.__name__
            raise ValueError(f"{myname}.write: expecting type {self.code}")

        if isinstance(value, str) and self.code == DynoEnum.String:
            return value
        if isinstance(value, (float, str)) and self.code == DynoEnum.Number:
            return float(value)
        if isinstance(value, (int, str)) and self.code == DynoEnum.Number:
            return int(value)
        if isinstance(value, bytes) and self.code == DynoEnum.Bytes:
            return value
        if isinstance(value, list) and self.code == DynoEnum.StringList:
            results = list[str]()
            for item in value:
                if isinstance(item, str):
                    results.append(item)
            return results
        if isinstance(value, list) and self.code == DynoEnum.NumberList:
            results = list[int | float]()
            for item in value:
                if isinstance(item, str):
                    results.append(float(item))
                if isinstance(item, (int, float)):
                    results.append(item)
            return results

        raise ValueError(f"DynoTypeBase:Unsupported value-type {type(value)} when expecting {self.code} dyno-type")


class DynoTypeUuid(DynoTypeBase):
    code: str = "S"

    def __init__(self, always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)

    def write(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, str):
            return {self.code: value}
        return {self.code: str(uuid.uuid4()).replace("-", "")}


class DynoTypeDateTime(DynoTypeBase):
    __slots__ = ["asinteger", "current"]
    code: str = "N"

    def __init__(self, asinteger: None | bool = None, current: None | bool = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.asinteger = asinteger if isinstance(asinteger, bool) else True
        self.current = current if isinstance(current, bool) else False

    def write(self, value: Any) -> Dict[str, Any]:
        if self.current:
            return {self.code: str(int(datetime.datetime.utcnow().timestamp()))}
        if isinstance(value, datetime.datetime):
            return {self.code: str(int(value.timestamp()))}
        if isinstance(value, (int, float)):
            return {self.code: str(int(value))}
        return {self.code: str(int(datetime.datetime.utcnow().timestamp()))}

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None
        if datatype != self.code:
            raise ValueError(f"DynoTypeDateTime.read: Unexpected datatype {datatype}")
        if self.asinteger:
            return int(value)
        dt = datetime.datetime.utcfromtimestamp(int(value))
        return dt


class DynoTypeIntEnum(DynoTypeBase):
    __slots__ = ["enumclass", "defval"]
    code: str = "N"

    def __init__(self, enumclass: Type[Enum], defval: Enum,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.enumclass = enumclass
        self.defval = defval if isinstance(defval, enumclass) else None

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None

        for item in self.enumclass:
            if isinstance(item.value, int) and datatype == DynoEnum.Number:
                if item.value == int(value):
                    return item
        return None

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval.value)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoTypeIntEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, int) and item.value == value:
                return {self.code: int(item.value)}

        raise ValueError(f"DynoTypeIntEnum.write: Value {value} is not a valid value")


class DynoTypeStrEnum(DynoTypeBase):
    __slots__ = ["enumclass", "defval"]
    code: str = "S"

    def __init__(self, enumclass: Type[Enum], defval: Enum,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.enumclass = enumclass
        self.defval = defval if isinstance(defval, enumclass) else None

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None

        for item in self.enumclass:
            if isinstance(item.value, str) and datatype == DynoEnum.String:
                if item.value == str(value):
                    return item
        return None

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval.value)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoTypeStrEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, str) and item.value == value:
                return {self.code: str(item.value)}

        raise ValueError(f"DynoTypeStrEnum.write: Value {value} is not a valid value")


class DynoTypeFlag(DynoTypeBase):
    __slots__ = ["options"]
    code: str = "S"

    def __init__(self, options: Set[str], always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.options = options

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None
        if datatype != DynoEnum.String:
            raise ValueError(f"DynoTypeFlag.read: Unexpected dyno type {datatype}")
        if not isinstance(value, str):
            raise ValueError(f"DynoTypeFlag.read: Unexpected value type {type(value)}")
        # let any obsolete value slip by
        return value

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, str):
            raise ValueError(f"DynoTypeFlag.write: Invalid value type {type(value)}")
        if value not in self.options:
            raise ValueError(f"DynoTypeFlag.write: Value {value} is not a valid flag value")
        return {self.code: value}


class DynoTypeString(DynoTypeBase):
    __slots__ = ["fmt_init", "fmt_save", "min_length", "max_length"]
    code: str = "S"

    def __init__(self,
                 always: None | bool = None, readonly: None | bool = None,
                 fmt_init: None | str = None, fmt_save: None | str = None,
                 min_length: None | int = None, max_length: None | int = None):
        super().__init__(always, readonly)
        self.fmt_init = fmt_init
        self.fmt_save = fmt_save
        self.min_length = min_length
        if isinstance(min_length, int) and isinstance(max_length, int):
            self.max_length = max(max_length, min_length)
        else:
            self.max_length = max_length if isinstance(max_length, int) else None

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}

        if not isinstance(value, str):
            raise ValueError(f"DynoTypeString.write: Unsupported type {type(value)} for string entry")

        if self.min_length is not None and len(value) < self.min_length:
            raise ValueError(f"DynoTypeString.write: Length must be greater than {self.min_length}")

        if self.max_length is not None and len(value) > self.max_length:
            return {self.code: value[:self.max_length]}

        return {self.code: value}


class DynoTypeStringList(DynoTypeBase):
    code: str = "SS"

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[str]()

        if datatype != self.code:
            raise ValueError(f"DynoTypeStringList.read: Unexpected dyno-type {datatype}")

        results = list[str]()
        if isinstance(value, list):
            for item in value:
                results.append(str(item))
        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoTypeStringList.write: Unexpected value type {type(value)}")
        results = list[str]()
        for item in value:
            results.append(str(item))
        return {self.code: results}


class DynoTypeIntList(DynoTypeBase):
    code: str = "NS"

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[int]()

        if datatype != self.code:
            raise ValueError(f"DynoTypeIntList.read: Unexpected dyno-type {datatype}")

        results = list[int]()
        if isinstance(value, list):
            for item in value:
                results.append(int(item))
        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoTypeIntList.write: Unexpected value type {type(value)}")
        results = list[int]()
        for item in value:
            results.append(int(item))
        return {self.code: results}


class DynoTypeFloatList(DynoTypeBase):
    code: str = "NS"

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[float]()

        if datatype != self.code:
            raise ValueError(f"DynoTypeFloatList.read: Unexpected dyno-type {datatype}")

        results = list[float]()
        if isinstance(value, list):
            for item in value:
                results.append(float(item))
        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoTypeFloatList.write: Unexpected value type {type(value)}")
        results = list[float]()
        for item in value:
            results.append(float(item))
        return {self.code: results}


class DynoTypeByteList(DynoTypeBase):
    code: str = "BS"

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[float]()

        if datatype != self.code:
            raise ValueError(f"DynoTypeByteList.read: Unexpected dyno-type {datatype}")

        results = list[bytes]()
        if isinstance(value, list):
            for item in value:
                if isinstance(item, bytes):
                    results.append(item)
        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoTypeByteList.write: Unexpected value type {type(value)}")
        results = list[bytes]()
        for item in value:
            if isinstance(item, bytes):
                results.append(item)
        return {self.code: results}


class DynoTypeInt(DynoTypeBase):
    __slots__ = ["defval", "gt", "ge", "lt", "le"]
    code: str = "N"

    def __init__(self, defval: None | int = None,
                 always: None | bool = None, readonly: None | bool = None,
                 gt: None | int = None, ge: None | int = None,
                 lt: None | int = None, le: None | int = None):
        super().__init__(always, readonly)
        self.defval = defval
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None
        if datatype != DynoEnum.Number:
            return None  # just accept it silently
        return int(value)

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: self.defval}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoTypeInt.write: Unexpected value type {type(value)}")
        value = int(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoTypeInt.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoTypeInt.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoTypeInt.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoTypeInt.write: Value {value} must be less than or equal to {self.le}")

        return {self.code: str(value)}


class DynoTypeFloat(DynoTypeBase):
    __slots__ = ["defval", "gt", "ge", "lt", "le"]
    code: str = "N"

    def __init__(self, defval: None | float = None,
                 always: None | bool = None, readonly: None | bool = None,
                 gt: None | float = None, ge: None | float = None,
                 lt: None | float = None, le: None | float = None):
        super().__init__(always, readonly)
        self.defval = defval
        self.gt = gt
        self.ge = ge
        self.lt = lt
        self.le = le

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None
        if datatype != DynoEnum.Number:
            return None  # just accept it silently
        return float(value)

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoTypeFloat.write: Unexpected value type {type(value)}")
        value = float(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoTypeFloat.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoTypeFloat.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoTypeFloat.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoTypeFloat.write: Value {value} must be less than or equal to {self.le}")

        return {self.code: str(value)}


class DynoTypeBool(DynoTypeBase):
    __slots__ = ["defval"]
    code: str = "BOOL"

    def __init__(self, defval: None | bool = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.defval = defval


class DynoTypeBytes(DynoTypeBase):
    __slots__ = ["defval"]
    code: str = "B"

    def __init__(self, defval: None | bytes = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.defval = defval


class DynoTypeMap(DynoTypeBase):
    code: str = "M"

    def get_attributes(self) -> Dict[str, DynoTypeBase]:
        results = dict[str, DynoTypeBase]()

        myclass = self.__class__
        for name, cls_attr in myclass.__dict__.items():
            if isinstance(cls_attr, DynoTypeBase):
                results[name] = cls_attr
        return results

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list()
        if datatype != self.code:
            raise ValueError(f"DynoTypeMap.read: Unexpected dyno-type {datatype}")

        results = dict[str, dict]()
        if not isinstance(value, dict):
            return results

        members = self.get_attributes()
        for k1, v1 in value.items():
            if not isinstance(v1, dict):
                continue
            v1_ret = dict[str, Any]()
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                v_type = next(iter(v2))
                v_value = v2.get(v_type)
                member = members.get(k2)
                if member is None:
                    continue

                try:
                    v2_ret = member.read(v_type, v_value)
                    v1_ret[k2] = v2_ret
                except Exception as e:
                    logger.exception(f"DynoTypeMap.read: {e!r}")

            results[k1] = v1_ret

        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, dict):
            raise ValueError(f"DynoTypeMap.write: Unexpected value-type {type(value)}")

        members = self.get_attributes()
        results = dict[str, Any]()
        for k1, v1 in value.items():
            member = members.get(k1)
            if member is not None:
                try:
                    results[k1] = member.write(v1)
                except Exception as e:
                    logger.exception(f"DynoTypeMap.write: {e!r}")
        return {self.code: results}

    def old_write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, dict):
            raise ValueError(f"DynoTypeMap.write: Unexpected value-type {type(value)}")

        members = self.get_attributes()
        results = dict[str, Any]()
        for k1, v1 in value.items():
            v1_ret = dict[str, Any]()
            for k2, v2 in v1.items():
                member = members.get(k2)
                if member is not None:
                    try:
                        v1_ret[k2] = member.write(v2)
                    except Exception as e:
                        logger.exception(f"DynoTypeMap.write: {e!r}")
            if len(v1_ret) > 0:
                results[k1] = v1_ret
        return {self.code: results}


class DynoTypeList(DynoTypeBase):
    code: str = "L"

    def get_attributes(self) -> Dict[str, DynoTypeBase]:
        results = dict[str, DynoTypeBase]()

        myclass = self.__class__
        for name, cls_attr in myclass.__dict__.items():
            if isinstance(cls_attr, DynoTypeBase):
                results[name] = cls_attr
        return results

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list()
        if datatype != self.code:
            raise ValueError(f"DynoTypeList.read: Unexpected dyno-type {datatype}")

        results = list[dict]()
        if not isinstance(value, list):
            return results

        members = self.get_attributes()
        for v1 in value:
            if not isinstance(v1, dict):
                continue
            v1_ret = dict[str, Any]()
            for k2, v2 in v1.items():
                if not isinstance(v2, dict):
                    continue
                v_type = next(iter(v2))
                v_value = v2.get(v_type)
                member = members.get(k2)
                if member is None:
                    continue
                try:
                    v2_ret = member.read(v_type, v_value)
                    v1_ret[k2] = v2_ret
                except Exception as e:
                    logger.exception(f"DynoTypeList.read: {e!r}")

            results.append(v1_ret)
        return results

    def write(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoTypeList.write: Unexpected value-type {type(value)}")

        members = self.get_attributes()
        results = list[dict[str, Any]]()
        for v1 in value:
            if not isinstance(v1, dict):
                continue
            v1_ret = dict[str, Any]()
            for k2, v2 in v1.items():
                member = members.get(k2)
                if member is None:
                    continue
                try:
                    v2_ret = member.write(v2)
                    v1_ret[k2] = v2_ret
                except Exception as e:
                    logger.exception(f"DynoTypeList.write: {e!r}")
            if len(v1_ret) > 0:
                results.append(v1_ret)

        return {self.code: results}
