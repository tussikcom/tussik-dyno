import datetime
import logging
import uuid
from enum import Enum
from typing import Set, Any, Dict, Type, Self

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

    @classmethod
    def get_datatype(cls, value: Any) -> None | Self:
        if value is None:
            return cls.Null
        if isinstance(value, str):
            return cls.String
        if isinstance(value, bool):
            return cls.Boolean
        if isinstance(value, (int, float)):
            return cls.Number
        if isinstance(value, bytes):
            return cls.Bytes
        if isinstance(value, dict):
            return cls.Map
        if isinstance(value, list):
            row = value[0]
            if isinstance(row, str):
                return cls.StringList
            if isinstance(row, (int, float)):
                return cls.NumberList
            if isinstance(row, bytes):
                return cls.ByteList
            if isinstance(row, dict):
                return cls.List
        return None


class DynoAttribAutoIncrement:
    __slots__ = ['step', 'start']

    def __init__(self, start: int = 0, step: int = 1):
        self.step = max(1, step)
        self.start = max(0, start)

    def __repr__(self):
        return f"DynoAttribAutoIncrement(start={self.start}, step={self.step})"


class DynoAttrBase:
    code: str = ...

    def __init__(self, always: None | bool = None, readonly: None | bool = None):
        self.always = always if isinstance(always, bool) else True
        self.readonly = readonly if isinstance(readonly, bool) else False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} code:{self.code}"

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool) and self.code == DynoEnum.Boolean:
            return value
        if isinstance(value, int) and self.code == DynoEnum.Number:
            return value
        if isinstance(value, float) and self.code == DynoEnum.Number:
            return value
        if isinstance(value, bytes) and self.code == DynoEnum.Bytes:
            return value
        if isinstance(value, str) and self.code == DynoEnum.String:
            return value
        if isinstance(value, list) and self.code == DynoEnum.StringList:
            result = list[str]()
            for item in value:
                if isinstance(item, str):
                    result.append(item)
            return value
        if isinstance(value, list) and self.code == DynoEnum.NumberList:
            result = list[int | float]()
            for item in value:
                if isinstance(item, (int, float)) and item:
                    result.append(item)
            return value
        myname = self.__class__.__name__
        raise ValueError(f"{myname}.read: Unsupported value-type {type(value)}")

    def write_encode(self, value: Any) -> Dict[str, Any]:
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
        if isinstance(value, bool) and self.code == DynoEnum.Boolean:
            return bool(value)
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

        raise ValueError(f"DynoAttrBase:Unsupported value-type {type(value)} when expecting {self.code} dyno-type")


class DynoAttrUuid(DynoAttrBase):
    code: str = DynoEnum.String.value

    def __init__(self, always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)

    def write_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return value
        return str(uuid.uuid4()).replace("-", "")

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, str):
            return {self.code: value}
        return {self.code: str(uuid.uuid4()).replace("-", "")}


class DynoAttrDateTime(DynoAttrBase):
    __slots__ = ["asinteger", "current"]
    code: str = DynoEnum.Number.value

    def __init__(self, asinteger: None | bool = None, current: None | bool = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.asinteger = asinteger if isinstance(asinteger, bool) else True
        self.current = current if isinstance(current, bool) else False

    def write_value(self, value: Any) -> Any:
        if self.current:
            return str(int(datetime.datetime.utcnow().timestamp()))
        if isinstance(value, datetime.datetime):
            return str(int(value.timestamp()))
        if isinstance(value, (int, float)):
            return str(int(value))
        return str(int(datetime.datetime.utcnow().timestamp()))

    def write_encode(self, value: Any) -> Dict[str, Any]:
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
            raise ValueError(f"DynoAttrDateTime.read: Unexpected datatype {datatype}")
        if self.asinteger:
            return int(value)
        dt = datetime.datetime.utcfromtimestamp(int(value))
        return dt


class DynoAttrIntEnum(DynoAttrBase):
    __slots__ = ["enumclass", "defval"]
    code: str = DynoEnum.Number.value

    def __init__(self, enumclass: Type[Enum], defval: Enum,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.enumclass = enumclass
        self.defval = defval if isinstance(defval, enumclass) else None

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            if self.defval is not None:
                return {self.code: str(self.defval.value)}
            return None

        for item in self.enumclass:
            if isinstance(item.value, int) and datatype == DynoEnum.Number:
                if item.value == int(value):
                    return item
        return None

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return str(self.defval.value)
            return None

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoAttrIntEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, int) and item.value == value:
                return int(item.value)

        raise ValueError(f"DynoAttrIntEnum.write: Value {value} is not a valid value")

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval.value)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoAttrIntEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, int) and item.value == value:
                return {self.code: int(item.value)}

        raise ValueError(f"DynoAttrIntEnum.write: Value {value} is not a valid value")


class DynoAttrStrEnum(DynoAttrBase):
    __slots__ = ["enumclass", "defval"]
    code: str = DynoEnum.String.value

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

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return str(self.defval.value)
            return None

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoAttrStrEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, str) and item.value == value:
                return str(item.value)

        raise ValueError(f"DynoAttrStrEnum.write: Value {value} is not a valid value")

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval.value)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, self.enumclass):
            raise ValueError(f"DynoAttrStrEnum.write: Invalid value type {type(value)}")

        for item in self.enumclass:
            if isinstance(item.value, str) and item.value == value:
                return {self.code: str(item.value)}

        raise ValueError(f"DynoAttrStrEnum.write: Value {value} is not a valid value")


class DynoAttrFlag(DynoAttrBase):
    __slots__ = ["options"]
    code: str = DynoEnum.String.value

    def __init__(self, options: Set[str], always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.options = options

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return None
        if datatype != DynoEnum.String:
            raise ValueError(f"DynoAttrFlag.read: Unexpected dyno type {datatype}")
        if not isinstance(value, str):
            raise ValueError(f"DynoAttrFlag.read: Unexpected value type {type(value)}")
        # let any obsolete value slip by
        return value

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"DynoAttrFlag.write: Invalid value type {type(value)}")
        if value not in self.options:
            raise ValueError(f"DynoAttrFlag.write: Value {value} is not a valid flag value")
        return value

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, str):
            raise ValueError(f"DynoAttrFlag.write: Invalid value type {type(value)}")
        if value not in self.options:
            raise ValueError(f"DynoAttrFlag.write: Value {value} is not a valid flag value")
        return {self.code: value}


class DynoAttrString(DynoAttrBase):
    __slots__ = ["fmt_init", "fmt_save", "min_length", "max_length"]
    code: str = DynoEnum.String.value

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

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None

        if not isinstance(value, str):
            raise ValueError(f"DynoAttrString.write: Unsupported type {type(value)} for string entry")

        if self.min_length is not None and len(value) < self.min_length:
            raise ValueError(f"DynoAttrString.write: Length must be greater than {self.min_length}")

        if self.max_length is not None and len(value) > self.max_length:
            return value[:self.max_length]

        return value

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}

        if not isinstance(value, str):
            raise ValueError(f"DynoAttrString.write: Unsupported type {type(value)} for string entry")

        if self.min_length is not None and len(value) < self.min_length:
            raise ValueError(f"DynoAttrString.write: Length must be greater than {self.min_length}")

        if self.max_length is not None and len(value) > self.max_length:
            return {self.code: value[:self.max_length]}

        return {self.code: value}


class DynoAttrStringList(DynoAttrBase):
    code: str = DynoEnum.StringList.value

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[str]()

        if datatype != self.code:
            raise ValueError(f"DynoAttrStringList.read: Unexpected dyno-type {datatype}")

        results = list[str]()
        if isinstance(value, list):
            for item in value:
                results.append(str(item))
        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrStringList.write: Unexpected value type {type(value)}")
        results = list[str]()
        for item in value:
            results.append(str(item))
        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrStringList.write: Unexpected value type {type(value)}")
        results = list[str]()
        for item in value:
            results.append(str(item))
        return {self.code: results}


class DynoAttrIntList(DynoAttrBase):
    code: str = DynoEnum.NumberList.value

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[int]()

        if datatype != self.code:
            raise ValueError(f"DynoAttrIntList.read: Unexpected dyno-type {datatype}")

        results = list[int]()
        if isinstance(value, list):
            for item in value:
                results.append(int(item))
        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrIntList.write: Unexpected value type {type(value)}")
        results = list[int]()
        for item in value:
            results.append(int(item))
        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrIntList.write: Unexpected value type {type(value)}")
        results = list[int]()
        for item in value:
            results.append(int(item))
        return {self.code: results}


class DynoAttrFloatList(DynoAttrBase):
    code: str = DynoEnum.NumberList.value

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[float]()

        if datatype != self.code:
            raise ValueError(f"DynoAttrFloatList.read: Unexpected dyno-type {datatype}")

        results = list[float]()
        if isinstance(value, list):
            for item in value:
                results.append(float(item))
        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrFloatList.write: Unexpected value type {type(value)}")
        results = list[float]()
        for item in value:
            results.append(float(item))
        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrFloatList.write: Unexpected value type {type(value)}")
        results = list[float]()
        for item in value:
            results.append(float(item))
        return {self.code: results}


class DynoAttrByteList(DynoAttrBase):
    code: str = DynoEnum.ByteList.value

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list[float]()

        if datatype != self.code:
            raise ValueError(f"DynoAttrByteList.read: Unexpected dyno-type {datatype}")

        results = list[bytes]()
        if isinstance(value, list):
            for item in value:
                if isinstance(item, bytes):
                    results.append(item)
        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrByteList.write: Unexpected value type {type(value)}")
        results = list[bytes]()
        for item in value:
            if isinstance(item, bytes):
                results.append(item)
        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrByteList.write: Unexpected value type {type(value)}")
        results = list[bytes]()
        for item in value:
            if isinstance(item, bytes):
                results.append(item)
        return {self.code: results}


class DynoAttrInt(DynoAttrBase):
    __slots__ = ["defval", "gt", "ge", "lt", "le"]
    code: str = DynoEnum.Number.value

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

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return str(self.defval)
            return None

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoAttrInt.write: Unexpected value type {type(value)}")
        value = int(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be less than or equal to {self.le}")

        return str(value)

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoAttrInt.write: Unexpected value type {type(value)}")
        value = int(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoAttrInt.write: Value {value} must be less than or equal to {self.le}")

        return {self.code: str(value)}


class DynoAttrFloat(DynoAttrBase):
    __slots__ = ["defval", "gt", "ge", "lt", "le"]
    code: str = DynoEnum.Number.value

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

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return str(self.defval)
            return None

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoAttrFloat.write: Unexpected value type {type(value)}")
        value = float(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be less than or equal to {self.le}")

        return str(value)

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            if self.defval is not None:
                return {self.code: str(self.defval)}
            return {DynoEnum.Null.value: True}

        if not isinstance(value, (int, float)):
            raise ValueError(f"DynoAttrFloat.write: Unexpected value type {type(value)}")
        value = float(value)

        if self.gt is not None and value <= self.gt:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be greater than {self.gt}")
        if self.ge is not None and value < self.ge:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be greater than or equal to {self.ge}")
        if self.lt is not None and value >= self.lt:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be less than {self.lt}")
        if self.le is not None and value > self.le:
            raise ValueError(f"DynoAttrFloat.write: Value {value} must be less than or equal to {self.le}")

        return {self.code: str(value)}


class DynoAttrBool(DynoAttrBase):
    __slots__ = ["defval"]
    code: str = DynoEnum.Boolean.value

    def __init__(self, defval: None | bool = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.defval = defval

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return self.defval
            return None
        return super().write_value(value)


class DynoAttrBytes(DynoAttrBase):
    __slots__ = ["defval"]
    code: str = DynoEnum.Bytes.value

    def __init__(self, defval: None | bytes = None,
                 always: None | bool = None, readonly: None | bool = None):
        super().__init__(always, readonly)
        self.defval = defval

    def write_value(self, value: Any) -> Any:
        if value is None:
            if self.defval is not None:
                return self.defval
            return None
        return super().write_value(value)


class DynoAttrMap(DynoAttrBase):
    code: str = DynoEnum.Map.value

    def get_attributes(self) -> Dict[str, DynoAttrBase]:
        results = dict[str, DynoAttrBase]()

        myclass = self.__class__
        for name, cls_attr in myclass.__dict__.items():
            if isinstance(cls_attr, DynoAttrBase):
                results[name] = cls_attr
        return results

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list()
        if datatype != self.code:
            raise ValueError(f"DynoAttrMap.read: Unexpected dyno-type {datatype}")

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
                    logger.exception(f"DynoAttrMap.read: {e!r}")

            results[k1] = v1_ret

        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError(f"DynoAttrMap.write: Unexpected value-type {type(value)}")

        members = self.get_attributes()
        results = dict[str, Any]()
        for k1, v1 in value.items():
            member = members.get(k1)
            if member is not None:
                try:
                    results[k1] = member.write_value(v1)
                except Exception as e:
                    logger.exception(f"DynoAttrMap.write: {e!r}")
        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, dict):
            raise ValueError(f"DynoAttrMap.write: Unexpected value-type {type(value)}")

        members = self.get_attributes()
        results = dict[str, Any]()
        for k1, v1 in value.items():
            member = members.get(k1)
            if member is not None:
                try:
                    results[k1] = member.write_encode(v1)
                except Exception as e:
                    logger.exception(f"DynoAttrMap.write: {e!r}")
        return {self.code: results}


class DynoAttrList(DynoAttrBase):
    code: str = DynoEnum.List.value

    def get_attributes(self) -> Dict[str, DynoAttrBase]:
        results = dict[str, DynoAttrBase]()

        myclass = self.__class__
        for name, cls_attr in myclass.__dict__.items():
            if isinstance(cls_attr, DynoAttrBase):
                results[name] = cls_attr
        return results

    def read(self, datatype: str, value: Any) -> Any:
        if datatype == DynoEnum.Null:
            return list()
        if datatype != self.code:
            raise ValueError(f"DynoAttrList.read: Unexpected dyno-type {datatype}")

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
                    logger.exception(f"DynoAttrList.read: {e!r}")

            results.append(v1_ret)
        return results

    def write_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrList.write: Unexpected value-type {type(value)}")

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
                    v2_ret = member.write_value(v2)
                    v1_ret[k2] = v2_ret
                except Exception as e:
                    logger.exception(f"DynoAttrList.write: {e!r}")
            if len(v1_ret) > 0:
                results.append(v1_ret)

        return results

    def write_encode(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {DynoEnum.Null.value: True}
        if not isinstance(value, list):
            raise ValueError(f"DynoAttrList.write: Unexpected value-type {type(value)}")

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
                    v2_ret = member.write_encode(v2)
                    v1_ret[k2] = v2_ret
                except Exception as e:
                    logger.exception(f"DynoAttrList.write: {e!r}")
            if len(v1_ret) > 0:
                results.append(v1_ret)

        return {self.code: results}
