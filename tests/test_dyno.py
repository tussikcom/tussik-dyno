"""Test for connecting"""
from enum import Enum

from tussik.dyno import *


class SampleAddress(DynoTypeMap):
    addr1 = DynoTypeString()
    addr2 = DynoTypeString()
    city = DynoTypeString()
    state = DynoTypeString(min_length=2, max_length=2)
    country = DynoTypeString(min_length=2, max_length=2)
    zip = DynoTypeString()


class SamplePetEnum(Enum):
    Cat = "cat"
    Dog = "dog"
    Snake = "snake"
    Goat = "goat"


class SampleStatusEnum(Enum):
    One = 1
    Two = 2
    Three = 3
    Four = 4


class SampleTable(DynoTypeTable):
    TableName: str = "sample"
    Key = DynoKey("pk", "sk")
    GlobalIndexes = {
        "gsi1": DynoGlobalIndex("gsi1_pk", "gsi1_sk", read_unit=1, write_unit=1),
        "gsi2": DynoGlobalIndex("gsi2_pk", "gsi2_sk")
    }

    class Account(DynoSchema):
        Key = DynoKeyFormat(pk="account#", sk="accountid#{accountid}", req={"accountid"})
        GlobalIndexes = {
            "gsi1": DynoKeyFormat(pk="account#", sk="alias#{alias}", req={"alias"}),
        }
        accountid = DynoTypeUuid()
        active = DynoTypeBool(defval=True)
        alias = DynoTypeString()
        address = SampleAddress()
        created = DynoTypeDateTime(readonly=True)
        modified = DynoTypeDateTime(current=True)

    class User(DynoSchema):
        Key = DynoKeyFormat(pk="account#user#", sk="accountid#{accountid}#user#{userid}")
        GlobalIndexes = {
            "gsi1": DynoKeyFormat(pk="user#", sk="email#{email}", req={"email"}),
        }
        accountid = DynoTypeUuid()
        userid = DynoTypeUuid()
        email = DynoTypeString()
        age = DynoTypeInt(defval=20)
        active = DynoTypeBool(defval=True)
        flag = DynoTypeFlag({"Left", "Right", "Center", "Top", "Bottom"})
        created = DynoTypeDateTime(readonly=True)
        modified = DynoTypeDateTime(current=True)
        pet = DynoTypeStrEnum(SamplePetEnum, SamplePetEnum.Cat)
        status = DynoTypeIntEnum(SampleStatusEnum, SampleStatusEnum.Two)


DynoConnect.set_host()


class TestDyno:
    def test_account(self) -> None:
        db = DynoConnect()
        data_account = {
            "address": {
                "addr1": "123 Main Street",
                "city": "somewhere"
            },
            "color": "red",
            "joe": "skip",
            "modified": 1,
        }

        value1 = SampleTable.write_key(data_account)

        dr = db.insert(data_account, SampleTable, "Account")
        accountid = dr.data.get('accountid')
        dr = db.get_item(dr.data, SampleTable, "Account")

    def test_user(self) -> None:
        db = DynoConnect()
        data_user = {
            "accountid": "AAABBBCCC",
            "userid": None,
            "email": "user@domain.com",
            "flag": "Left",
            "joe": "skip",
            "modified": 1,
        }

        value1 = SampleTable.write_key(data_user, "User", True)

        dr = db.insert(data_user, SampleTable, "User")
        userid = dr.data.get('userid')
        dr = db.get_item(dr.data, SampleTable, "User")
