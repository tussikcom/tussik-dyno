"""Test for connecting"""
from enum import Enum

from tussik.dyno import *
from tussik.dyno.attributes import DynoAttribAutoIncrement
from tussik.dyno.query import DynoQuery


class SampleAddress(DynoAttrMap):
    addr1 = DynoAttrString()
    addr2 = DynoAttrString()
    city = DynoAttrString()
    state = DynoAttrString(min_length=2, max_length=2)
    country = DynoAttrString(min_length=2, max_length=2)
    zip = DynoAttrString()


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


class SampleTable(DynoTable):
    TableName: str = "sample"
    SchemaFieldName: str = "type"

    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    Key = DynoKey("pk", "sk")
    GlobalIndexes = {
        "gsi1": DynoGlobalIndex("gsi1_pk", "gsi1_sk", read_unit=1, write_unit=1),
        "gsi2": DynoGlobalIndex("gsi2_pk", "gsi2_sk")
    }

    class AutoIncrement(DynoSchema):
        SchemaFieldValue = "autoincrement"
        Key = DynoKeyFormat(pk="autoincrement#", sk="autoincrement#")
        next_accountid = DynoAttribAutoIncrement()
        next_moonid = DynoAttribAutoIncrement()

    class Account(DynoSchema):
        SchemaFieldValue = "account"
        Key = DynoKeyFormat(pk="account#", sk="accountid#{accountid}", req={"accountid"})
        GlobalIndexes = {
            "gsi1": DynoKeyFormat(pk="account#", sk="alias#{alias}", req={"alias"}),
        }
        accountid = DynoAttrUuid()
        next_userid = DynoAttribAutoIncrement()
        active = DynoAttrBool(defval=True)
        alias = DynoAttrString()
        address = SampleAddress()
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)

    class User(DynoSchema):
        SchemaFieldValue = "user"
        Key = DynoKeyFormat(pk="account#user#", sk="accountid#{accountid}#user#{userid}")
        GlobalIndexes = {
            "gsi1": DynoKeyFormat(pk="user#", sk="email#{email}", req={"email"}),
        }
        accountid = DynoAttrUuid()
        userid = DynoAttrUuid()
        email = DynoAttrString()
        age = DynoAttrInt(defval=20)
        active = DynoAttrBool(defval=True)
        flag = DynoAttrFlag({"Left", "Right", "Center", "Top", "Bottom"})
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)
        pet = DynoAttrStrEnum(SamplePetEnum, SamplePetEnum.Cat)
        status = DynoAttrIntEnum(SampleStatusEnum, SampleStatusEnum.Two)


DynoConnect.set_host()


class TestDyno:

    def test_create_table(self) -> None:
        db = DynoConnect()
        dr = db.table_create(SampleTable)
        assert dr.ok or dr.code == 400

    def test_delete_table(self) -> None:
        db = DynoConnect()
        dr = db.table_protect(SampleTable, False)
        assert dr.ok
        dr = db.table_delete(SampleTable)
        assert dr.ok

    def test_autoinc(self) -> None:
        db = DynoConnect()
        value = db.auto_increment(dict(), SampleTable, "AutoIncrement", "next_accountid")
        assert isinstance(value, int)

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

        dr = db.insert(data_account, SampleTable, "Account")
        assert dr.ok
        accountid = dr.data.get('accountid')
        dr = db.get_item(dr.data, SampleTable, "Account")
        assert dr.ok

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
        dr = db.insert(data_user, SampleTable, "User")
        assert dr.ok
        userid = dr.data.get('userid')
        dr = db.get_item(dr.data, SampleTable, "User")
        assert dr.ok

    def test_query_pk(self):
        query = DynoQuery(SampleTable, "Account")
        #query.FilterKey().pk("AAABBBCCC")

        #query.FilterKey().op(DynoOpEnum.eq, "sortkey#value")
        #query.FilterExpression().op("accountid", "=", "AAABBBCCC")
        #query.FilterGlobalIndex("gsi1").Contains("test_gsi1")

        query.set_limit(5)
        params = query.write()

        # r = ddo.query(
        #     TableName="primary",
        #     ExpressionAttributeValues={":v1": {"S": "account#"}},
        #     KeyConditionExpression="pk = :v1"
        # )

        db = DynoConnect()
        dr = db.query(query)

        if dr.LastEvaluatedKey:
            dr = db.query(query)

        assert True
