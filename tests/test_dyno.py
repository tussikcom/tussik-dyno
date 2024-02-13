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
    Gsi1 = DynoGlobalIndex("gsi1_pk", "gsi1_sk", read_unit=1, write_unit=1)
    Gsi2 = DynoGlobalIndex("gsi2_pk", "gsi2_sk")

    class AutoIncrement(DynoSchema):
        Key = DynoKeyFormat(pk="autoincrement#", sk="autoincrement#")
        next_accountid = DynoAttribAutoIncrement()
        next_moonid = DynoAttribAutoIncrement()

    class Account(DynoSchema):
        Key = DynoKeyFormat(pk="account#", sk="accountid#{accountid}", req={"accountid"})
        Gsi1 = DynoKeyFormat(pk="account#", sk="alias#{alias}", req={"alias"})
        accountid = DynoAttrUuid()
        next_userid = DynoAttribAutoIncrement()
        active = DynoAttrBool(defval=True)
        alias = DynoAttrString()
        address = SampleAddress()
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)

    class User(DynoSchema):
        Key = DynoKeyFormat(pk="account#user#", sk="accountid#{accountid}#user#{userid}")
        Gsi1 = DynoKeyFormat(pk="user#", sk="email#{email}", req={"email"})
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

    def test_metadata(self):
        sn = SampleTable.Account.get_schema_name()
        account = SampleTable.get_schema("Account")
        schemaname = account.get_schema_name()
        attributes = account.get_attributes(True)
        account.get_autoincrement("foo")
        x = account.get_globalindex("gsi1")
        y = SampleTable.get_link(SampleTable.User, "gsi1")
        assert True

    def test_init_values(self):
        data = {
            "address": {
                "addr1": "123 Main Street",
                "city": "somewhere"
            },
            "color": "red",
            "joe": "skip",
            "modified": 1,
        }
        data = dict()
        result = SampleTable.write_value(data, SampleTable.Account)
        assert True

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

    def test_autoinc_value(self) -> None:
        db = DynoConnect()
        value = db.auto_increment(dict(), SampleTable, SampleTable.AutoIncrement, "next_accountid")
        assert isinstance(value, int)

    def test_insert_account(self) -> None:
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

        dr = db.insert(data_account, SampleTable, SampleTable.Account)
        assert dr.ok
        accountid = dr.data.get('accountid')
        dr = db.get_item(dr.data, SampleTable, SampleTable.Account)
        assert dr.ok

    def test_insert_user(self) -> None:
        db = DynoConnect()
        data_user = {
            "accountid": "AAABBBCCC",
            "userid": None,
            "email": "user@domain.com",
            "flag": "Left",
            "joe": "skip",
            "modified": 1,
        }
        dr = db.insert(data_user, SampleTable, SampleTable.User)
        assert dr.ok
        userid = dr.data.get('userid')
        dr = db.get_item(dr.data, SampleTable, SampleTable.User)
        assert dr.ok

    def test_query_pk(self):
        query = DynoQuery(SampleTable, SampleTable.Account)
        # query.FilterKey().pk("AAABBBCCC")

        # query.FilterKey().op(DynoOpEnum.eq, "sortkey#value")
        # query.FilterExpression().op("accountid", "=", "AAABBBCCC")
        # query.FilterGlobalIndex("gsi1").Contains("test_gsi1")

        query.set_limit(5)
        params = query.build()

        db = DynoConnect()
        dr = db.query(query)

        if dr.LastEvaluatedKey:
            dr = db.query(query)

        assert True

    def test_update(self):
        db = DynoConnect()
        # dr = db.all(SampleTable, "Account", limit=5)
        # row = dr.data[0]
        # item = db.fetch(row['pk'], row['sk'], SampleTable, "Account")
        item = {"accountid": "xsdd", "created": 11111}

        item["address"] = {
            "addr1": "456 Main Street",
            "city": "smallville",
            "zipcode": "90210",
            "country": "US"
        }
        item['junk_field'] = 123
        item['age'] = 45

        update = DynoUpdate(SampleTable, SampleTable.Account)
        update.apply_set(item)

        params = update.build(item)

        db.update(update)
        assert True

    def test_reader(self):
        # db = DynoConnect()
        # dr = db.all(SampleTable, "Account", limit=5)
        # row = dr.data[0]
        # item = db.fetch(row['pk'], row['sk'], SampleTable, "Account")
        item = {"accountid": "xsdd", "created": 11111}

        item["address"] = {
            "addr1": "456 Main Street",
            "city": "smallville",
            "zipcode": "90210",
            "country": "US"
        }
        item['junk_field'] = 123
        item['age'] = 45

        testdata = [
            {"clothing": "shirt", "size": 12},
            {"color": "red", "height": 24}
        ]
        valueA = DynoReader(item)
        valueB = valueA.encode(SampleTable, SampleTable.Account)
        valueC = valueA.decode(SampleTable, SampleTable.Account)
        valueD = valueA.decode(SampleTable)
        valueE = valueA.dataset

        assert True
