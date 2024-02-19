"""Test for connecting"""
from enum import Enum

from tussik.dyno import *
from tussik.dyno.attributes import DynoAttribAutoIncrement
from tussik.dyno.query import DynoQuery, DynoQuerySelectEnum
from tussik.dyno.table import DynoGlobalIndexFormat


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
    SchemaFieldName: str = "myrectype"

    DeletionProtection = True
    TableClassStandard = True
    PayPerRequest = True
    ReadCapacityUnits: int = 1
    WriteCapacityUnits: int = 1

    Key = DynoKey("pk", "sk")
    Indexes = [
        DynoGlobalIndex("gsi1", read_unit=1, write_unit=1),
        DynoGlobalIndex("gsi2")
    ]

    class AutoIncrement(DynoSchema):
        Key = DynoKeyFormat(pk="autoincrement#", sk="autoincrement#")
        next_accountid = DynoAttribAutoIncrement()
        next_moonid = DynoAttribAutoIncrement()

    class Account(DynoSchema):
        Key = DynoKeyFormat(pk="account#", sk="accountid#{accountid}", req={"accountid"})
        Indexes = [DynoGlobalIndexFormat("gsi1", pk="account#", sk="alias#{alias}", req={"alias"})]
        accountid = DynoAttrUuid()
        next_userid = DynoAttribAutoIncrement()
        active = DynoAttrBool(defval=True)
        alias = DynoAttrString()
        address = SampleAddress()
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)

    class User(DynoSchema):
        Key = DynoKeyFormat(pk="account#user#", sk="accountid#{accountid}#user#{userid}")
        Indexes = [DynoGlobalIndexFormat("gsi1", pk="user#", sk="email#{email}", req={"email"})]
        accountid = DynoAttrUuid()
        userid = DynoAttrUuid()
        email = DynoAttrString()
        age = DynoAttrInt(defval=20)
        active = DynoAttrBool(defval=True)
        tags = DynoAttrStringList()
        flag = DynoAttrFlag({"Left", "Right", "Center", "Top", "Bottom"})
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)
        pet = DynoAttrStrEnum(SamplePetEnum, SamplePetEnum.Cat)
        status = DynoAttrIntEnum(SampleStatusEnum, SampleStatusEnum.Two)


SampleTable.isvalid()

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

    def test_metadata(self):
        assert SampleTable.isvalid()

        sn = SampleTable.Account.get_schema_name()
        assert sn

        account = SampleTable.get_schema("Account")
        assert account

        schemaname = account.get_schema_name()
        assert schemaname == sn

        attributes = account.get_attributes(True)
        assert len(attributes) > 0

        foo = account.get_autoincrement("foo")
        assert foo is None
        next_userid = account.get_autoincrement("next_userid")
        assert next_userid

        x = account.get_globalindex("gsi1")
        assert x

        y = SampleTable.get_link(SampleTable.User, "gsi1")
        assert y

    def test_initalize_values(self):
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
        assert result

    def test_autoinc_value(self) -> None:
        db = DynoConnect()
        value = db.auto_increment(dict(), SampleTable, SampleTable.AutoIncrement, "next_accountid")
        assert isinstance(value, int)

    def test_conditional_gsi(self) -> None:
        data_account = {
            "address": {
                "addr1": "123 Main Street",
                "city": "somewhere"
            },
            "color": "red",
            "joe": "skip",
            "modified": 1,
        }

        x1 = SampleTable.write_value(data_account, SampleTable.Account, include_readonly=True)
        assert x1
        assert "gsi1_sk" not in x1

        data_account['alias'] = "fresh_value"
        x2 = SampleTable.write_value(data_account, SampleTable.Account, include_readonly=True)
        assert x2
        assert "gsi1_sk" in x2

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

        dr = db.put_item(data_account, SampleTable, SampleTable.Account)
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
            "tags": ["one", "two"],
            "joe": "skip",
            "modified": 1,
        }
        dr1 = db.put_item(data_user, SampleTable, SampleTable.User)
        assert dr1.ok
        userid = dr1.data.get('userid')
        dr2 = db.get_item(dr1.data, SampleTable, SampleTable.User)

        query = DynoQuery(SampleTable, SampleTable.User, limit=1)
        query.Key.pk = dr1.data.get("pk")
        query.Key.op("=", dr1.data.get("sk"))
        params1 = query.build()
        dr3 = db.query(query)

        query = DynoQuery(SampleTable, SampleTable.User, limit=1)
        query.apply_key(dr1.data)
        params2 = query.build()
        dr4 = db.query(query)

        query = DynoQuery(SampleTable, SampleTable.User)
        query.set_select(DynoQuerySelectEnum.count)
        dr5 = db.query(query)

        assert dr2.ok

    def test_scan(self):
        db = DynoConnect()

        query = DynoQuery(SampleTable)
        dr1 = db.scan(query)
        assert dr1.ok

        query = DynoQuery(SampleTable, SampleTable.Account)
        dr2 = db.scan(query)
        assert dr2.ok

        query = DynoQuery(SampleTable, SampleTable.Account, "gsi1")
        dr3 = db.scan(query)
        assert dr3.ok

    def test_query_all(self):
        db = DynoConnect()

        query = DynoQuery(SampleTable, SampleTable.Account)
        dr0 = db.scan(query)

        dr1 = db.all(SampleTable)
        assert dr1.ok

        dr2 = db.all(SampleTable, SampleTable.Account)
        assert dr2.ok

        dr3 = db.all(SampleTable, SampleTable.User)
        assert dr3.ok

    def test_query_one(self):
        query = DynoQuery(SampleTable, SampleTable.User)
        userid = '286e456d410c4ffdb6d0d52273a68eab'
        query.Key.pk = "user#"
        # query.Key.op("=", f"user#{userid}")
        params = query.build()
        db = DynoConnect()
        dr = db.query(query)
        assert True

    def test_query_pk(self):
        query = DynoQuery(SampleTable, SampleTable.Account, "gsi1")

        query.Key.pk = "user#"
        query.Key.op(DynoOpEnum.eq, "user#1234")
        query.Attrib.op("email", DynoOpEnum.eq, "user@tld.com")

        # query.FilterKey().pk("AAABBBCCC")

        # query.FilterKey().op(DynoOpEnum.eq, "sortkey#value")
        # query.FilterExpression().op("accountid", "=", "AAABBBCCC")
        # query.FilterGlobalIndex("gsi1").Contains("test_gsi1")

        query.limit = 5
        params = query.build()

        db = DynoConnect()
        dr = db.query(query)

        if dr.LastEvaluatedKey:
            dr = db.query(query)

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

    def test_update(self):
        db = DynoConnect()

        dr = db.all(SampleTable, SampleTable.Account)
        assert dr.ok
        accounts = dr.data
        assert len(accounts) > 0

        # dr = db.all(SampleTable, "Account", limit=5)
        # row = dr.data[0]
        # item = db.fetch(row['pk'], row['sk'], SampleTable, "Account")
        item = accounts[0]

        item["address"] = {
            "addr1": "456 Main Street",
            "city": "smallville",
            "zipcode": "90210",
            "country": "US"
        }
        item['junk_field'] = 123
        item['age'] = 45
        item['title'] = "Updated Title"

        update = DynoUpdate(SampleTable, SampleTable.Account)
        update.apply_key(item)
        update.apply_set(item)
        params = update.build()

        dr = db.update(update)
        assert dr.ok

    def test_update_put(self):
        db = DynoConnect()

        dr0 = db.all(SampleTable, SampleTable.User)
        assert dr0.ok
        users = dr0.data
        assert len(users) > 0
        item = users[-1]
        current_age = item['age']
        current_tags = item['tags']

        item["age"] = 65
        item['tags'] = ["thing1", "thing2"]

        dr1 = db.put_item(item, SampleTable, SampleTable.User, enforce_gsi=False, ignore_gsi=True)

        dr2 = db.fetch(item, SampleTable, SampleTable.User)
        new_age = dr2['age']
        new_tags = dr2['tags']

        assert new_age == 65
        assert new_tags == ["thing1", "thing2"]

    def test_update_add(self):
        db = DynoConnect()

        dr0 = db.all(SampleTable, SampleTable.User)
        assert dr0.ok
        users = dr0.data
        assert len(users) > 0
        item = users[0]
        current_age = item['age']
        current_tags = item['tags']

        update = DynoUpdate(SampleTable, SampleTable.User)
        update.apply_key(item)
        # update.apply_add({"age": 5})
        update.apply_add({"tags": "three"})
        update.apply_add({"tags": "four"})
        update.apply_add({"tags": ["five", "six"]})
        # update.apply_delete({"tags": "one"})

        msg = update.build()
        dr1 = db.update(update)

        dr2 = db.fetch(item, SampleTable, SampleTable.User)
        new_age = dr2['age']
        new_tags = dr2['tags']

        assert current_age + 5 == new_age
        assert len(new_tags) == 5


