import logging

from tussik.dyno import DynoAttrMap, DynoAttrString, DynoTable, DynoKey, DynoGlobalIndex, DynoSchema, DynoKeyFormat, \
    DynoAttrUuid, DynoAttrBool, DynoAttrDateTime, DynoAttrInt, DynoAttrFlag, DynoConnect

logger = logging.getLogger()


class SampleAddress(DynoAttrMap):
    addr1 = DynoAttrString()
    addr2 = DynoAttrString()
    city = DynoAttrString()
    state = DynoAttrString(min_length=2, max_length=2)
    country = DynoAttrString(min_length=2, max_length=2)
    zip = DynoAttrString()


class SampleTable(DynoTable):
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
        accountid = DynoAttrUuid()
        active = DynoAttrBool(defval=True)
        alias = DynoAttrString()
        address = SampleAddress()
        created = DynoAttrDateTime(readonly=True)
        modified = DynoAttrDateTime(current=True)

    class User(DynoSchema):
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


#
# TEST ZONE
#
DynoConnect.set_host()
x = SampleTable.get_schemas()

db = DynoConnect()
data1 = {
    "accountid": "AAABBBCCC",
    "userid": None,
    "email": "user@domain.com",
    "flag": "Left",
    "joe": "skip",
    "modified": 1,
}
y = SampleTable.get_schema("User")
y1 = y.get_attributes()
yy1 = y.write(data1, True)
yyy1 = SampleTable.write_key(data1, "User", True)
yyy2 = SampleTable.write_key(data1, "User", False)
yyy3 = SampleTable.write_globalindex(data1, "User", "gsi1", False)
yy2 = y.read(yyy1)

data2 = {
    "accountid": None,
    "address": {
        "home": {
            "addr1": "123 Main Street",
            "city": "somewhere"
        }
    },
    "color": "red",
    "joe": "skip",
    "modified": 1,
}
z = SampleTable.get_schema("Account")
z1 = z.get_attributes()
zz1 = z.write(data2, True)
zzz1 = SampleTable.write_key(data2, "Account", True)
zzz2 = SampleTable.write_key(data2, "Account", False)
zzz3 = SampleTable.write_globalindex(data2, "Account", "gsi1", False)
zz2 = z.read(zzz1)

# db.insert(data, SampleTable)

# db.create_table(SampleTable)

data_account = {
    "address": {
        "addr1": "123 Main Street",
        "city": "somewhere"
    },
    "color": "red",
    "joe": "skip",
    "modified": 1,
}
dr1 = db.insert(data_account, SampleTable, "Account")
accountid = dr1.data.get('accountid')
dr2 = db.isexist(dr1.data, SampleTable, "Account")
