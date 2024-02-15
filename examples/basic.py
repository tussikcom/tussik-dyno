import logging

from tussik.dyno import DynoAttrMap, DynoAttrString, DynoTable, DynoKey, DynoGlobalIndex, DynoSchema, DynoKeyFormat, \
    DynoAttrUuid, DynoAttrBool, DynoAttrDateTime, DynoAttrInt, DynoAttrFlag

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
    Key = DynoKey()
    Gsi1 = DynoGlobalIndex("gsi1_pk", "gsi1_sk", read_unit=1, write_unit=1)
    Gsi2 = DynoGlobalIndex("gsi2_pk", "gsi2_sk")

    class Account(DynoSchema):
        Key = DynoKeyFormat(pk="account#", sk="accountid#{accountid}", req={"accountid"})
        Gsi1 = DynoKeyFormat(pk="account#", sk="alias#{alias}", req={"alias"})
        accountid = DynoAttrUuid()
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
