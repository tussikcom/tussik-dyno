import logging
from typing import Any, Type, Dict

import boto3

from .attributes import DynoAttrDateTime
from .query import DynoQuery
from .table import DynoTable, DynoTableLink
from .update import DynoUpdate

logger = logging.getLogger()


class DynoResponse:
    __slots__ = ["ok", "code", "errors", "data", "consumed", "attributes", "count", "scanned", "LastEvaluatedKey"]

    def __init__(self):
        self.ok = True
        self.code = 200
        self.count = 0
        self.scanned = 0
        self.errors = list[str]()
        self.consumed = 0.0
        self.data: Any = None
        self.attributes = dict()
        self.LastEvaluatedKey: None | Dict[str, Dict[str, Any]] = None

    def set_error(self, code: int, message: str) -> None:
        self.ok = False
        self.code = code
        self.errors.append(message)

    def set_response(self, r: dict, link: None | DynoTableLink = None) -> None:
        self.code = int(r.get('ResponseMetadata', {}).get('HTTPStatusCode', 500))
        self.ok = self.code == 200
        self.count = r.get("Count") or 0
        self.scanned = r.get("ScannedCount") or 0
        self.LastEvaluatedKey = r.get("LastEvaluatedKey")

        if "ConsumedCapacity" in r:
            self.consumed = r.get("ConsumedCapacity", dict()).get("CapacityUnits") or 0.0

        if "Items" in r:
            items = r.get("Items") or []
            if link is None:
                self.data = items
            else:
                self.data = list[dict]()
                for item in items:
                    row = link.table.read(item, link.schema, link.globalindex)
                    self.data.append(row)
            return

        if "Item" in r:
            if link is None:
                self.data = r['Item']
            else:
                self.data = link.table.read(r['Item'], link.schema, link.globalindex)
            return

        if "Attributes" in r:
            items = r.get("Attributes") or []
            if link is None:
                self.data = items
            else:
                self.data = list[dict]()
                for item in items:
                    row = link.table.read(item, link.schema, link.globalindex)
                    self.data.append(row)
            return
        return

    def __repr__(self):
        if self.ok and self.data is None:
            return "DR ok"
        if self.ok and isinstance(self.data, (list, dict)):
            return f"DR ok with {len(self.data)} items"
        if self.ok:
            return f"DR ok with results"
        return f"DR {self.code} : {'; '.join(self.errors)}"


class DynoConnect:
    __slots__ = ["_host", "_access", "_secret", "_region"]
    __g_host: None | str = None
    __g_access: None | str = None
    __g_secret: None | str = None
    __g_region: None | str = None

    def __init__(self,
                 host: None | str = None,
                 access: None | str = None,
                 secret: None | str = None,
                 region: None | str = None
                 ):
        self._host = host or DynoConnect.__g_host
        self._access = access or DynoConnect.__g_access
        self._secret = secret or DynoConnect.__g_secret
        self._region = region or DynoConnect.__g_region

    def __repr__(self):
        if self._host is not None:
            return f"DynoConnect: {self._host}"
        else:
            return f"DynoConnect: aws"

    @classmethod
    def set_host(cls, host: None | str = None) -> None:
        cls.__g_host = host or "http://localhost:8000"
        cls.__g_access = None
        cls.__g_secret = None
        cls.__g_region = None

    @classmethod
    def set_iam(cls) -> None:
        cls.__g_host = None
        cls.__g_access = None
        cls.__g_secret = None
        cls.__g_region = None

    @classmethod
    def set_aws(cls, access: str, secret: str, region: None | str = None) -> None:
        cls.__g_host = None
        cls.__g_access = access
        cls.__g_secret = secret
        cls.__g_region = region

    def client(self) -> 'botocore.client.DynamoDB':
        params = dict()
        if self._host is not None:
            params["endpoint_url"] = self._host
        if self._access is not None:
            params["aws_access_key_id"] = self._access
        if self._secret is not None:
            params["aws_secret_access_key"] = self._secret
        if self._region is not None:
            params["region_name"] = self._region
        ddb = boto3.client('dynamodb', **params)
        return ddb

    def resource(self) -> 'dynamodb.ServiceResource':
        params = dict()
        if self._host is not None:
            params["endpoint_url"] = self._host
        if self._access is not None:
            params["aws_access_key_id"] = self._access
        if self._secret is not None:
            params["aws_secret_access_key"] = self._secret
        if self._region is not None:
            params["region_name"] = self._region
        ddb = boto3.resource('dynamodb', **params)
        return ddb

    def insert(self, data: dict, table: Type[DynoTable], schema: str) -> DynoResponse:
        dr = DynoResponse()
        try:
            item = table.write_key(data, schema=schema)
            dr.data = table.read(item)

            cond_list = list[str]()
            cond_list.append(f"attribute_not_exists({table.Key.pk})")
            cond_list.append(f"attribute_not_exists({table.Key.sk})")

            # enforce gsi uniqueness when enabled
            for name, gsi in table.GlobalIndexes.items():
                if gsi.unique and gsi.pk in item:
                    cond_list.append(f"attribute_not_exists({gsi.pk})")
                if gsi.unique and gsi.sk in item:
                    cond_list.append(f"attribute_not_exists({gsi.sk})")

            db = self.client()
            r = db.put_item(
                TableName=table.TableName,
                Item=item,
                ConditionExpression=" AND ".join(cond_list),
                ReturnConsumedCapacity="INDEXES",
                ReturnItemCollectionMetrics="SIZE",
            )
            link = table.get_link(schema)
            dr.set_response(r, link)
            if dr.code != 200:
                logger.error(f"DynoConnect.insert({table.TableName}[{schema}])")
                dr.set_error(dr.code, f"Failed to insert {table.TableName}[{schema}")
        except Exception as e:
            logger.exception(f"DynoConnect.insert({table.TableName}[{schema}])")
            dr.set_error(500, f"{e!r}")
        return dr

    def delete_item(self, data: dict, table: Type[DynoTable], schema: str) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                dr.set_error(400, f"Schema {schema} not found")
                return dr

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                dr.set_error(400, f"Key format for {schema} not found")
                return dr

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                dr.set_error(400, "One or more key values are not available")
                return dr

            params = {
                "TableName": table.TableName,
                "ExpressionAttributeValues": {
                    ":v1": {key.pk_type.value: pk},
                    ":v2": {key.sk_type.value: sk}
                },
                "ExpressionAttributeNames": {
                    "#n1": key.pk,
                    "#n2": key.sk
                },
                "KeyConditionExpression": f"#n1 = :v1 AND #n2 = :v2",
                "ReturnValues": "ALL_OLD",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }

            db = self.client()
            r = db.delete_item(**params)
            link = table.get_link(schema)
            dr.set_response(r, link)
            if dr.data is None:
                dr.data = []
            dr.ok = len(data) > 0
            if len(dr.data) >= 1:
                dr.data = dr.data[0]  # only report one record or no record always
        except Exception as e:
            if type(e).__name__ == "ConditionalCheckFailedException":
                dr.set_error(404, f"Not Found")
            else:
                logger.exception(f"DynoConnect.isexist({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def isexist(self, data: dict, table: Type[DynoTable], schema: str, globalindex: None | str = None) -> bool:
        dr = self.fetch(data, table, schema, globalindex)
        return dr.ok

    def fetch(self, data: dict, table: Type[DynoTable],
              schema: str, globalindex: None | str = None) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                dr.set_error(400, f"Schema {schema} not found")
                return dr

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                dr.set_error(400, f"Key format for {schema} not found")
                return dr

            if isinstance(globalindex, str):
                key = table.GlobalIndexes.get(globalindex)
                if key is None:
                    dr.set_error(400, f"GlobalIndex {globalindex} not found")
                    return dr
                fmt = tbl.GlobalIndexes.get(globalindex)
                if fmt is None:
                    dr.set_error(400, f"Key format for {globalindex} not found")
                    return dr

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                dr.set_error(400, "One or more key values are not available")
                return dr

            params = {
                "TableName": table.TableName,
                "ExpressionAttributeValues": {
                    ":v1": {key.pk_type.value: pk},
                    ":v2": {key.sk_type.value: sk}
                },
                "ExpressionAttributeNames": {
                    "#n1": key.pk,
                    "#n2": key.sk
                },
                "KeyConditionExpression": f"#n1 = :v1 AND #n2 = :v2"
            }

            if isinstance(globalindex, str):
                params["IndexName"] = globalindex

            db = self.client()
            r = db.query(**params)
            link = table.get_link(schema, globalindex)
            dr.set_response(r, link)
            if dr.data is None:
                dr.data = []
            dr.ok = len(data) > 0
            if len(dr.data) >= 1:
                dr.data = dr.data[0]  # only report one record or no record always
        except Exception as e:
            if type(e).__name__ == "ConditionalCheckFailedException":
                dr.set_error(404, f"Not Found")
            else:
                logger.exception(f"DynoConnect.fetch({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def get_item(self, data: dict, table: Type[DynoTable], schema: str) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                dr.set_error(400, f"Schema {schema} not found")
                return dr

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                dr.set_error(400, f"Key format for {schema} not found")
                return dr

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                dr.set_error(400, "One or more key values are not available")
                return dr

            params = {
                "TableName": table.TableName,
                "Key": {
                    key.pk: {key.pk_type.value: pk},
                    key.sk: {key.sk_type.value: sk}
                },
                "ConsistentRead": False,
                "ReturnConsumedCapacity": "INDEXES",
            }

            db = self.client()
            r = db.get_item(**params)
            link = table.get_link(schema)
            dr.set_response(r, link)
        except Exception as e:
            if type(e).__name__ == "ConditionalCheckFailedException":
                dr.set_error(404, f"Not Found")
            else:
                logger.exception(f"DynoConnect.get_item({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def update(self, update: DynoUpdate) -> DynoResponse:
        dr = DynoResponse()

        try:
            params = update.write()
            if params is None:
                dr.set_error(500, "Update incomplete")
                return dr

            db = self.client()
            r = db.query(**params)
            dr.set_response(r, update.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.update({update.TableName})")
                dr.errors.append(f"Failed to update {update.TableName}")
        except Exception as e:
            logger.exception(f"DynoConnect.update({update.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr

    def query(self, query: DynoQuery) -> DynoResponse:
        dr = DynoResponse()
        try:
            params = query.write()
            if params is None:
                dr.set_error(500, "Query incomplete")
                return dr

            db = self.client()
            r = db.query(**params)
            dr.set_response(r, query.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.query({query.TableName})")
                dr.errors.append(f"Failed to query {query.TableName}")

            if "LastEvaluatedKey" in r:
                query.StartKey(dr.LastEvaluatedKey)

        except Exception as e:
            logger.exception(f"DynoConnect.query({query.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr

    def table_delete(self, table: Type[DynoTable]) -> DynoResponse:
        dr = DynoResponse()
        try:
            db = self.client()
            r = db.delete_table(TableName=table.TableName)

            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.table_delete({table.TableName})")
                dr.errors.append(f"Failed to delete table {table.TableName}")
        except Exception as e:
            if type(e).__name__ == "ClientError" and "protected against deletion" in e.args[0]:
                logger.exception(f"DynoConnect.table_delete({table.TableName}) {e!r}")
                dr.set_error(400, f"Table is delete protected")
            else:
                logger.exception(f"DynoConnect.table_delete({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def table_protect(self, table: Type[DynoTable], protect: bool) -> DynoResponse:
        dr = DynoResponse()
        try:
            db = self.client()
            r = db.update_table(TableName=table.TableName, DeletionProtectionEnabled=protect)
            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.table_protect({table.TableName})")
                dr.errors.append(f"Failed to update table {table.TableName}")
        except Exception as e:
            if type(e).__name__ == "ClientError" and "protected against deletion" in e.args[0]:
                logger.exception(f"DynoConnect.table_protect({table.TableName}) {e!r}")
                dr.set_error(400, f"Table is delete protected")
            else:
                logger.exception(f"DynoConnect.table_protect({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def table_create(self, table: Type[DynoTable]) -> DynoResponse:
        dr = DynoResponse()
        try:
            KeySchema = table.table_create_keys()
            Attributes = table.table_create_attributes()
            GlobalIndexes = table.table_create_globalindexes()

            db = self.client()
            r = db.create_table(
                TableName=table.TableName,
                TableClass="STANDARD" if table.TableClassStandard else "STANDARD_INFREQUENT_ACCESS",
                DeletionProtectionEnabled=table.DeletionProtection,
                AttributeDefinitions=Attributes,
                KeySchema=KeySchema,
                BillingMode="PAY_PER_REQUEST" if table.PayPerRequest else "PROVISIONED",
                ProvisionedThroughput={
                    "ReadCapacityUnits": table.ReadCapacityUnits,
                    "WriteCapacityUnits": table.WriteCapacityUnits
                },
                GlobalSecondaryIndexes=GlobalIndexes
            )
            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.exception(f"DynoConnect.table_create({table.TableName})")
                dr.errors.append(f"Failed to create table {table.TableName}")
        except Exception as e:
            if type(e).__name__ == "ResourceInUseException":
                logger.exception(f"DynoConnect.table_create({table.TableName}) {e!r}")
                dr.set_error(400, "Table already exists")
            else:
                logger.exception(f"DynoConnect.table_create({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def _insert_auto_increment(self, data: Dict[str, Any], table: Type[DynoTable], schema: str):
        dr = DynoResponse()
        db = self.client()
        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                msg = f"DynoConnect._insert_auto_increment({table.TableName}.{schema})"
                dr.set_error(404, msg)
                return dr

            pk = tbl.Key.format_pk(data)
            sk = tbl.Key.format_pk(data)
            item = {
                table.Key.pk: {table.Key.pk_type.value: pk},
                table.Key.sk: {table.Key.sk_type.value: sk}
            }

            if isinstance(table.SchemaFieldName, str) and len(table.SchemaFieldName) > 0:
                if isinstance(tbl.SchemaFieldValue, str) and len(tbl.SchemaFieldValue) > 0:
                    item[table.SchemaFieldName] = {"S": str(tbl.SchemaFieldValue)}

            conditional = f"attribute_not_exists({table.Key.pk}) AND attribute_not_exists({table.Key.sk})"
            r = db.put_item(
                TableName=table.TableName,
                Item=item,
                ConditionExpression=conditional,
                ReturnConsumedCapacity="INDEXES",
                ReturnItemCollectionMetrics="SIZE",
            )
            link = table.get_link(schema)
            dr.set_response(r, link)
            if dr.code != 200:
                logger.error(f"DynoConnect._insert_auto_increment({table.TableName}[{schema}])")
                dr.set_error(dr.code, f"Failed to insert {table.TableName}[{schema}")
        except Exception as e:
            logger.exception(f"DynoConnect._insert_auto_increment({table.TableName}[{schema}])")
            dr.set_error(500, f"{e!r}")
        return dr

    def set_time_to_live(self, table: Type[DynoTable], expired_time_attribute: str, enable: bool) -> DynoResponse:
        dr = DynoResponse()

        confirmed = False
        for schema, tbl in table.get_schemas().items():
            for name, attrib in tbl.get_attributes():
                if name == expired_time_attribute and isinstance(attrib, DynoAttrDateTime):
                    confirmed = True
        if not confirmed:
            dr.set_error(404, f"The DateTime Attribute {expired_time_attribute} was not found")
            return dr

        db = self.client()
        try:
            r = db.update_time_to_live(
                TableName=table.TableName,
                TimeToLiveSpecification={
                    "Enabled": enable,
                    "AttributeName": expired_time_attribute
                }
            )
            dr.set_response(r)
        except Exception as e:
            logger.exception(f"DynoConnect.set_time_to_live({table.TableName}, {expired_time_attribute}, {enable})")
            dr.set_error(500, f"{e!r}")
        return dr

    def auto_increment(self, data: Dict[str, Any], table: Type[DynoTable], schema: str,
                       name: str, reset: bool = False) -> None | int:
        params = table.auto_increment(data, schema, name, reset)
        if params is None:
            return None

        db = self.client()
        result = None

        for retry in range(2):
            try:
                r = db.update_item(**params)
                nextid = r.get("Attributes", dict()).get(name, dict()).get("N")
                if nextid is None:
                    break
                result = int(nextid)
                break
            except Exception as e:
                if type(e).__name__ == "ConditionalCheckFailedException":
                    dr = self._insert_auto_increment(data, table, schema)
                    if not dr.ok:
                        break
                else:
                    logger.exception(f"DynoConnect.auto_increment({table.TableName}.{schema}.{name}) {e!r}")
                    break
        return result
