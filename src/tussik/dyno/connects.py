import logging
from typing import Any, Type, Dict

import boto3

from .attributes import DynoAttrDateTime
from .query import DynoQuery
from .reading import DynoReader
from .table import DynoTable, DynoTableLink, DynoSchema
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
            reader = DynoReader(r.get("Items"))
            self.data = reader.decode(link.table, link.schema)
            return

        if "Item" in r:
            reader = DynoReader(r['Item'])
            self.data = reader.decode(link.table, link.schema)
            return

        if "Attributes" in r:
            reader = DynoReader(r['Attributes'])
            self.data = reader.decode(link.table, link.schema)
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

    def insert(self, data: dict[str, Any], table: type[DynoTable], schema: type[DynoSchema]) -> DynoResponse:
        dr = DynoResponse()
        try:
            if not isinstance(data, dict):
                dr.set_error(500, f"DynoConnect.insert: invalid data parameter")
                return dr

            dr.data = table.write_value(data, schema, include_readonly=True)
            reader = DynoReader(dr.data)
            item = reader.encode(table, schema)

            cond_list = list[str]()
            cond_list.append(f"attribute_not_exists({table.Key.pk})")
            cond_list.append(f"attribute_not_exists({table.Key.sk})")

            # enforce gsi uniqueness when enabled
            for name, gsi in table.get_globalindexes().items():
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
                logger.error(f"DynoConnect.insert({table}[{schema}])")
                dr.set_error(dr.code, f"Failed to insert {table}[{schema}")
        except Exception as e:
            logger.exception(f"DynoConnect.insert({table}[{schema}])")
            dr.set_error(500, f"{e!r}")
        return dr

    def delete_item(self, data: dict, table: type[DynoTable], schema: type[DynoSchema]) -> DynoResponse:
        dr = DynoResponse()

        try:
            check = table.get_schema(schema.get_schema_name())
            if check is None:
                dr.set_error(400, f"Schema {schema} not found")
                return dr

            key = table.Key
            fmt = schema.Key
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

    def get_item(self, data: dict, table: type[DynoTable], schema: type[DynoSchema]) -> DynoResponse:
        dr = DynoResponse()

        try:
            check = table.get_schema(schema.get_schema_name())
            if check is None:
                dr.set_error(400, f"Schema {schema} not found")
                return dr

            key = table.Key
            fmt = schema.Key
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
                logger.exception(f"DynoConnect.get_item({table}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def update(self, update: DynoUpdate) -> DynoResponse:
        dr = DynoResponse()

        try:
            params = update.build()
            if params is None:
                dr.set_error(500, "Update incomplete")
                return dr

            db = self.client()
            r = db.update_item(**params)
            dr.set_response(r, update.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.update({update.TableName})")
                dr.errors.append(f"Failed to update {update.TableName}")
        except Exception as e:
            logger.exception(f"DynoConnect.update({update.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr

    def all(self, table: type[DynoTable], schema: type[DynoSchema], globalindex: None | str = None,
            limit: int = 100, startkey: None | dict = None) -> DynoResponse:
        query = DynoQuery(table, schema, globalindex)
        query.set_limit(limit)
        query.StartKey(startkey)
        dr = self.query(query)
        return dr

    def fetch(self, pk: str | bytes | int | float, sk: str | bytes | int | float,
              table: type[DynoTable], schema: type[DynoSchema], globalindex: None | str = None) -> None | dict:
        query = DynoQuery(table, schema, globalindex)
        query.set_limit(1)
        query.Key.pk = pk
        query.Key.op("=", sk)

        dr = self.query(query)
        if len(dr.data) > 0:
            dr.data = dr.data[0]
        return dr.data

    def query(self, query: DynoQuery) -> DynoResponse:
        dr = DynoResponse()
        try:
            params = query.build()
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
                query.set_startkey(dr.LastEvaluatedKey)

        except Exception as e:
            logger.exception(f"DynoConnect.query({query.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr

    def table_delete(self, table: type[DynoTable]) -> DynoResponse:
        dr = DynoResponse()
        try:
            db = self.client()
            r = db.delete_table(TableName=table.TableName)

            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.table_delete({table})")
                dr.errors.append(f"Failed to delete table {table}")
        except Exception as e:
            if type(e).__name__ == "ClientError" and "protected against deletion" in e.args[0]:
                logger.exception(f"DynoConnect.table_delete({table}) {e!r}")
                dr.set_error(400, f"Table is delete protected")
            else:
                logger.exception(f"DynoConnect.table_delete({table}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def table_protect(self, table: type[DynoTable], protect: bool) -> DynoResponse:
        dr = DynoResponse()
        try:
            db = self.client()
            r = db.update_table(TableName=table.TableName, DeletionProtectionEnabled=protect)
            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.error(f"DynoConnect.table_protect({table})")
                dr.errors.append(f"Failed to update table {table}")
        except Exception as e:
            if type(e).__name__ == "ClientError" and "protected against deletion" in e.args[0]:
                logger.exception(f"DynoConnect.table_protect({table}) {e!r}")
                dr.set_error(400, f"Table is delete protected")
            else:
                logger.exception(f"DynoConnect.table_protect({table}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def table_create(self, table: type[DynoTable]) -> DynoResponse:
        dr = DynoResponse()
        try:
            params = table.write_table_create()

            db = self.client()
            r = db.create_table(**params)

            dr.set_response(r, table.get_link())
            if dr.code != 200:
                logger.exception(f"DynoConnect.table_create({table})")
                dr.errors.append(f"Failed to create table {table}")
        except Exception as e:
            if type(e).__name__ == "ResourceInUseException":
                logger.exception(f"DynoConnect.table_create({table}) {e!r}")
                dr.set_error(400, "Table already exists")
            else:
                logger.exception(f"DynoConnect.table_create({table}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def _insert_auto_increment(self, data: Dict[str, Any], table: type[DynoTable], schema: type[DynoSchema]):
        dr = DynoResponse()
        db = self.client()
        try:
            check = table.get_schema(schema.get_schema_name())
            if check is None:
                msg = f"DynoConnect._insert_auto_increment({table}.{schema})"
                dr.set_error(404, msg)
                return dr

            pk = schema.Key.format_pk(data)
            sk = schema.Key.format_pk(data)
            item = {
                table.Key.pk: {table.Key.pk_type.value: pk},
                table.Key.sk: {table.Key.sk_type.value: sk}
            }

            if isinstance(table.SchemaFieldName, str) and len(table.SchemaFieldName) > 0:
                item[table.SchemaFieldName] = {"S": str(schema.get_schema_name())}

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
                logger.error(f"DynoConnect._insert_auto_increment({table}[{schema}])")
                dr.set_error(dr.code, f"Failed to insert {table}[{schema}")
        except Exception as e:
            logger.exception(f"DynoConnect._insert_auto_increment({table}[{schema}])")
            dr.set_error(500, f"{e!r}")
        return dr

    def set_time_to_live(self, table: type[DynoTable], expired_time_attribute: str, enable: bool) -> DynoResponse:
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

    def auto_increment(self, data: Dict[str, Any], table: type[DynoTable], schema: type[DynoSchema],
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
                    logger.exception(f"DynoConnect.auto_increment({table}.{schema}.{name}) {e!r}")
                    break
        return result
