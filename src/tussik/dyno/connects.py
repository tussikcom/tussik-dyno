import logging
from typing import Any, Type, Self

import boto3

from .table import DynoTypeTable

logger = logging.getLogger()


class DynoResponse:
    __slots__ = ["ok", "code", "errors", "data", "consumed", "attributes", "count", "scanned"]

    def __init__(self):
        self.ok = True
        self.code = 200
        self.count = 0
        self.scanned = 0
        self.errors = list[str]()
        self.consumed = 0.0
        self.data: Any = None
        self.attributes = dict()

    def set_error(self, code: int, message: str) -> Self:
        self.ok = False
        self.code = code
        self.errors.append(message)
        return self

    def set_response(self, r: dict, table: None | Type[DynoTypeTable] = None) -> Self:
        self.code = int(r.get('ResponseMetadata', {}).get('HTTPStatusCode', 500))
        self.ok = self.code == 200
        self.count = r.get("Count") or 0
        self.scanned = r.get("ScannedCount") or 0

        if "ConsumedCapacity" in r:
            self.consumed = r.get("ConsumedCapacity", dict()).get("CapacityUnits") or 0.0

        if "Items" in r:
            items = r.get("Items") or []
            if table is None:
                self.data = items
            else:
                self.data = list[dict]()
                for item in items:
                    row = table.dyno2data(item)
                    self.data.append(row)
            return self

        if "Attributes" in r:
            items = r.get("Attributes") or []
            if table is None:
                self.data = items
            else:
                self.data = list[dict]()
                for item in items:
                    row = table.dyno2data(item)
                    self.data.append(row)
            return self

        return self

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

    def insert(self, data: dict, table: Type[DynoTypeTable], schema: str) -> DynoResponse:
        dr = DynoResponse()
        try:
            item = table.write_key(data, schema=schema)
            dr.data = table.dyno2data(item)

            db = self.client()
            conditional = f"attribute_not_exists({table.Key.pk}) AND attribute_not_exists({table.Key.sk})"
            r = db.put_item(
                TableName=table.TableName,
                Item=item,
                ConditionExpression=conditional,
                ReturnConsumedCapacity="INDEXES",
                ReturnItemCollectionMetrics="SIZE",
            )
            dr.set_response(r, table)
            if dr.code != 200:
                logger.error(f"DynoConnect.insert({table.TableName}[{schema}])")
                dr.set_error(dr.code, f"Failed to insert {table.TableName}[{schema}")
        except Exception as e:
            logger.exception(f"DynoConnect.insert({table.TableName}[{schema}])")
            dr.set_error(500, f"{e!r}")
        return dr

    def delete_item(self, data: dict, table: Type[DynoTypeTable], schema: str) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                return dr.set_error(400, f"Schema {schema} not found")

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                return dr.set_error(400, f"Key format for {schema} not found")

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                return dr.set_error(400, "One or more key values are not available")

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
            dr.set_response(r, table)
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

    def isexist(self, data: dict, table: Type[DynoTypeTable], schema: str, globalindex: None | str = None) -> bool:
        dr = self.fetch(data, table, schema, globalindex)
        return dr.ok

    def fetch(self, data: dict, table: Type[DynoTypeTable],
              schema: str, globalindex: None | str = None) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                return dr.set_error(400, f"Schema {schema} not found")

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                return dr.set_error(400, f"Key format for {schema} not found")

            if isinstance(globalindex, str):
                key = table.GlobalIndexes.get(globalindex)
                if key is None:
                    return dr.set_error(400, f"GlobalIndex {globalindex} not found")
                fmt = tbl.GlobalIndexes.get(globalindex)
                if fmt is None:
                    return dr.set_error(400, f"Key format for {globalindex} not found")

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                return dr.set_error(400, "One or more key values are not available")

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
            dr.set_response(r, table)
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

    def get_item(self, data: dict, table: Type[DynoTypeTable], schema: str) -> DynoResponse:
        dr = DynoResponse()

        try:
            tbl = table.get_schema(schema)
            if tbl is None:
                return dr.set_error(400, f"Schema {schema} not found")

            key = table.Key
            fmt = tbl.Key
            if fmt is None:
                return dr.set_error(400, f"Key format for {schema} not found")

            #
            # generate key values
            #
            pk = fmt.format_pk(data)
            sk = fmt.format_sk(data)
            if pk is None or sk is None:
                return dr.set_error(400, "One or more key values are not available")

            params = {
                "TableName": table.TableName,
                "Key": {
                    "#n1": {key.pk_type.value: pk},
                    "#n2": {key.sk_type.value: sk}
                },
                "ConsistentRead": False,
                "ReturnConsumedCapacity": "INDEXES",
                "ExpressionAttributeNames": {
                    "#n1": key.pk,
                    "#n2": key.sk
                }
            }

            db = self.client()
            r = db.query(**params)
            dr.set_response(r, table)
            if dr.data is None:
                dr.data = []
            elif len(dr.data) >= 1:
                dr.data = dr.data[0]  # only report one record or no record always
        except Exception as e:
            if type(e).__name__ == "ConditionalCheckFailedException":
                dr.set_error(404, f"Not Found")
            else:
                logger.exception(f"DynoConnect.get_item({table.TableName}) {e!r}")
                dr.set_error(500, f"{e!r}")
        return dr

    def update(self, data: dict, table: Type[DynoTypeTable], schema: str) -> DynoResponse:
        dr = DynoResponse()
        # Attributes
        return dr

    def query(self, data: dict, table: Type[DynoTypeTable], indexname: None | str = None) -> DynoResponse:
        dr = DynoResponse()
        return dr

    def delete_table(self, table: Type[DynoTypeTable]) -> DynoResponse:
        dr = DynoResponse()
        try:
            db = self.client()
            r = db.delete_table(TableName=table.TableName)

            dr.set_response(r, table)
            if dr.code != 200:
                logger.error(f"DynoConnect.delete_table({table.TableName})")
                dr.errors.append(f"Failed to delete table {table.TableName}")
        except Exception as e:
            logger.exception(f"DynoConnect.delete_table({table.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr

    def create_table(self, table: Type[DynoTypeTable]) -> DynoResponse:
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
            dr.set_response(r, table)
            if dr.code != 200:
                logger.exception(f"DynoConnect.create_table({table.TableName})")
                dr.errors.append(f"Failed to create table {table.TableName}")
        except Exception as e:
            logger.exception(f"DynoConnect.create_table({table.TableName}) {e!r}")
            dr.set_error(500, f"{e!r}")
        return dr
