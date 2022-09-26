class Model:
    def __init__(self, schema: dict) -> None:
        self.__schema = schema

    @property
    def table_name(self):
        return self.__schema["TableName"]

    @property
    def schema(self):
        return self.__schema.copy()


Collections = Model(
    {
        "TableName": "collection",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "collection_id", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
            {"AttributeName": "collection_id", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)

Tokens = Model(
    {
        "TableName": "token",
        "KeySchema": [
            {"AttributeName": "blockchain_collection_id", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "token_id", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain_collection_id", "AttributeType": "S"},
            {"AttributeName": "token_id", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)

TokenTransfers = Model(
    {
        "TableName": "tokentransfers",
        "KeySchema": [
            {"AttributeName": "blockchain_collection_id", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "transaction_log_index_hash", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain_collection_id", "AttributeType": "S"},
            {"AttributeName": "transaction_log_index_hash", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)

Owners = Model(
    {
        "TableName": "owner",
        "KeySchema": [
            {"AttributeName": "blockchain_account", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "collection_id_token_id", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain_account", "AttributeType": "S"},
            {"AttributeName": "collection_id_token_id", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    }
)

BlockCrawlerConfig = Model(
    {
        "TableName": "crawler_config",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)
