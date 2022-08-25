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
        "TableName": "Collections",
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
        "TableName": "Tokens",
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
        "TableName": "TokenTransfers",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "transaction_log_index_hash", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
            {"AttributeName": "transaction_log_index_hash", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)


LastBlock = Model(
    {
        "TableName": "LastBlock",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)
