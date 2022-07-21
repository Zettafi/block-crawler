from collections import namedtuple

Model = namedtuple("Model", ["table_name", "schema"])

Contracts = Model(
    "Contracts",
    {
        "TableName": "Contracts",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "address", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
            {"AttributeName": "address", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)


TokenTransfers = Model(
    "TokenTransfers",
    {
        "TableName": "TokenTransfers",
        "KeySchema": [
            {"AttributeName": "blockchain", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "transaction_hash", "KeyType": "RANGE"},  # Sort key
        ],
        "AttributeDefinitions": [
            {"AttributeName": "blockchain", "AttributeType": "S"},
            {"AttributeName": "transaction_hash", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    },
)
