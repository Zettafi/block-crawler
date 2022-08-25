import asyncio
from datetime import datetime
from math import floor
from statistics import median, mean

import click

from chainconductor.blockcrawler.processors import (
    BlockBatchProcessor,
    TransactionBatchProcessor,
    ContractBatchProcessor,
    CollectionPersistenceBatchProcessor,
    TokenTransferPersistenceBatchProcessor,
    TokenMetadataUriBatchProcessor,
    TokenMetadataRetrievalBatchProcessor,
    TokenPersistenceBatchProcessor,
)
from chainconductor.blockcrawler.stats import StatsService


class StatsWriter:
    def __init__(self, stats_service: StatsService) -> None:
        self.__stats_service = stats_service

    async def status_writer(
        self,
        start_time: datetime,
        run_forever=True,
        show_header=True,
    ):
        if show_header:
            click.echo(
                "Total Time  | Blocks      | Transactions | Contracts   | "
                "Collection Writes | Token Transfer Writes | Token Metadata | "
                "Metadata Gather | Token Writes"
            )
            click.echo("", nl=False)

        while True:
            await asyncio.sleep(1)
            end = datetime.utcnow()
            total_time = end - start_time
            seconds = total_time.seconds % 60 + total_time.microseconds / 1_000_000
            total_minutes = total_time.seconds // 60
            hours = total_minutes // 60
            minutes = total_minutes % 60
            click.echo(
                "\r{:02d}:{:02d}:{:05.2f}".format(hours, minutes, seconds)
                + " | "
                + "{:<11,}".format(
                    self.__stats_service.get_count(BlockBatchProcessor.PROCESSED_STAT)
                )
                + " | "
                + "{:<12,}".format(
                    self.__stats_service.get_count(TransactionBatchProcessor.PROCESSED_STAT)
                )
                + " | "
                + "{:<11,}".format(
                    self.__stats_service.get_count(ContractBatchProcessor.PROCESSED_STAT)
                )
                + " | "
                + "{:<17,}".format(
                    self.__stats_service.get_count(
                        CollectionPersistenceBatchProcessor.PROCESSED_STAT
                    )
                )
                + " | "
                + "{:<21,}".format(
                    self.__stats_service.get_count(
                        TokenTransferPersistenceBatchProcessor.PROCESSED_STAT
                    )
                )
                + " | "
                + "{:<14,}".format(
                    self.__stats_service.get_count(TokenMetadataUriBatchProcessor.PROCESSED_STAT)
                )
                + " | "
                + "{:<15,}".format(
                    self.__stats_service.get_count(
                        TokenMetadataRetrievalBatchProcessor.PROCESSED_STAT
                    )
                )
                + " | "
                + "{:<12,}".format(
                    self.__stats_service.get_count(TokenPersistenceBatchProcessor.PROCESSED_STAT)
                ),
                nl=False,
            )
            if not run_forever:
                break
        click.echo()

    @staticmethod
    def __get_time_from_secs(nanoseconds):
        seconds = nanoseconds / 1_000_000_000
        minutes = floor(seconds / 60)
        return "{:02d}:{:05.2f}".format(minutes, seconds % 60)

    async def print_statistics(self):
        click.echo("Blocks:")
        blocks_processed = self.__stats_service.get_count(BlockBatchProcessor.PROCESSED_STAT)
        click.echo("  Total Processed: {:,}".format(blocks_processed))
        block_batches = self.__stats_service.get_timings(BlockBatchProcessor.RPC_TIMER_GET_BLOCKS)
        block_batches_count = len(block_batches)
        click.echo("  Get Block RPC Calls: {:,}".format(block_batches_count))
        block_batches_average_size = (
            blocks_processed / block_batches_count if block_batches_count else 0.0
        )
        click.echo("  Get Block RPC Avg Batch Size: {:0.2f}".format(block_batches_average_size))
        click.echo(
            "  Get Block RPC Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(block_batches))
            )
        )
        click.echo(
            "  Get Block RPC Median Processing Time: {}".format(
                self.__get_time_from_secs(median(block_batches)) if block_batches else "n/a"
            )
        )
        click.echo(
            "  Get Block RPC Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(block_batches))
            )
            if block_batches
            else "n/a"
        )

        click.echo()
        click.echo("Transactions Receipts:")
        transactions_processed = self.__stats_service.get_count(
            TransactionBatchProcessor.PROCESSED_STAT
        )
        click.echo("  Total Processed: {:,}".format(transactions_processed))
        transaction_batches = self.__stats_service.get_timings(
            TransactionBatchProcessor.RPC_TIMER_GET_TRANSACTION_RECEIPTS
        )
        transaction_batches_count = len(transaction_batches)
        transaction_batches_average_size = (
            transactions_processed / transaction_batches_count
            if transaction_batches_count > 0
            else 0.0
        )
        click.echo("  Get Transaction Receipt RPC Calls: {:,}".format(len(transaction_batches)))
        click.echo(
            "  Get Transaction Receipt RPC Avg Batch Size: {:0.2f}".format(
                transaction_batches_average_size
            )
        )
        click.echo(
            "  Get Transaction Receipt RPC Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(transaction_batches))
                if transaction_batches
                else "n/a"
            )
        )
        click.echo(
            "  Get Transaction Receipt RPC Median Processing Time: {}".format(
                self.__get_time_from_secs(median(transaction_batches))
                if transaction_batches
                else "n/a"
            )
        )
        click.echo(
            "  Get Transaction Receipt RPC Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(transaction_batches))
                if transaction_batches
                else "n/a"
            )
        )

        click.echo()
        click.echo("Smart Contracts:")
        contracts_processed = self.__stats_service.get_count(ContractBatchProcessor.PROCESSED_STAT)
        click.echo("  Total Processed: {:,}".format(contracts_processed))
        click.echo("  Contract Info:")
        get_contract_info_batches = self.__stats_service.get_timings(
            ContractBatchProcessor.RPC_TIMER_CALL_CONTRACT_INFO
        )
        click.echo(
            "    Get Call Get Contract Info RPC Calls: {:,}".format(len(get_contract_info_batches))
        )
        click.echo(
            "    Get Call Get Contract Info RPC Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(get_contract_info_batches))
                if get_contract_info_batches
                else "n/a"
            )
        )
        click.echo(
            "    Get Call Get Contract Info RPC Median Processing Time: {}".format(
                self.__get_time_from_secs(median(get_contract_info_batches))
                if get_contract_info_batches
                else "n/a"
            )
        )
        click.echo(
            "    Get Call Get Contract Info RPC Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(get_contract_info_batches))
                if get_contract_info_batches
                else "n/a"
            )
        )

        click.echo()
        click.echo("Stored Collections:")
        collections_stored = self.__stats_service.get_count(
            CollectionPersistenceBatchProcessor.PROCESSED_STAT
        )
        click.echo("  Total Collections Stored: {:,}".format(collections_stored))
        dynamodb_collections_write_batches = self.__stats_service.get_timings(
            CollectionPersistenceBatchProcessor.DYNAMODB_TIMER_WRITE_CONTRACTS
        )
        dynamodb_collections_write_batches_count = len(dynamodb_collections_write_batches)
        click.echo(
            "  DynamoDB Collections Batch Put Calls: {:,}".format(
                len(dynamodb_collections_write_batches)
            )
        )
        dynamodb_write_batches_average_size = (
            collections_stored / dynamodb_collections_write_batches_count
            if dynamodb_collections_write_batches_count
            else 0.0
        )
        click.echo(
            "  DynamoDB Collections Batch Put Avg Batch Size: {:0.2f}".format(
                dynamodb_write_batches_average_size
            )
        )
        click.echo(
            "  DynamoDB Collections Batch Put Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(dynamodb_collections_write_batches))
                if len(dynamodb_collections_write_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB Collections Batch Put Median Processing Time: {}".format(
                self.__get_time_from_secs(median(dynamodb_collections_write_batches))
                if len(dynamodb_collections_write_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB Collections Batch Put Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(dynamodb_collections_write_batches))
                if len(dynamodb_collections_write_batches)
                else "n/a"
            )
        )

        click.echo()
        click.echo("Stored Token Transfers:")
        token_transfers_stored = self.__stats_service.get_count(
            TokenTransferPersistenceBatchProcessor.PROCESSED_STAT
        )
        click.echo("  Total Token Transfers Stored: {:,}".format(token_transfers_stored))
        token_transfer_decode_errors = self.__stats_service.get_count(
            TokenTransferPersistenceBatchProcessor.TOPICS_DECODE_ERRORS
        )
        click.echo(
            "  Total Token Transfer Decode Errors: {:,}".format(token_transfer_decode_errors)
        )
        token_transfer_decode_error_rate = (
            token_transfers_stored / token_transfer_decode_errors
            if token_transfer_decode_errors
            else 0
        )
        click.echo(
            "  Total Token Transfer Error Rate: {:0.2f}".format(token_transfer_decode_error_rate)
        )
        dynamodb_write_token_transfer_batches = self.__stats_service.get_timings(
            TokenTransferPersistenceBatchProcessor.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS
        )
        dynamodb_write_token_transfer_batches_count = len(dynamodb_write_token_transfer_batches)
        click.echo(
            "  Get DynamoDB TokenTransfers Batch Put Calls: {:,}".format(
                len(dynamodb_write_token_transfer_batches)
            )
        )
        dynamodb_write_batches_average_size = (
            token_transfers_stored / dynamodb_write_token_transfer_batches_count
            if dynamodb_write_token_transfer_batches_count
            else 0.0
        )
        click.echo(
            "  DynamoDB TokenTransfers Batch Put Avg Batch Size: {:0.2f}".format(
                dynamodb_write_batches_average_size
            )
        )
        click.echo(
            "  DynamoDB TokenTransfers Batch Put Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(dynamodb_write_token_transfer_batches))
                if len(dynamodb_write_token_transfer_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB TokenTransfers Batch Put Median Processing Time: {}".format(
                self.__get_time_from_secs(median(dynamodb_write_token_transfer_batches))
                if len(dynamodb_write_token_transfer_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB TokenTransfers Batch Put Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(dynamodb_write_token_transfer_batches))
                if len(dynamodb_write_token_transfer_batches)
                else "n/a"
            )
        )

        click.echo()
        click.echo("Token Metadata:")
        token_metadata_processed = self.__stats_service.get_count(
            TokenMetadataUriBatchProcessor.PROCESSED_STAT
        )
        click.echo("  Total Tokens Metadata Processed: {:,}".format(token_metadata_processed))
        rpc_get_token_metadata_uri_batches = self.__stats_service.get_timings(
            TokenMetadataUriBatchProcessor.RPC_GET_TOKEN_METADATA_URI
        )
        rpc_get_token_metadata_uri_batches_count = len(rpc_get_token_metadata_uri_batches)
        click.echo(
            "  RPC Calls to Get Metadata URI: {:,}".format(rpc_get_token_metadata_uri_batches_count)
        )
        dynamodb_write_tokens_batches_average_size = (
            token_metadata_processed / rpc_get_token_metadata_uri_batches_count
            if rpc_get_token_metadata_uri_batches_count
            else 0.0
        )
        click.echo(
            "  RPC Calls to Get Metadata URI Avg Batch Size: {:0.2f}".format(
                dynamodb_write_tokens_batches_average_size
            )
        )
        click.echo(
            "  RPC Calls to Get Metadata URI Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(rpc_get_token_metadata_uri_batches))
                if len(rpc_get_token_metadata_uri_batches)
                else "n/a"
            )
        )
        click.echo(
            "  RPC Calls to Get Metadata URI Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(rpc_get_token_metadata_uri_batches))
                if len(rpc_get_token_metadata_uri_batches)
                else "n/a"
            )
        )
        click.echo(
            "  RPC Calls to Get Metadata URI Median Processing Time: {}".format(
                self.__get_time_from_secs(median(rpc_get_token_metadata_uri_batches))
                if len(rpc_get_token_metadata_uri_batches)
                else "n/a"
            )
        )

        click.echo()
        click.echo("Token Metadata Gather:")
        token_metadata_gather_processed = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.PROCESSED_STAT
        )
        click.echo(
            "  Total Tokens Metadata Gather Processed: {:,}".format(token_metadata_gather_processed)
        )
        metadata_retrieval_http_timers = self.__stats_service.get_timings(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_HTTP_TIMER
        )
        metadata_retrieval_http_timers_count = len(metadata_retrieval_http_timers)
        metadata_retrieval_http_timeouts = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_HTTP_TIMEOUT_STAT
        )
        metadata_retrieval_http_timeout_rate = (
            metadata_retrieval_http_timeouts / metadata_retrieval_http_timers_count
            if metadata_retrieval_http_timers_count
            else 0
        )
        metadata_retrieval_http_errors = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_HTTP_ERROR_STAT
        )
        metadata_retrieval_http_error_rate = (
            metadata_retrieval_http_errors / metadata_retrieval_http_timers_count
            if metadata_retrieval_http_timers_count
            else 0
        )
        click.echo("  HTTP Gather Calls:")
        click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_http_timers)))
        click.echo("    Total Timeouts: {:,}".format(metadata_retrieval_http_timeouts))
        click.echo("    Timeout Rate: {:0.2f}".format(metadata_retrieval_http_timeout_rate))
        click.echo("    Total Errors: {:,}".format(metadata_retrieval_http_errors))
        click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_http_error_rate))
        click.echo(
            "    Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(metadata_retrieval_http_timers))
                if len(metadata_retrieval_http_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Median Batch Processing Time: {}".format(
                self.__get_time_from_secs(median(metadata_retrieval_http_timers))
                if len(metadata_retrieval_http_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Mean Batch Processing Time: {}".format(
                self.__get_time_from_secs(mean(metadata_retrieval_http_timers))
                if len(metadata_retrieval_http_timers)
                else "n/a"
            )
        )
        metadata_retrieval_ipfs_timers = self.__stats_service.get_timings(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_IPFS_TIMER
        )
        metadata_retrieval_ipfs_timers_count = len(metadata_retrieval_ipfs_timers)
        metadata_retrieval_ipfs_timeouts = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_IPFS_TIMEOUT_STAT
        )
        metadata_retrieval_ipfs_timeout_rate = (
            metadata_retrieval_ipfs_timeouts / metadata_retrieval_ipfs_timers_count
            if metadata_retrieval_ipfs_timers_count
            else 0
        )
        metadata_retrieval_ipfs_errors = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_IPFS_ERROR_STAT
        )
        metadata_retrieval_ipfs_error_rate = (
            metadata_retrieval_ipfs_errors / metadata_retrieval_ipfs_timers_count
            if metadata_retrieval_ipfs_timers_count
            else 0
        )
        click.echo("  IPFS Gather Calls:")
        click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_ipfs_timers)))
        click.echo("    Total Timeouts: {:,}".format(metadata_retrieval_ipfs_timeouts))
        click.echo("    Timeout Rate: {:0.2f}".format(metadata_retrieval_ipfs_timeout_rate))
        click.echo("    Total Errors: {:,}".format(metadata_retrieval_ipfs_errors))
        click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_ipfs_error_rate))
        click.echo(
            "    Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(metadata_retrieval_ipfs_timers))
                if len(metadata_retrieval_ipfs_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Median Batch Processing Time: {}".format(
                self.__get_time_from_secs(median(metadata_retrieval_ipfs_timers))
                if len(metadata_retrieval_ipfs_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Mean Batch Processing Time: {}".format(
                self.__get_time_from_secs(mean(metadata_retrieval_ipfs_timers))
                if len(metadata_retrieval_ipfs_timers)
                else "n/a"
            )
        )
        metadata_retrieval_arweave_timers = self.__stats_service.get_timings(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_ARWEAVE_TIMER
        )
        metadata_retrieval_arweave_timers_count = len(metadata_retrieval_arweave_timers)
        metadata_retrieval_arweave_timeouts = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_ARWEAVE_TIMEOUT_STAT
        )
        metadata_retrieval_arweave_timeout_rate = (
            metadata_retrieval_arweave_timeouts / metadata_retrieval_arweave_timers_count
            if metadata_retrieval_arweave_timers_count
            else 0
        )
        metadata_retrieval_arweave_errors = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT
        )
        metadata_retrieval_arweave_error_rate = (
            metadata_retrieval_arweave_errors / metadata_retrieval_arweave_timers_count
            if metadata_retrieval_arweave_timers_count
            else 0
        )
        click.echo("  Arweave Gather Calls:")
        click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_arweave_timers)))
        click.echo("    Total Timeouts: {:,}".format(metadata_retrieval_arweave_timeouts))
        click.echo("    Timeout Rate: {:0.2f}".format(metadata_retrieval_arweave_timeout_rate))
        click.echo("    Total Errors: {:,}".format(metadata_retrieval_arweave_errors))
        click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_arweave_error_rate))
        click.echo(
            "    Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(metadata_retrieval_arweave_timers))
                if len(metadata_retrieval_arweave_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Median Batch Processing Time: {}".format(
                self.__get_time_from_secs(median(metadata_retrieval_arweave_timers))
                if len(metadata_retrieval_arweave_timers)
                else "n/a"
            )
        )
        click.echo(
            "    Mean Batch Processing Time: {}".format(
                self.__get_time_from_secs(mean(metadata_retrieval_arweave_timers))
                if len(metadata_retrieval_arweave_timers)
                else "n/a"
            )
        )
        metadata_retrieval_unsupported = self.__stats_service.get_count(
            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT
        )
        click.echo("  Total Unsupported URIs: {:,}".format(metadata_retrieval_unsupported))

        click.echo()
        click.echo("Stored Tokens:")
        tokens_stored = self.__stats_service.get_count(
            TokenPersistenceBatchProcessor.PROCESSED_STAT
        )
        click.echo("  Total Tokens Stored: {:,}".format(tokens_stored))
        dynamodb_write_tokens_batches = self.__stats_service.get_timings(
            TokenPersistenceBatchProcessor.DYNAMODB_TIMER_WRITE_TOKENS
        )
        dynamodb_write_tokens_batches_count = len(dynamodb_write_tokens_batches)
        click.echo(
            "  Get DynamoDB Tokens Batch Put Calls: {:,}".format(
                dynamodb_write_tokens_batches_count
            )
        )
        dynamodb_write_tokens_batches_average_size = (
            tokens_stored / dynamodb_write_tokens_batches_count
            if dynamodb_write_tokens_batches_count
            else 0.0
        )
        click.echo(
            "  DynamoDB Tokens Batch Put Avg Batch Size: {:0.2f}".format(
                dynamodb_write_tokens_batches_average_size
            )
        )
        click.echo(
            "  DynamoDB Tokens Batch Put Total Processing Time: {}".format(
                self.__get_time_from_secs(sum(dynamodb_write_tokens_batches))
                if len(dynamodb_write_tokens_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB Tokens Batch Put Median Processing Time: {}".format(
                self.__get_time_from_secs(median(dynamodb_write_tokens_batches))
                if len(dynamodb_write_tokens_batches)
                else "n/a"
            )
        )
        click.echo(
            "  DynamoDB Tokens Batch Put Mean Processing Time: {}".format(
                self.__get_time_from_secs(mean(dynamodb_write_tokens_batches))
                if len(dynamodb_write_tokens_batches)
                else "n/a"
            )
        )
