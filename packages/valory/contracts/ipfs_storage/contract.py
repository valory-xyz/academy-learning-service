# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2023-2024 Valory AG
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# ------------------------------------------------------------------------------

"""This module contains the class to connect to an ipfs_storage token contract."""

from typing import Dict

from aea.common import JSONLike
from aea.configurations.base import PublicId
from aea.contracts.base import Contract
from aea.crypto.base import LedgerApi
from aea_ledger_ethereum import EthereumApi


PUBLIC_ID = PublicId.from_str("valory/ipfs_storage:0.1.0")


class IPFSDataStorage(Contract):
    """
    This contract class interfaces with the Ethereum blockchain to interact with a smart contract designed
    for the storage and retrieval of IPFS hashes related to various cryptocurrency data. It facilitates the
    interaction with blockchain-stored IPFS hashes for comprehensive cryptocurrency analytics and insights.

    Attributes:
        contract_id (PublicId): Identifier for the contract, specifying its unique location within the AEA framework.
    """

    contract_id = PUBLIC_ID

    @classmethod
    def get_historical_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """Check the balance of the given account."""
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(
            contract_instance.functions, "getHistoricalDataIPFS"
        )  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_regulatory_geographic_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for regulatory and geographic data from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(
            contract_instance.functions, "getRegulatoryGeographicDataIPFS"
        )  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_social_media_sentiment_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for social media sentiment data from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(
            contract_instance.functions, "getSocialMediaSentimentDataIPFS"
        )  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_top_crypto_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for the top cryptocurrency data from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(contract_instance.functions, "getTopCryptoDataIPFS")  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_real_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for real-time cryptocurrency data from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(contract_instance.functions, "getRealDataIPFS")  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_recent_data_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for recent cryptocurrency data from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(contract_instance.functions, "getRecentDataIPFS")  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def get_comprehensive_crypto_insights_list_ipfs(
        cls,
        ledger_api: EthereumApi,
        contract_address: str,
    ) -> JSONLike:
        """
        Retrieve the IPFS hash for a comprehensive list of cryptocurrency insights from the blockchain.

        Returns:
            JSONLike: A dictionary containing the IPFS hash.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        ipfs_hash = getattr(
            contract_instance.functions, "getComprehensiveCryptoInsightsListIPFS"
        )  # noqa
        ipfs_hash = ipfs_hash().call()
        return dict(ipfs_hash=ipfs_hash)

    @classmethod
    def build_comprehensive_crypto_insights_list_tx(
        cls,
        ledger_api: LedgerApi,
        contract_address: str,
        _ipfsHash: str,
    ) -> Dict[str, bytes]:
        """
        Build a transaction to update the comprehensive cryptocurrency insights list on the blockchain.

        Args:
            ledger_api (LedgerApi): The API object to interact with the blockchain.
            contract_address (str): The address of the deployed contract.
            _ipfsHash (str): The new IPFS hash to be updated in the contract.

        Returns:
            Dict[str, bytes]: A dictionary containing the transaction data as bytes.
        """
        contract_instance = cls.get_instance(ledger_api, contract_address)
        data = contract_instance.encodeABI(
            "updateComprehensiveCryptoInsightsListIpfs", args=(_ipfsHash,)
        )
        return {"data": bytes.fromhex(data[2:])}
