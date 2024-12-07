# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#
#   Copyright 2024 Valory AG
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

"""This package contains round behaviours of LearningAbciApp."""

import json
from abc import ABC
from pathlib import Path
from tempfile import mkdtemp
from typing import Dict, Generator, Optional, Set, Type, cast

from packages.valory.contracts.erc20.contract import ERC20
from packages.valory.contracts.gnosis_safe.contract import (
    GnosisSafeContract,
    SafeOperation,
)
from packages.valory.contracts.multisend.contract import (
    MultiSendContract,
    MultiSendOperation,
)
from packages.valory.protocols.contract_api import ContractApiMessage
from packages.valory.protocols.ledger_api import LedgerApiMessage
from packages.valory.skills.abstract_round_abci.base import AbstractRound
from packages.valory.skills.abstract_round_abci.behaviours import (
    AbstractRoundBehaviour,
    BaseBehaviour,
)
from packages.valory.skills.abstract_round_abci.io_.store import SupportedFiletype

from packages.valory.skills.token_data_collection_abci.models import (
    CoingeckoTopCryptocurrenciesSpecs,
    Params,
    SharedState,
)

from packages.valory.skills.token_data_collection_abci.payloads import (
    HistoricalDataCollectionPayload,
    RealTimeDataStreamingPayload,
    RecentDataCollectionPayload,
    TopCryptoListDataCollectionPayload,
)
from packages.valory.skills.token_data_collection_abci.rounds import (
    SynchronizedData,
    HistoricalDataCollectionRound,
    RealTimeDataStreamingRound,
    RecentDataCollectionRound,
    TopCryptoListDataCollectionRound,
    TokenDataCollectionAbciApp,
)

# Define some constants
ZERO_VALUE = 0
HTTP_OK = 200
GNOSIS_CHAIN_ID = "gnosis"
EMPTY_CALL_DATA = b"0x"
SAFE_GAS = 0
VALUE_KEY = "value"
TO_ADDRESS_KEY = "to_address"
METADATA_FILENAME = "metadata.json"


class TokenDataCollectionBaseBehaviour(BaseBehaviour, ABC):  # pylint: disable=too-many-ancestors
    """Base behaviour for the learning_abci behaviours."""

    @property
    def params(self) -> Params:
        """Return the params. Configs go here"""
        return cast(Params, super().params)

    @property
    def synchronized_data(self) -> SynchronizedData:
        """Return the synchronized data. This data is common to all agents"""
        return cast(SynchronizedData, super().synchronized_data)

    @property
    def local_state(self) -> SharedState:
        """Return the local state of this particular agent."""
        return cast(SharedState, self.context.state)

    @property
    def coingecko_top_cryptocurrencies_specs(self) -> CoingeckoTopCryptocurrenciesSpecs:
        """Get the Coingecko api specs."""
        return self.context.coingecko_top_cryptocurrencies_specs

    @property
    def metadata_filepath(self) -> str:
        """Get the temporary filepath to the metadata."""
        return str(Path(mkdtemp()) / METADATA_FILENAME)


    def get_sync_timestamp(self) -> float:
        """Get the synchronized time from Tendermint's last block."""
        now = cast(
            SharedState, self.context.state
        ).round_sequence.last_round_transition_timestamp.timestamp()

        return now


class TopCryptoListDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):   # pylint: disable=too-many-ancestors
    """TopCryptoListDataCollectionBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound]  = TopCryptoListDataCollectionRound

    def async_act(self) -> Generator:
        """Do the action."""
        with self.context.benchmark_tool.measure(self.behaviour_id).local():

            # Get the top crypto token list
            top_crypto_currencies = yield from self.get_top_crypto_list_specs()

            # Store the top crypto token list in IPFS
            top_crypto_currencies_ipfs_hash = yield from self.send_price_to_ipfs(top_crypto_currencies)
            
            # Prepare the payload to be shared with other agents
            # After consensus, all the agents will have the same price, price_ipfs_hash and balance variables in their synchronized data
            payload = TopCryptoListDataCollectionPayload(
                sender=self.context.agent_address,
                content="content", 
                top_crypto_currencies=top_crypto_currencies,
                top_crypto_currencies_ipfs_hash=top_crypto_currencies_ipfs_hash,
            )

        with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
            yield from self.send_a2a_transaction(payload)
            yield from self.wait_until_round_end()
        
        self.set_done()
    
    def get_top_crypto_list_specs(self) -> Generator[None, None, Optional[str]]:
        """Get token price from Coingecko using ApiSpecs"""
        
        try:
            self.context.logger.debug("Fetching historical price data.")
            
            # Get the specs
            specs = self.coingecko_top_cryptocurrencies_specs.get_spec()
    
            # Make the call
            raw_response = yield from self.get_http_response(**specs)
            # handle the error 
            if raw_response.status_code != HTTP_OK:
                self.context.logger.error(f"Failed to fetch crypto list data: {response.body}")
                return []

            # Process the response
            response = json.loads(raw_response.body)
    
            # Get the top cryptocurrencies
            top_crypto_currencies = ", ".join([crypto["id"] for crypto in response])
            self.context.logger.info(f"Top 10 Cryptocurrencies: {top_crypto_currencies}")
            
            # return the list of top cryptocurrencies  
            return top_crypto_currencies
        
        except Exception as e:
            self.context.logger.error(f"Exception in fetching historical data: {str(e)}")
            return []

    def send_price_to_ipfs(self, top_crypto_currencies) -> Generator[None, None, Optional[str]]:
        """Store the token price in IPFS"""
        data = {"top_crypto_currencies": top_crypto_currencies}
        top_crypto_currencies_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, obj=data, filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Price data stored in IPFS: https://gateway.autonolas.tech/ipfs/{top_crypto_currencies_ipfs_hash}"
        )
        return top_crypto_currencies_ipfs_hash



class HistoricalDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):   # pylint: disable=too-many-ancestors
    """HistoricalDataCollectionBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound]  = HistoricalDataCollectionRound

    def async_act(self) -> Generator:
        """Do the action."""
        with self.context.benchmark_tool.measure(self.behaviour_id).local():
            yield from self.fetch_crypto_historical_data()

            payload = HistoricalDataCollectionPayload(
                self.context.agent_address, "content"
            )
        
        with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
            yield from self.send_a2a_transaction(payload)
            yield from self.wait_until_round_end()

        self.set_done()

    def fetch_crypto_historical_data(self) -> Generator:
        """"""

        top_crypto_currencies = yield from self.get_top_crypto_currencies_from_ipfs()
        self.context.logger.info(f"top_crypto_currencies",top_crypto_currencies)

        all_crypto_ohlc_data = yield from self.fetch_ohlc_data(top_crypto_currencies)
        self.context.logger.info(f"all_crypto_ohlc_data",all_crypto_ohlc_data)
    
    def get_top_crypto_currencies_from_ipfs(self) -> Generator[None, None, Optional[dict]]:
        """Load the top crypto currencies data from IPFS"""

        ipfs_hash = self.synchronized_data.top_crypto_currencies_ipfs_hash

        top_crypto_currencies = yield from self.get_from_ipfs(
            ipfs_hash=ipfs_hash, filetype=SupportedFiletype.JSON
        )
        self.context.logger.error(f"Got top crypto currencies data from IPFS: {top_crypto_currencies}")
        return top_crypto_currencies

            
    def fetch_ohlc_data(self,crypto_ids, days=365, vs_currency='usd'):
        """ """
        
        all_crypto_ohlc_data = {}
        
        for crypto_id in crypto_ids:
            
            url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/ohlc?vs_currency={vs_currency}&days={days}"
            headers = {"accept": "application/json"}

            # Make the HTTP request to Coingecko API
            response = yield from self.get_http_response(
                method="GET", url=url, headers=headers
            )

            # Handle HTTP errors
            if response.status_code != HTTP_OK:
                self.context.logger.error(
                    f"Error while pulling the price from CoinGecko: {response.body}"
                )
            else:
                all_crypto_ohlc_data[crypto_id] = response.json()
            
                
        return all_crypto_ohlc_data
    



class RealTimeDataStreamingBehaviour(TokenDataCollectionBaseBehaviour):   # pylint: disable=too-many-ancestors
    """RealTimeDataStreamingBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound]  = RealTimeDataStreamingRound

    def async_act(self) -> Generator:
        """Do the action."""
        with self.context.benchmark_tool.measure(self.behaviour_id).local():
            payload = RealTimeDataStreamingPayload(
                self.context.agent_address, "content"
            )
        with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
            yield from self.send_a2a_transaction(payload)
            yield from self.wait_until_round_end()
        self.set_done()


class RecentDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):   # pylint: disable=too-many-ancestors
    """RecentDataCollectionBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound]  = RecentDataCollectionRound

    def async_act(self) -> Generator:
        """Do the action."""
        with self.context.benchmark_tool.measure(self.behaviour_id).local():
            payload = RecentDataCollectionPayload(self.context.agent_address, "content")
        with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
            yield from self.send_a2a_transaction(payload)
            yield from self.wait_until_round_end()
        self.set_done()

class TokenDataCollectionRoundBehaviour(AbstractRoundBehaviour):
    """This behaviour manages the consensus stages."""

    initial_behaviour_cls = TopCryptoListDataCollectionBehaviour
    abci_app_cls = TokenDataCollectionAbciApp   # type: ignore
    behaviours: Set[Type[BaseBehaviour]] = [  # type: ignore
        HistoricalDataCollectionBehaviour,
        RealTimeDataStreamingBehaviour,
        RecentDataCollectionBehaviour,
        TopCryptoListDataCollectionBehaviour,
    ]
