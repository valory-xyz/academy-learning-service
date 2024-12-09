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

"""This package contains round behaviours of TokenDataCollectionAbciApp."""

import json
from abc import ABC
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Dict, Generator, Optional, Set, Type, cast, Iterator
import traceback
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests

from packages.valory.contracts.erc20.contract import ERC20
from packages.valory.contracts.ipfs_storage.contract import IPFSDataStorage
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
    CryptoTxPreparationPayload,
    ComprehensiveCryptoInsightsPayload,
    HistoricalDataCollectionPayload,
    RealTimeDataStreamingPayload,
    RecentDataCollectionPayload,
    TopCryptoListDataCollectionPayload,
)
from packages.valory.skills.token_data_collection_abci.rounds import (
    ComprehensiveCryptoInsightsRound,
    CryptoTxPreparationRound,
    HistoricalDataCollectionRound,
    RealTimeDataStreamingRound,
    RecentDataCollectionRound,
    SynchronizedData,
    TopCryptoListDataCollectionRound,
    TokenDataCollectionAbciApp,
)
from packages.valory.skills.transaction_settlement_abci.payload_tools import (
    hash_payload_to_hex,
)
from packages.valory.skills.transaction_settlement_abci.rounds import TX_HASH_LENGTH


# Define some constants
ZERO_VALUE = 0
HTTP_OK = 200
GNOSIS_CHAIN_ID = "gnosis"
EMPTY_CALL_DATA = b"0x"
SAFE_GAS = 0
VALUE_KEY = "value"
TO_ADDRESS_KEY = "to_address"
METADATA_FILENAME = "metadata.json"


class TokenDataCollectionBaseBehaviour(
    BaseBehaviour, ABC
):  # pylint: disable=too-many-ancestors
    """
    Base behaviour class for ABcI applications within the learning_abci framework, providing common properties
    and methods that facilitate the access and manipulation of shared parameters, synchronized data, and local state
    across different agents. This class serves as a foundational component for defining specific behaviors in
    the TokenDataCollection ABcI application.
    """

    @property
    def params(self) -> Params:
        """
        Provides access to configurable parameters for this behavior. These parameters include various settings
        and configurations that are critical for the operation of ABcI rounds.

        Returns:
            Params: An instance of Params containing configurations.
        """
        return cast(Params, super().params)

    @property
    def synchronized_data(self) -> SynchronizedData:
        """
        Provides access to data that is synchronized across all agents in the ABcI application. This data includes
        state information that must be consistent and available to all agents.

        Returns:
            SynchronizedData: An instance of SynchronizedData containing data shared across agents.
        """
        return cast(SynchronizedData, super().synchronized_data)

    @property
    def local_state(self) -> SharedState:
        """
        Provides access to the local state of this particular agent. The local state holds agent-specific data
        and settings, distinguishing individual agent states within a collective environment.

        Returns:
            SharedState: An instance of SharedState representing this agent's local state.
        """
        return cast(SharedState, self.context.state)

    @property
    def coingecko_top_cryptocurrencies_specs(self) -> CoingeckoTopCryptocurrenciesSpecs:
        """
        Retrieves the API specifications for interacting with the Coingecko API to fetch top cryptocurrency data.
        This specification is crucial for ensuring that API interactions are handled correctly.

        Returns:
            CoingeckoTopCryptocurrenciesSpecs: Specifications needed for Coingecko API requests.
        """
        return self.context.coingecko_top_cryptocurrencies_specs

    @property
    def metadata_filepath(self) -> str:
        """
        Generates a temporary filepath for storing metadata. This is particularly useful for operations that require
        temporary data storage and manipulation during a specific ABcI round.

        Returns:
            str: The path to the temporary metadata file.
        """
        return str(Path(mkdtemp()) / METADATA_FILENAME)

    def get_sync_timestamp(self) -> float:
        """
        Retrieves the timestamp of the last round transition, synchronized across all nodes via Tendermint's blockchain.
        This timestamp is essential for time-sensitive operations and synchronization checks.

        Returns:
            float: The timestamp of the last known round transition.
        """

        now = cast(
            SharedState, self.context.state
        ).round_sequence.last_round_transition_timestamp.timestamp()

        return now

    def current_round_id(self) -> str:
        """
        Retrieves the identifier of the current round from the round sequence. This ID is crucial for tracking
        the progress and state of the current behavior in the ABcI round sequence.

        Returns:
            str: The identifier of the current round.
        """
        id = cast(
            SharedState, self.context.state
        ).round_sequence.current_round.auto_round_id()

        return id


class TopCryptoListDataCollectionBehaviour(
    TokenDataCollectionBaseBehaviour
):  # pylint: disable=too-many-ancestors
    """
    Behaviour for collecting and storing top cryptocurrency list from the CoinGecko API, storing the list in IPFS,
    and sharing the list with other agents for consensus.

    Responsibilities:
    - Fetch top cryptocurrencies from CoinGecko API
    - Store the list in IPFS
    - Create and send a payload for consensus among agents in the network

    Attributes:
        matching_round (Type[AbstractRound]): The ABcI round class that this behaviour corresponds to.
    """

    matching_round: Type[AbstractRound] = TopCryptoListDataCollectionRound

    def async_act(self) -> Generator:
        """
        Asynchronously perform the actions of collecting and sharing top cryptocurrency list data.

        Steps:
        1. Fetch top cryptocurrencies from the CoinGecko API.
        2. Store the list in IPFS.
        3. Create and send payload for consensus among agents.

        The method implements retry mechanisms for API requests and error handling for each step of the process.
        """
        try:
            # Begin performance benchmarking for the local operations phase
            with self.context.benchmark_tool.measure(self.behaviour_id).local():
                # Simulate a short delay for real-time conditions
                sender = self.context.agent_address
                self.sleep(10)

                # Attempt to fetch the top cryptocurrency data
                top_crypto_currencies = yield from self.get_top_crypto_list_specs()

                # If no cryptocurrencies fetched after all retries
                if not top_crypto_currencies:
                    self.context.logger.warning(
                        "No cryptocurrencies fetched after all retries. Setting done."
                    )
                    self.set_done()
                    return

                # Store the top crypto token list in IPFS
                try:
                    top_crypto_currencies_ipfs_hash = (
                        yield from self.send_top_crypto_to_ipfs(top_crypto_currencies)
                    )
                except Exception as ipfs_error:
                    self.context.logger.error(f"IPFS storage failed: {ipfs_error}")
                    return

                # Prepare the payload to be shared with other agents
                payload = TopCryptoListDataCollectionPayload(
                    sender=sender,
                    top_crypto_currencies=top_crypto_currencies,
                    top_crypto_currencies_ipfs_hash=top_crypto_currencies_ipfs_hash,
                )

            # Measure the consensus phase
            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Critical error in async_act: {e}")
            self.context.logger.exception(traceback.format_exc())
            self.set_done()  # Directly set done in case of any unhandled exception

    def get_top_crypto_list_specs(self) -> Generator[None, None, Optional[str]]:
        """
        Fetch top cryptocurrencies from the CoinGecko API with retry mechanism, processing the responses to
        extract a comma-separated list of top cryptocurrency IDs.

        Returns:
            Optional[str]: Comma-separated list of top cryptocurrency IDs, or None if unsuccessful after retries.
        """
        max_retries = 4  # Total attempts including the initial try

        for attempt in range(max_retries):
            try:
                self.context.logger.info(
                    f"Initiating top cryptocurrency list fetch (Attempt {attempt + 1})"
                )

                # Validate API specs
                if not hasattr(self, "coingecko_top_cryptocurrencies_specs"):
                    raise ValueError("CoinGecko API specs are not configured")

                # Get the specs
                specs = self.coingecko_top_cryptocurrencies_specs.get_spec()

                # Make the HTTP call
                raw_response = yield from self.get_http_response(**specs)

                # Validate HTTP response
                if raw_response.status_code != HTTP_OK:
                    error_msg = (
                        f"API request failed with status {raw_response.status_code}"
                    )
                    self.context.logger.error(error_msg)
                    self.context.logger.error(f"Response body: {raw_response.body}")

                    # If it's a rate limit error, wait and retry
                    if raw_response.status_code == 429 and attempt < max_retries - 1:
                        wait_time = 2**attempt  # Exponential backoff
                        self.context.logger.info(
                            f"Rate limited. Waiting {wait_time} seconds before retry."
                        )
                        yield from self.sleep(wait_time)
                        continue  # Only continue if not the last attempt and recoverable error

                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        self.context.logger.error(
                            "Max retries reached. Unable to fetch cryptocurrencies."
                        )
                        return None

                # Parse the response
                try:
                    response = json.loads(raw_response.body)
                except json.JSONDecodeError as json_error:
                    self.context.logger.error(f"JSON parsing error: {json_error}")

                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None

                    # Wait and retry
                    yield from self.sleep(2**attempt)
                    continue

                # Validate response structure
                if not isinstance(response, list) or len(response) == 0:
                    self.context.logger.warning(
                        "Received empty or invalid cryptocurrency list"
                    )

                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None

                    # Wait and retry
                    yield from self.sleep(2**attempt)
                    continue

                # Extract top cryptocurrencies
                try:
                    top_crypto_currencies = ", ".join(
                        [
                            crypto.get("id", "")
                            for crypto in response
                            if crypto.get("id")
                        ]
                    )
                except Exception as extraction_error:
                    self.context.logger.error(
                        f"Error extracting cryptocurrency IDs: {extraction_error}"
                    )

                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None

                    # Wait and retry
                    yield from self.sleep(2**attempt)
                    continue

                # Log and return if successful
                if not top_crypto_currencies:
                    self.context.logger.warning("No valid cryptocurrency IDs found")

                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None

                    # Wait and retry
                    yield from self.sleep(2**attempt)
                    continue

                self.context.logger.info(
                    f"Top Cryptocurrencies: {top_crypto_currencies}"
                )
                return top_crypto_currencies

            except Exception as e:
                self.context.logger.error(
                    f"Unexpected error in get_top_crypto_list_specs (Attempt {attempt + 1}): {e}"
                )

                # If it's the last attempt, log and exit
                if attempt == max_retries - 1:
                    self.context.logger.exception(traceback.format_exc())
                    return None

                # Wait before retrying
                yield from self.sleep(2**attempt)

        # If all retries fail
        self.context.logger.error(
            "Failed to fetch top cryptocurrencies after all retries"
        )
        return None

    def send_top_crypto_to_ipfs(
        self, top_crypto_currencies: str
    ) -> Generator[None, None, Optional[str]]:
        """
        Simulated method to send data to IPFS. This method would be replaced with actual IPFS storage logic.

        Args:
            filename (str): The filename under which the data will be stored in IPFS.
            obj (dict): The data object to be stored.
            filetype (SupportedFiletype): The type of file to be stored.

        Returns:
            str: The IPFS hash of the stored data.
        """
        try:
            # Validate input
            if not top_crypto_currencies:
                raise ValueError("Empty cryptocurrency list cannot be stored")

            # Prepare data for IPFS storage
            data = {"top_crypto_currencies": top_crypto_currencies}

            # Validate metadata filepath
            if not hasattr(self, "metadata_filepath"):
                raise AttributeError("Metadata filepath is not configured")

            # Store in IPFS
            top_crypto_currencies_ipfs_hash = yield from self.send_to_ipfs(
                filename=self.metadata_filepath,
                obj=data,
                filetype=SupportedFiletype.JSON,
            )

            # Log successful storage
            self.context.logger.info(
                f"Top cryptocurrencies stored in IPFS: https://gateway.autonolas.tech/ipfs/{top_crypto_currencies_ipfs_hash}"
            )

            return top_crypto_currencies_ipfs_hash

        except Exception as e:
            self.context.logger.error(
                f"Error storing top cryptocurrencies in IPFS: {e}"
            )
            self.context.logger.exception(traceback.format_exc())
            raise


class HistoricalDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):
    """
    This behaviour manages the collection of historical data for top cryptocurrencies.
    It involves fetching historical OHLC (Open, High, Low, Close) data and other market
    data from external APIs, storing this data in IPFS, and managing the consensus process
    among multiple agents.

    Attributes:
        matching_round (Type[AbstractRound]): Specifies the ABcI round class that this behaviour matches,
                                              which is the HistoricalDataCollectionRound.
    """

    matching_round: Type[AbstractRound] = HistoricalDataCollectionRound

    def async_act(self) -> Generator:
        """
        Asynchronously perform the actions to collect historical data.

        The method includes:
        - Fetching top cryptocurrencies from IPFS.
        - Fetching and aggregating OHLC data for these cryptocurrencies.
        - Storing the aggregated data in IPFS.
        - Creating and sending a payload for agent consensus.
        """
        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():

                sender = self.context.agent_address
                # Fetch top cryptocurrencies
                top_crypto_currencies = (
                    yield from self.get_top_crypto_currencies_from_ipfs()
                )

                # Log the fetched cryptocurrencies
                self.context.logger.info(
                    f"Fetched top cryptocurrencies: {top_crypto_currencies}"
                )

                # Fetch OHLC data for the cryptocurrencies
                all_crypto_ohlc_market_data = yield from self.aggregate_crypto_data(
                    top_crypto_currencies
                )

                # Log the fetched OHLC data
                self.context.logger.info(
                    f"Fetched OHLC and Market data for {len(all_crypto_ohlc_market_data)} cryptocurrencies"
                )

                # Store the top crypto token list in IPFS
                ohlc_market_data_ipfs_hash = yield from self.send_ohlc_market_data_ipfs(
                    all_crypto_ohlc_market_data
                )

                # Prepare payload
                payload = HistoricalDataCollectionPayload(
                    sender=sender, ohlc_market_data_ipfs_hash=ohlc_market_data_ipfs_hash
                )

            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in historical data collection: {e}")
            self.context.logger.exception(traceback.format_exc())

    def get_top_crypto_currencies_from_ipfs(
        self,
    ) -> Generator[None, None, Optional[list[str]]]:
        """
        Retrieve the list of top cryptocurrency identifiers from IPFS.

        Returns:
            Optional[list[str]]: A list of top cryptocurrency identifiers if successful, None otherwise.
        """
        try:
            self.context.logger.info(
                "Attempting to retrieve top cryptocurrencies from IPFS"
            )

            # Validate IPFS hash
            if not hasattr(self.synchronized_data, "top_crypto_currencies_ipfs_hash"):
                raise ValueError("IPFS hash for top cryptocurrencies is not available")

            ipfs_hash = self.synchronized_data.top_crypto_currencies_ipfs_hash
            self.context.logger.info(f"IPFS hash retrieved: {ipfs_hash}")

            # Fetch data from IPFS
            top_crypto_currencies = yield from self.get_from_ipfs(
                ipfs_hash=ipfs_hash, filetype=SupportedFiletype.JSON
            )

            # Validate retrieved data
            if (
                not top_crypto_currencies
                or "top_crypto_currencies" not in top_crypto_currencies
            ):
                raise ValueError("No cryptocurrency data found in IPFS")

            # Parse and return cryptocurrency list
            crypto_list = top_crypto_currencies.get("top_crypto_currencies", "").split(
                ", "
            )

            if not crypto_list:
                raise ValueError("Empty cryptocurrency list")

            return crypto_list

        except Exception as e:
            self.context.logger.error(
                f"Error retrieving top cryptocurrencies from IPFS: {e}"
            )
            self.context.logger.exception(traceback.format_exc())
            raise

    def process_crypto_data(
        self, ohlc_data: list[list[float]], market_data: Dict[str, list[list[float]]]
    ) -> Dict[str, list[Dict[str, Any]]]:
        """
        Process cryptocurrency OHLC and market data into a structured format.

        Args:
            ohlc_data (list[list[float]]): OHLC data from CoinGecko API
            market_data (Dict[str, list[list[float]]]): Market data including volumes and market caps

        Returns:
            Dict[str, list[Dict[str, Any]]]: Processed cryptocurrency data
        """
        try:
            # Validate input data
            if not ohlc_data or not market_data:
                raise ValueError("Empty OHLC or market data provided")

            # Create DataFrame from OHLC data
            try:
                ohlc_df = pd.DataFrame(
                    ohlc_data, columns=["timestamp", "open", "high", "low", "close"]
                )

                # Convert timestamp to datetime
                ohlc_df["timestamp"] = pd.to_datetime(ohlc_df["timestamp"], unit="ms")
                ohlc_df.set_index("timestamp", inplace=True)

            except Exception as e:
                self.context.logger.error(f"Error creating OHLC DataFrame: {e}")
                raise

            # Process volume data
            try:
                volume_df = pd.DataFrame(
                    market_data.get("total_volumes", []),
                    columns=["timestamp", "volume"],
                )
                volume_df["timestamp"] = pd.to_datetime(
                    volume_df["timestamp"], unit="ms"
                )
                volume_df.set_index("timestamp", inplace=True)

            except Exception as e:
                self.context.logger.error(f"Error processing volume data: {e}")
                raise

            # Process market cap data
            try:
                market_cap_df = pd.DataFrame(
                    market_data.get("market_caps", []),
                    columns=["timestamp", "market_cap"],
                )
                market_cap_df["timestamp"] = pd.to_datetime(
                    market_cap_df["timestamp"], unit="ms"
                )
                market_cap_df.set_index("timestamp", inplace=True)

            except Exception as e:
                self.context.logger.error(f"Error processing market cap data: {e}")
                raise

            # Combine dataframes
            try:
                crypto_df = pd.concat(
                    [ohlc_df, volume_df["volume"], market_cap_df["market_cap"]], axis=1
                )

                # Fill any NaN values to ensure data consistency
                crypto_df.fillna(method="ffill", inplace=True)

            except Exception as e:
                self.context.logger.error(f"Error combining dataframes: {e}")
                raise

            # Log data summary
            self.context.logger.info("Cryptocurrency Data Summary:")
            self.context.logger.info(
                f"  Date range: {crypto_df.index.min()} to {crypto_df.index.max()}"
            )
            self.context.logger.info(f"  Total data points: {len(crypto_df)}")

            # Convert to list of dictionaries for serialization
            try:
                crypto_data = crypto_df.reset_index().to_dict(orient="records")
                return crypto_data

            except Exception as e:
                self.context.logger.error(
                    f"Error converting DataFrame to dictionary: {e}"
                )
                raise

        except Exception as e:
            self.context.logger.error(f"Comprehensive error in data processing: {e}")
            self.context.logger.exception(traceback.format_exc())
            return []

    def aggregate_crypto_data(
        self, crypto_ids: list[str]
    ) -> Generator[None, None, Dict[str, list[Dict[str, Any]]]]:
        """
        Fetch and aggregate OHLC and market data for specified cryptocurrencies.

        Args:
            crypto_ids (list[str]): List of cryptocurrency identifiers.

        Returns:
            Dict[str, Any]: A dictionary containing aggregated data for each cryptocurrency.
        """
        all_crypto_data = {}
        failed_cryptocurrencies = []

        for crypto_id in crypto_ids:
            try:
                # Fetch OHLC data
                ohlc_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/ohlc?vs_currency=usd&days=365"
                ohlc_response = yield from self.get_http_response(
                    method="GET", url=ohlc_url
                )

                if ohlc_response.status_code != HTTP_OK:
                    self.context.logger.error(
                        f"OHLC data fetch failed for {crypto_id}: {ohlc_response.status_code}"
                    )
                    failed_cryptocurrencies.append(crypto_id)
                    continue

                # Respect API rate limits
                self.sleep(3)

                # Fetch market data
                market_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=365"
                market_response = yield from self.get_http_response(
                    method="GET", url=market_url
                )

                if market_response.status_code != HTTP_OK:
                    self.context.logger.error(
                        f"Market data fetch failed for {crypto_id}: {market_response.status_code}"
                    )
                    failed_cryptocurrencies.append(crypto_id)
                    continue

                # Parse responses
                ohlc_data = ohlc_response.json()
                market_data = market_response.json()

                # Process data
                processed_data = yield from self.process_crypto_data(
                    ohlc_data, market_data
                )

                if processed_data:
                    all_crypto_data[crypto_id] = processed_data
                    self.context.logger.info(
                        f"Successfully processed data for {crypto_id}"
                    )
                else:
                    failed_cryptocurrencies.append(crypto_id)

                # Respect API rate limits
                self.sleep(3)

            except Exception as e:
                self.context.logger.error(f"Error processing data for {crypto_id}: {e}")
                self.context.logger.exception(traceback.format_exc())
                failed_cryptocurrencies.append(crypto_id)

        # Log summary of data collection
        if failed_cryptocurrencies:
            self.context.logger.warning(
                f"Failed to fetch data for cryptocurrencies: {failed_cryptocurrencies}"
            )

        return all_crypto_data

    def send_ohlc_market_data_ipfs(
        self, all_crypto_ohlc_market_data
    ) -> Generator[None, None, Optional[str]]:
        """
        Store the aggregated OHLC and market data in IPFS.

        Args:
            all_crypto_ohlc_market_data (Dict[str, Any]): The aggregated market data.

        Returns:
            Optional[str]: The IPFS hash of the stored data if successful.
        """

        data = {"all_crypto_ohlc_market_data": all_crypto_ohlc_market_data}

        ohlc_market_data_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, obj=data, filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Price data stored in IPFS: https://gateway.autonolas.tech/ipfs/{ohlc_market_data_ipfs_hash}"
        )
        return ohlc_market_data_ipfs_hash


class RecentDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):
    """
    This behaviour manages the collection of recent data for top cryptocurrencies. It involves
    fetching data from different timeframes (daily, weekly, monthly) and additional market details,
    storing this data in IPFS, and managing the consensus process among multiple agents.

    Attributes:
        matching_round (Type[AbstractRound]): Specifies the ABcI round class that this behaviour matches,
                                              which is the RecentDataCollectionRound.
    """

    matching_round: Type[AbstractRound] = RecentDataCollectionRound

    def async_act(self) -> Generator:
        """
        Asynchronously perform the actions to collect recent data for cryptocurrencies.

        The method includes:
        - Fetching top cryptocurrencies from IPFS.
        - Fetching and aggregating recent data for these cryptocurrencies.
        - Storing the aggregated data in IPFS.
        - Creating and sending a payload for agent consensus.
        """
        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():
                sender = self.context.agent_address

                # Fetch top cryptocurrencies
                top_crypto_currencies = (
                    yield from self.get_top_crypto_currencies_from_ipfs()
                )

                # Log the fetched cryptocurrencies
                self.context.logger.info(
                    f"Fetched top cryptocurrencies: {top_crypto_currencies}"
                )

                # Fetch recent crypto data
                all_crypto_recent_data = yield from self.aggregate_recent_crypto_data(
                    top_crypto_currencies
                )

                # Log the fetched recent data
                self.context.logger.info(
                    f"Fetched recent data for {len(all_crypto_recent_data)} cryptocurrencies"
                )

                # Store the recent data in IPFS
                recent_data_ipfs_hash = yield from self.send_recent_data_to_ipfs(
                    all_crypto_recent_data
                )

                # Prepare payload
                payload = RecentDataCollectionPayload(
                    sender=sender, recent_data_ipfs_hash=recent_data_ipfs_hash
                )

            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in recent data collection: {e}")
            self.context.logger.exception(traceback.format_exc())

    def get_top_crypto_currencies_from_ipfs(
        self,
    ) -> Generator[None, None, Optional[list[str]]]:
        """
        Retrieve the list of top cryptocurrency identifiers from IPFS.

        Returns:
            Optional[list[str]]: A list of top cryptocurrency identifiers if successful, None otherwise.
        """
        try:
            self.context.logger.info(
                "Attempting to retrieve top cryptocurrencies from IPFS"
            )

            # Validate IPFS hash
            if not hasattr(self.synchronized_data, "top_crypto_currencies_ipfs_hash"):
                raise ValueError("IPFS hash for top cryptocurrencies is not available")

            ipfs_hash = self.synchronized_data.top_crypto_currencies_ipfs_hash
            self.context.logger.info(f"IPFS hash retrieved: {ipfs_hash}")

            # Fetch data from IPFS
            top_crypto_currencies = yield from self.get_from_ipfs(
                ipfs_hash=ipfs_hash, filetype=SupportedFiletype.JSON
            )

            # Validate retrieved data
            if (
                not top_crypto_currencies
                or "top_crypto_currencies" not in top_crypto_currencies
            ):
                raise ValueError("No cryptocurrency data found in IPFS")

            # Parse and return cryptocurrency list
            crypto_list = top_crypto_currencies.get("top_crypto_currencies", "").split(
                ", "
            )

            if not crypto_list:
                raise ValueError("Empty cryptocurrency list")

            return crypto_list

        except Exception as e:
            self.context.logger.error(
                f"Error retrieving top cryptocurrencies from IPFS: {e}"
            )
            self.context.logger.exception(traceback.format_exc())
            raise

    def aggregate_recent_crypto_data(
        self, crypto_ids: list[str]
    ) -> Generator[None, None, Dict[str, list[Dict[str, Any]]]]:
        """
        Aggregate recent cryptocurrency data for multiple cryptocurrencies.

        Args:
            crypto_ids (list[str]): list of cryptocurrency identifiers

        Returns:
            Dict[str, list[Dict[str, Any]]]: Aggregated recent cryptocurrency data
        """
        all_crypto_recent_data = {}
        failed_cryptocurrencies = []

        for crypto_id in crypto_ids:
            try:
                # Fetch daily data for last 30 days
                daily_data = yield from self.fetch_daily_data(crypto_id)

                # Fetch weekly data for last 6 months
                weekly_data = yield from self.fetch_weekly_data(crypto_id)

                # Fetch monthly data for last year
                monthly_data = yield from self.fetch_monthly_data(crypto_id)

                # Fetch additional market data
                market_details = yield from self.fetch_market_details(crypto_id)

                # Process and combine data
                processed_data = {
                    "daily_data": daily_data,
                    "weekly_data": weekly_data,
                    "monthly_data": monthly_data,
                    "market_details": market_details,
                }

                if processed_data:
                    all_crypto_recent_data[crypto_id] = processed_data
                    self.context.logger.info(
                        f"Successfully processed recent data for {crypto_id}"
                    )
                else:
                    failed_cryptocurrencies.append(crypto_id)

                # Respect API rate limits
                self.sleep(3)

            except Exception as e:
                self.context.logger.error(
                    f"Error processing recent data for {crypto_id}: {e}"
                )
                self.context.logger.exception(traceback.format_exc())
                failed_cryptocurrencies.append(crypto_id)

        # Log summary of data collection
        if failed_cryptocurrencies:
            self.context.logger.warning(
                f"Failed to fetch recent data for cryptocurrencies: {failed_cryptocurrencies}"
            )

        return all_crypto_recent_data

    def fetch_daily_data(
        self, crypto_id: str
    ) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch daily data for the last 30 days.

        Args:
            crypto_id (str): Cryptocurrency identifier

        Returns:
            list[Dict[str, Any]]: Daily price and market data
        """
        try:
            daily_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=30"
            daily_response = yield from self.get_http_response(
                method="GET", url=daily_url
            )

            if daily_response.status_code != HTTP_OK:
                self.context.logger.error(
                    f"Daily data fetch failed for {crypto_id}: {daily_response.status_code}"
                )
                return []

            daily_data = daily_response.json()
            processed_daily_data = self.process_market_chart_data(daily_data)

            return processed_daily_data

        except Exception as e:
            self.context.logger.error(f"Error fetching daily data for {crypto_id}: {e}")
            return []

    def fetch_weekly_data(
        self, crypto_id: str
    ) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch weekly data for the last 6 months.

        Args:
            crypto_id (str): Cryptocurrency identifier

        Returns:
            list[Dict[str, Any]]: Weekly price and market data
        """
        try:
            weekly_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=180"

            weekly_response = yield from self.get_http_response(
                method="GET", url=weekly_url
            )

            if weekly_response.status_code != HTTP_OK:
                self.context.logger.error(
                    f"Weekly data fetch failed for {crypto_id}: {weekly_response.status_code}"
                )
                return []

            weekly_data = weekly_response.json()
            processed_weekly_data = self.process_market_chart_data(
                weekly_data, resample="W"
            )

            return processed_weekly_data

        except Exception as e:
            self.context.logger.error(
                f"Error fetching weekly data for {crypto_id}: {e}"
            )
            return []

    def fetch_monthly_data(
        self, crypto_id: str
    ) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch monthly data for the last year.

        Args:
            crypto_id (str): Cryptocurrency identifier

        Returns:
            list[Dict[str, Any]]: Monthly price and market data
        """
        try:
            monthly_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=365"
            monthly_response = yield from self.get_http_response(
                method="GET", url=monthly_url
            )

            if monthly_response.status_code != HTTP_OK:
                self.context.logger.error(
                    f"Monthly data fetch failed for {crypto_id}: {monthly_response.status_code}"
                )
                return []

            monthly_data = monthly_response.json()
            processed_monthly_data = self.process_market_chart_data(
                monthly_data, resample="M"
            )

            return processed_monthly_data

        except Exception as e:
            self.context.logger.error(
                f"Error fetching monthly data for {crypto_id}: {e}"
            )
            return []

    def fetch_market_details(
        self, crypto_id: str
    ) -> Generator[None, None, Dict[str, Any]]:
        """
        Fetch additional market details including liquidity and significant events.

        Args:
            crypto_id (str): Cryptocurrency identifier

        Returns:
            Dict[str, Any]: Market details
        """
        try:
            # Fetch detailed market information
            market_details_url = (
                f"{self.params.coingecko_histroy_data_template}/coins/{crypto_id}"
            )
            market_details_response = yield from self.get_http_response(
                method="GET", url=market_details_url
            )

            if market_details_response.status_code != HTTP_OK:
                self.context.logger.error(
                    f"Market details fetch failed for {crypto_id}: {market_details_response.status_code}"
                )
                return {}

            market_details = market_details_response.json()

            # Extract relevant market information
            processed_details = {
                "market_cap_rank": market_details.get("market_cap_rank"),
                "current_price": market_details.get("market_data", {})
                .get("current_price", {})
                .get("usd"),
                "total_volume": market_details.get("market_data", {})
                .get("total_volume", {})
                .get("usd"),
                "liquidity_score": market_details.get("liquidity_score"),
                "significant_events": self.extract_significant_events(market_details),
            }

            return processed_details

        except Exception as e:
            self.context.logger.error(
                f"Error fetching market details for {crypto_id}: {e}"
            )
            return {}

    def extract_significant_events(
        self, market_details: Dict[str, Any]
    ) -> list[Dict[str, Any]]:
        """
        Extract significant market events from market details.

        Args:
            market_details (Dict[str, Any]): Detailed market information

        Returns:
            list[Dict[str, Any]]: list of significant events
        """
        # This is a placeholder method. In a real implementation,
        # parse through the market details to extract
        # significant events like forks, major updates, etc.
        events = []

        # Example of how you might extract events
        if market_details.get("categories"):
            events.append(
                {"type": "category_update", "details": market_details["categories"]}
            )

        return events

    def process_market_chart_data(
        self, market_chart_data: Dict[str, list[list[float]]], resample: str = None
    ) -> list[Dict[str, Any]]:
        """
        Process market chart data with optional resampling.

        Args:
            market_chart_data (Dict[str, list[list[float]]]): Market chart data
            resample (str, optional): Resampling frequency

        Returns:
            list[Dict[str, Any]]: Processed market data
        """
        try:
            # Create DataFrames for prices, volumes, and market caps
            prices_df = pd.DataFrame(
                market_chart_data.get("prices", []), columns=["timestamp", "price"]
            )
            volumes_df = pd.DataFrame(
                market_chart_data.get("total_volumes", []),
                columns=["timestamp", "volume"],
            )
            market_caps_df = pd.DataFrame(
                market_chart_data.get("market_caps", []),
                columns=["timestamp", "market_cap"],
            )

            # Convert timestamps
            for df in [prices_df, volumes_df, market_caps_df]:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                df.set_index("timestamp", inplace=True)

            # Optional resampling
            if resample:
                prices_df = prices_df.resample(resample).mean()
                volumes_df = volumes_df.resample(resample).sum()
                market_caps_df = market_caps_df.resample(resample).mean()

            # Combine data
            combined_df = pd.concat([prices_df, volumes_df, market_caps_df], axis=1)
            combined_df.reset_index(inplace=True)

            # Convert to list of dictionaries
            processed_data = combined_df.to_dict(orient="records")

            return processed_data

        except Exception as e:
            self.context.logger.error(f"Error processing market chart data: {e}")
            return []

    def send_recent_data_to_ipfs(
        self, all_crypto_recent_data
    ) -> Generator[None, None, Optional[str]]:
        """
        Store the aggregated recent cryptocurrency data in IPFS and retrieve the hash.

        Args:
            all_crypto_recent_data (Dict[str, Dict[str, list]]): Aggregated recent data for multiple cryptocurrencies.

        Returns:
            Optional[str]: IPFS hash of the stored data if successful.
        """
        data = {"all_crypto_recent_data": all_crypto_recent_data}
        recent_data_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, obj=data, filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Recent data stored in IPFS: https://gateway.autonolas.tech/ipfs/{recent_data_ipfs_hash}"
        )
        return recent_data_ipfs_hash


class RealTimeDataStreamingBehaviour(
    TokenDataCollectionBaseBehaviour
):  # pylint: disable=too-many-ancestors
    """
    RealTimeDataStreamingBehaviour handles the streaming of real-time data related to cryptocurrencies.
    This behaviour is responsible for creating payloads of real-time data and managing the consensus
    process for these data among agents in the ABcI application.

    This implementation is designed to operate within rounds defined by the RealTimeDataStreamingRound,
    ensuring that real-time data is consistently captured, processed, and agreed upon by all participating agents.

    Attributes:
        matching_round (Type[AbstractRound]): Specifies the ABcI round class that this behaviour matches,
                                              which is the RealTimeDataStreamingRound.
    """

    matching_round: Type[AbstractRound] = RealTimeDataStreamingRound

    def async_act(self) -> Generator:
        """
        Perform the asynchronous actions required for real-time data streaming.

        The method includes:
        - Creating a payload containing the real-time data.
        - Sending the payload across to other agents for consensus.
        - Waiting until the round ends to ensure all data is synchronized and confirmed.

        This behavior encapsulates both the creation of the data payload and the coordination
        of the consensus process, ensuring that real-time data is handled efficiently and accurately.
        """
        try:
            # Measure the local operation of creating the real-time data payload
            with self.context.benchmark_tool.measure(self.behaviour_id).local():
                payload = RealTimeDataStreamingPayload(
                    self.context.agent_address, "content"
                )
            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in real-time data streaming: {e}")
            self.context.logger.exception(traceback.format_exc())
            self.set_done()  # Ensure that the behavior is marked as complete even in case of error


class ComprehensiveCryptoInsightsBehaviour(TokenDataCollectionBaseBehaviour):
    """
    Handles the collection and processing of comprehensive cryptocurrency insights. This behaviour is integral
    to gathering diverse data sets from IPFS, analyzing these datasets to generate actionable insights, and then
    distributing these insights through a consensus mechanism among ABcI agents.

    It leverages IPFS to access historical, recent, and real-time data, sentiment analysis, regulatory changes,
    and news updates related to cryptocurrencies. The behavior consolidates this information into structured
    insights, which include detailed token analysis and recommendations.

    Attributes:
        matching_round (Type[AbstractRound]): Specifies the ABcI round that this behaviour orchestrates,
                                              which is the ComprehensiveCryptoInsightsRound.
    """

    matching_round: Type[AbstractRound] = ComprehensiveCryptoInsightsRound

    def async_act(self) -> Generator:
        """
        Execute asynchronous actions to manage comprehensive crypto insights collection and analysis.

        Steps:
        1. Retrieve predefined IPFS URLs for data sources.
        2. Fetch all relevant data from these URLs.
        3. Process the data to generate token lists and detailed insights.
        4. Store the insights in IPFS.
        5. Create and send a payload containing the insights and its IPFS hash to other agents for consensus.
        """

        # IPFS URLs for data sources
        ipfs_urls = self.define_ipfs_urls()

        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():

                sender = self.context.agent_address
                # Fetch all data sources
                data_sources = dict(self.fetch_ipfs_data(ipfs_urls))

                # Generate and organize token lists
                token_generator = self.generate_token_lists(data_sources)

                comprehensive_crypto_currencies_insights = self.organize_token_lists(
                    token_generator
                )
                self.context.logger.info(
                    f"Analysis complete. Results saved to token_lists.json:{comprehensive_crypto_currencies_insights}"
                )

                self.context.logger.info(
                    f"Analysis complete. Results saved to crypto_token_lists.json:{type(comprehensive_crypto_currencies_insights)}"
                )

                # Store the recent data in IPFS
                comprehensive_crypto_currencies_insights_ipfs_hash = (
                    yield from self.send_comprensive_crypto_list_data_to_ipfs(
                        comprehensive_crypto_currencies_insights
                    )
                )

                self.context.logger.info(
                    f"Analysis complete. Results saved to comprehensive_crypto_currencies_insights_ipfs_hash.json:{(comprehensive_crypto_currencies_insights_ipfs_hash)}"
                )
                # Prepare payload
                payload = ComprehensiveCryptoInsightsPayload(
                    sender=sender,
                    comprehensive_crypto_currencies_insights=comprehensive_crypto_currencies_insights,
                    comprehensive_crypto_currencies_insights_ipfs_hash=comprehensive_crypto_currencies_insights_ipfs_hash,
                )

            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in recent data collection: {e}")
            self.context.logger.exception(traceback.format_exc())

    def define_ipfs_urls(self) -> Dict[str, str]:
        """
        Define IPFS URLs for various cryptocurrency data sources.

        Returns:
            Dict[str, str]: A dictionary of data source names and their corresponding IPFS URLs.
        """
        return {
            "realtime": "https://gateway.pinata.cloud/ipfs/QmWe6CgU1NMVN8juiJcCqJ8qZYjTaB8nkGnjK4CG49g99f",
            "recent_time": "https://gateway.pinata.cloud/ipfs/QmT9DNsqkNNjnW6DXaPv1jMyeKoCNGon9txRV1i4fep3Hw",
            "sentiment": "https://gateway.pinata.cloud/ipfs/QmZJ11k3cnVMDE9fm3U7k8VjkN5ZheHyK3mQFN8DxXBpGP",
            "top10crypto": "https://gateway.pinata.cloud/ipfs/QmfZoKWTL3XZMh1m9izazCb9AZMhb6XgeLMGxLJjLj7TpJ",
            "regulatory": "https://gateway.pinata.cloud/ipfs/QmbMCWA6cfLy1mfRaPzkBoYgHHhTD2T8TmhT6Z8zpMpBzr",
            "news": "https://gateway.pinata.cloud/ipfs/QmbMCWA6cfLy1mfRaPzkBoYgHHhTD2T8TmhT6Z8zpMpBzr",
            "history": "https://gateway.pinata.cloud/ipfs/QmQjDad2HPsvC98kmzEXdwehEPGnpKTwFSBaYnMDDWhjvX",
        }

    def send_comprensive_crypto_list_data_to_ipfs(
        self, comprehensive_crypto_currencies_insights
    ) -> Generator[None, None, Optional[str]]:
        """
        Store the recent cryptocurrency data in IPFS.

        Args:
            all_crypto_recent_data (Dict): Cryptocurrency recent data

        Returns:
            Optional[str]: IPFS hash of stored data
        """
        data = {"all_crypto_recent_data": comprehensive_crypto_currencies_insights}
        comprehensive_crypto_data_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, obj=data, filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Recent data stored in IPFS: https://gateway.autonolas.tech/ipfs/{comprehensive_crypto_data_ipfs_hash}"
        )
        return comprehensive_crypto_data_ipfs_hash

    def fetch_ipfs_data(
        self, ipfs_urls: Dict[str, str]
    ) -> Generator[tuple, None, None]:
        """
        Fetch data from IPFS gateway URLs with robust error handling using generator

        Args:
            ipfs_urls (dict): Dictionary of data source names and their IPFS URLs

        Yields:
            tuple: (key, data) for successfully fetched data sources
        """
        for key, url in ipfs_urls.items():
            try:
                # Use requests to fetch data from IPFS gateway
                response = requests.get(url, timeout=30)

                # Check if request was successful
                if response.status_code == 200:
                    try:
                        # Try to parse JSON data
                        data = response.json()

                        # Special handling for nested dictionaries if needed
                        if key in ["regulatory", "news"]:
                            data = data.get(f"crypto_{key}_data", {})

                        self.context.logger.info(
                            f"Successfully loaded {key} from {url}"
                        )
                        yield key, data

                    except json.JSONDecodeError:
                        self.context.logger.info(f"Error: Invalid JSON for {key}")
                        yield key, {}
                else:
                    self.context.logger.info(
                        f"Failed to fetch {key}. Status code: {response.status_code}"
                    )
                    yield key, {}

            except requests.RequestException as e:
                self.context.logger.info(f"Network error fetching {key}: {e}")
                yield key, {}
            except Exception as e:
                self.context.logger.info(f"Unexpected error loading {key}: {e}")
                yield key, {}

    def yield_price_data(
        self, historical_data: list[Dict]
    ) -> Generator[float, None, None]:
        """
        Generator to yield valid price data

        Args:
            historical_data (list): list of historical price data

        Yields:
            float: Valid close prices
        """
        for entry in historical_data:
            if entry.get("close") and str(entry["close"]).replace(".", "").isdigit():
                yield float(entry["close"])

    def calculate_volatility(self, historical_data: list[Dict]) -> float:
        """
        Calculate volatility using standard deviation of returns

        Args:
            historical_data (list): list of historical price data

        Returns:
            float: Volatility index
        """
        # Use generator to get prices
        prices = list(self.yield_price_data(historical_data))

        if len(prices) < 2:
            self.context.logger.info(
                f"Insufficient price data for volatility calculation. Data length: {len(prices)}"
            )
            return 0

        # Calculate returns using generator expression
        returns = [
            (prices[i + 1] - prices[i]) / prices[i] for i in range(len(prices) - 1)
        ]

        return np.std(returns) * np.sqrt(252)  # Annualized volatility

    def yield_volume_data(
        self, recent_data: list[Dict]
    ) -> Generator[float, None, None]:
        """
        Generator to yield valid volume data

        Args:
            recent_data (list): Recent trading data

        Yields:
            float: Valid trading volumes
        """
        for entry in recent_data:
            if entry.get("volume") and float(entry["volume"]) > 0:
                yield float(entry["volume"])

    def calculate_liquidity_score(self, recent_data: list[Dict]) -> float:
        """
        Calculate liquidity score based on trading volume

        Args:
            recent_data (list): Recent trading data

        Returns:
            float: Liquidity score
        """
        # Use generator to get volumes
        volumes = list(self.yield_volume_data(recent_data))

        if not volumes:
            self.context.logger.info("No valid trading volumes found")
            return 0

        # Normalize volume and create a composite liquidity score
        mean_volume = np.mean(volumes)
        median_volume = np.median(volumes)
        volume_std = np.std(volumes)

        # Liquidity score considers volume stability and magnitude
        liquidity_score = (mean_volume / (volume_std + 1)) * (1 + np.log(median_volume))

        return liquidity_score

    def generate_token_lists(
        self, data_sources: Dict[str, Any]
    ) -> Generator[Dict, None, None]:
        """
        Generate comprehensive token lists using generator

        Args:
            data_sources (dict): Dictionary of data sources

        Yields:
            dict: Token analysis for each coin
        """
        # self.context.logger.info available coins for debugging
        self.context.logger.info(
            f"Available coins in history: {list(data_sources['history'].keys())}"
        )

        # Iterate through cryptocurrencies
        for coin in data_sources["history"].keys():
            try:
                # Retrieve data for each data source with robust error handling
                history_data = data_sources["history"].get(coin, [])
                recent_data = (
                    data_sources["recent_time"].get(coin, {}).get("daily_data", [])
                )
                sentiment_data = data_sources["sentiment"].get(f"{coin}_posts", {})
                realtime_data = data_sources["realtime"].get(coin.upper(), {})

                # Try to get regulatory and news data safely
                regulatory_data = data_sources.get("regulatory", {}).get(coin, {})
                news_items = data_sources.get("news", {}).get(coin, [])

                # Calculate key metrics with error handling
                volatility = self.calculate_volatility(history_data)
                liquidity_score = self.calculate_liquidity_score(recent_data)

                # Calculate sentiment score
                sentiments = sentiment_data.get("vader_distribution", {})
                sentiment_score = (
                    sentiments.get("positive", 0) * 1.0
                    + sentiments.get("neutral", 0) * 0.0
                    - sentiments.get("negative", 0) * 1.0
                )

                # Classify tokens
                coin_analysis = {
                    "name": realtime_data.get("name", coin),
                    "symbol": coin.upper(),
                    "current_price": realtime_data.get("current_price", 0),
                    "volatility": volatility,
                    "liquidity_score": liquidity_score,
                    "sentiment_score": sentiment_score,
                    "market_cap_rank": realtime_data.get("market_cap_rank", 0),
                    "regulatory_stance": regulatory_data.get(
                        "crypto_stance", "Unknown"
                    ),
                }

                # Buy/Avoid Recommendations
                risk_score = (
                    volatility * -1  # Higher volatility is riskier
                    + sentiment_score  # Positive sentiment helps
                    + (liquidity_score / 10)  # Liquidity contributes positively
                )

                coin_analysis["recommendation"] = "Buy" if risk_score > 0 else "Avoid"

                # Add latest news if available
                if news_items:
                    coin_analysis["latest_news"] = news_items[0]

                yield coin_analysis

            except Exception as e:
                self.context.logger.info(f"Error processing coin {coin}: {e}")
                continue

    def organize_token_lists(
        self, token_generator: Iterator[Dict]
    ) -> Dict[str, list[Dict]]:
        """
        Organize token analysis into different lists

        Args:
            token_generator (iterator): Generator of token analysis

        Returns:
            dict: Organized token lists
        """
        token_lists = {
            "long_run_tokens": [],
            "short_run_tokens": [],
            "tokens_in_demand": [],
            "highlighted_tokens": [],
            "betting_tokens": [],
            "buy_avoid_recommendations": [],
        }

        for token in token_generator:
            # Long run tokens
            if (
                token["volatility"] < 0.3
                and token["sentiment_score"] > 0.5
                and token["liquidity_score"] > 5
            ):
                token_lists["long_run_tokens"].append(token)

            # Short run tokens
            if token["volatility"] > 0.5 and token["sentiment_score"] > 0.2:
                token_lists["short_run_tokens"].append(token)

            # Tokens in demand
            if token.get("total_volume", 0) > 1000000 and token.get("latest_news"):
                token_lists["tokens_in_demand"].append(token)

            # Highlighted tokens
            if token.get("latest_news"):
                token_lists["highlighted_tokens"].append(token)

            # Betting tokens
            if (
                token["volatility"] > 0.4
                and token["sentiment_score"] > 0.3
                and token["liquidity_score"] > 3
            ):
                token_lists["betting_tokens"].append(token)

            # Buy/Avoid recommendations
            token_lists["buy_avoid_recommendations"].append(token)

        return token_lists


class CryptoTxPreparationBehaviour(
    TokenDataCollectionBaseBehaviour
):  # pylint: disable=too-many-ancestors
    """
    Manages the preparation of cryptocurrency transactions, including native transfers, Crypto insight,
    and multisend transactions using a Safe contract. This behavior aligns with the CryptoTxPreparationRound to
    coordinate transaction preparation and execution with other agents in the network.

    Attributes:
        matching_round (Type[AbstractRound]): Specifies the ABcI round that this behaviour orchestrates,
                                              which is the CryptoTxPreparationRound.
    """

    matching_round: Type[AbstractRound] = CryptoTxPreparationRound

    def async_act(self) -> Generator:
        """
        Asynchronously perform transaction preparation actions. This method orchestrates the creation of different
        types of transactions based on the configuration and requirements of the current round, ensuring all transactions
        are ready for submission to the blockchain.

        The workflow includes:
        1. Fetching transaction data.
        2. Creating payloads based on transaction data.
        3. Sending transactions for consensus among agents.
        4. Handling transaction execution.
        """

        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():
                sender = self.context.agent_address
                tx_hash = yield from self.get_tx_hash()

                payload = CryptoTxPreparationPayload(
                    sender=sender,
                    tx_submitter=self.auto_behaviour_id(),
                    tx_hash=tx_hash,
                )

            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error during transaction preparation: {e}")
            self.context.logger.exception(traceback.format_exc())

    def get_tx_hash(self) -> Generator[None, None, Optional[str]]:
        """
        Prepare different types of transactions based on the scenario requirements, including native transfers,
        Crypto Insight, and multisend transactions. This method determines the type of transaction based on
        configurations and prepares the appropriate transaction hash.

        Returns:
            Optional[str]: The transaction hash of the prepared transaction, or None if an error occurred.
        """

        # Native transaction (Safe -> recipient)
        # self.context.logger.info("Preparing a native transaction")
        # tx_hash = yield from self.get_native_transfer_safe_tx_hash()
        # return tx_hash

        # smart contract transaction (Safe -> recipient)
        # self.context.logger.info("Preparing an Crypto Insight transaction")
        # tx_hash = yield from self.get_crypto_insight_safe_tx_hash()
        # return tx_hash

        # Multisend transaction (both native and Crypto Insight) (Safe -> recipient)
        self.context.logger.info("Preparing a multisend transaction")
        tx_hash = yield from self.get_multisend_safe_tx_hash()
        return tx_hash

    def get_crypto_insight_safe_tx_hash(self) -> Generator[None, None, Optional[str]]:
        """Prepare an crypto_insight safe transaction"""

        # Transaction data
        data_hex = yield from self.get_crypto_insight_transfer_data()

        # Check for errors
        if data_hex is None:
            return None

        # Prepare safe transaction
        safe_tx_hash = yield from self._build_safe_tx_hash(
            to_address=self.params.transfer_target_address, data=bytes.fromhex(data_hex)
        )

        self.context.logger.info(f"crypto insight transfer hash is {safe_tx_hash}")

        return safe_tx_hash

    def get_crypto_insight_transfer_data(self) -> Generator[None, None, Optional[str]]:
        """Get the crypto_insight transaction data"""

        self.context.logger.info("Preparing smart contract transfer transaction")

        # Use the contract api to interact with the Crypto Insight contract
        response_msg = yield from self.get_contract_api_response(
            performative=ContractApiMessage.Performative.GET_RAW_TRANSACTION,  # type: ignore
            contract_address=self.sanitize_hex_string(
                self.params.ipfs_storage_contract
            ),
            contract_id=str(IPFSDataStorage.contract_id),
            contract_callable="build_comprehensive_crypto_insights_list_tx",
            _ipfsHash="https://gateway.autonolas.tech/ipfs/QmakmAVwbMRBpqVjMKBsi4pKb6gcUUvVtGwPNzezvSkXjY/metadata.json",
            chain_id=GNOSIS_CHAIN_ID,
        )

        # Check that the response is what we expect
        if response_msg.performative != ContractApiMessage.Performative.RAW_TRANSACTION:
            self.context.logger.error(f"Error while retrieving : {response_msg}")
            return None

        data_bytes: Optional[bytes] = response_msg.raw_transaction.body.get(
            "data", None
        )

        # Ensure that the data is not None
        if data_bytes is None:
            self.context.logger.error(
                f"Error while preparing the transaction: {response_msg}"
            )
            return None

        data_hex = data_bytes.hex()
        self.context.logger.info(
            f"update Comprehensive Crypto Insights data is {data_hex}"
        )
        return data_hex

    def sanitize_hex_string(self, hex_string):
        """correct the address"""
        # Remove single quotes
        sanitized = hex_string.replace("'", "")
        # Ensure it starts with '0x'
        if not sanitized.startswith("0x"):
            sanitized = "0x" + sanitized
        return sanitized

    def get_native_transfer_safe_tx_hash(self) -> Generator[None, None, Optional[str]]:
        """Prepare a native safe transaction"""

        # Transaction data
        # This method is not a generator, therefore we don't use yield from
        data = self.get_native_transfer_data()

        # Prepare safe transaction
        safe_tx_hash = yield from self._build_safe_tx_hash(**data)
        self.context.logger.info(f"Native transfer hash is {safe_tx_hash}")

        return safe_tx_hash

    def get_native_transfer_data(self) -> Dict:
        """Get the native transaction data"""
        # Send 1 wei to the recipient
        data = {VALUE_KEY: 1, TO_ADDRESS_KEY: self.params.transfer_target_address}
        self.context.logger.info(f"Native transfer data is {data}")
        return data

    def get_multisend_safe_tx_hash(self) -> Generator[None, None, Optional[str]]:
        """
        Prepare and execute a multisend transaction that includes both native and crypto insight transfers.
        This method orchestrates the compilation of individual transactions into a multisend transaction.

        Returns:
            Optional[str]: The hash of the multisend transaction if successful, or None if an error occurred.
        """
        # Step 1: we prepare a list of transactions
        # Step 2: we pack all the transactions in a single one using the mulstisend contract
        # Step 3: we wrap the multisend call inside a Safe call, as always
        try:
            multi_send_txs = []

            # Native transfer
            native_transfer_data = self.get_native_transfer_data()
            multi_send_txs.append(
                {
                    "operation": MultiSendOperation.CALL,
                    "to": self.params.transfer_target_address,
                    "value": native_transfer_data[VALUE_KEY],
                    # No data key in this transaction, since it is a native transfer
                }
            )

            # crypto insight transfer
            crypto_insight_transfer_data_hex = (
                yield from self.get_crypto_insight_transfer_data()
            )

            if crypto_insight_transfer_data_hex is None:
                return None

            multi_send_txs.append(
                {
                    "operation": MultiSendOperation.CALL,
                    "to": self.sanitize_hex_string(self.params.ipfs_storage_contract),
                    "value": ZERO_VALUE,
                    "data": bytes.fromhex(crypto_insight_transfer_data_hex),
                }
            )

            # Multisend call
            contract_api_msg = yield from self.get_contract_api_response(
                performative=ContractApiMessage.Performative.GET_RAW_TRANSACTION,  # type: ignore
                contract_address="0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761",
                contract_id=str(MultiSendContract.contract_id),
                contract_callable="get_tx_data",
                multi_send_txs=multi_send_txs,
                chain_id=GNOSIS_CHAIN_ID,
            )

            # Check for errors
            if (
                contract_api_msg.performative
                != ContractApiMessage.Performative.RAW_TRANSACTION
            ):
                self.context.logger.error(
                    f"Could not get Multisend tx hash. "
                    f"Expected: {ContractApiMessage.Performative.RAW_TRANSACTION.value}, "
                    f"Actual: {contract_api_msg.performative.value}"
                )
                return None

            # Prepare the multisend transaction using the collected transaction data
            multisend_data = cast(str, contract_api_msg.raw_transaction.body["data"])[
                2:
            ]
            self.context.logger.info(f"Multisend data is {multisend_data}")

            # Prepare the Safe transaction
            safe_tx_hash = yield from self._build_safe_tx_hash(
                to_address="0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761",
                value=ZERO_VALUE,  # the safe is not moving any native value into the multisend
                data=bytes.fromhex(multisend_data),
                operation=SafeOperation.DELEGATE_CALL.value,  # we are delegating the call to the multisend contract
            )
            return safe_tx_hash

        except Exception as e:
            self.context.logger.error(
                f"Failed to prepare multisend transaction: {str(e)}"
            )
            return None

    def _build_safe_tx_hash(
        self,
        to_address: str,
        value: int = ZERO_VALUE,
        data: bytes = EMPTY_CALL_DATA,
        operation: int = SafeOperation.CALL.value,
    ) -> Generator[None, None, Optional[str]]:
        """
        Build and return the hash for a Safe transaction that will be executed using the Gnosis Safe contract.

        Args:
            to_address (str): The address of the contract or recipient in the transaction.
            data (bytes): The data to be sent in the transaction.
            operation (int): The operation type (CALL, DELEGATE_CALL, etc.) of the transaction.

        Returns:
            Optional[str]: The hash of the Safe transaction if successful, or None if an error occurred.
        """

        self.context.logger.info(
            f"Preparing Safe transaction [{self.synchronized_data.safe_contract_address}]"
        )

        # Prepare the safe transaction
        response_msg = yield from self.get_contract_api_response(
            performative=ContractApiMessage.Performative.GET_STATE,  # type: ignore
            contract_address=self.synchronized_data.safe_contract_address,
            contract_id=str(GnosisSafeContract.contract_id),
            contract_callable="get_raw_safe_transaction_hash",
            to_address=to_address,
            value=value,
            data=data,
            safe_tx_gas=SAFE_GAS,
            chain_id=GNOSIS_CHAIN_ID,
            operation=operation,
        )

        # Check for errors
        if response_msg.performative != ContractApiMessage.Performative.STATE:
            self.context.logger.error(
                "Couldn't get safe tx hash. Expected response performative "
                f"{ContractApiMessage.Performative.STATE.value!r}, "  # type: ignore
                f"received {response_msg.performative.value!r}: {response_msg}."
            )
            return None

        # Extract the hash and check it has the correct length
        tx_hash: Optional[str] = response_msg.state.body.get("tx_hash", None)

        if tx_hash is None or len(tx_hash) != TX_HASH_LENGTH:
            self.context.logger.error(
                "Something went wrong while trying to get the safe transaction hash. "
                f"Invalid hash {tx_hash!r} was returned."
            )
            return None

        # Transaction to hex
        tx_hash = tx_hash[2:]  # strip the 0x

        safe_tx_hash = hash_payload_to_hex(
            safe_tx_hash=tx_hash,
            ether_value=value,
            safe_tx_gas=SAFE_GAS,
            to_address=to_address,
            data=data,
            operation=operation,
        )

        self.context.logger.info(f"Safe transaction hash is {safe_tx_hash}")

        return safe_tx_hash


class TokenDataCollectionRoundBehaviour(AbstractRoundBehaviour):
    """This behaviour manages the consensus stages."""

    initial_behaviour_cls = TopCryptoListDataCollectionBehaviour
    abci_app_cls = TokenDataCollectionAbciApp  # type: ignore
    behaviours: Set[Type[BaseBehaviour]] = [  # type: ignore
        HistoricalDataCollectionBehaviour,
        ComprehensiveCryptoInsightsBehaviour,
        CryptoTxPreparationBehaviour,
        RealTimeDataStreamingBehaviour,
        RecentDataCollectionBehaviour,
        TopCryptoListDataCollectionBehaviour,
    ]
