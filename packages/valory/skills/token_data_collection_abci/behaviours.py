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
from typing import Any, Dict, Generator, Optional, Set, Type, cast
import traceback
import pandas as pd
from datetime import datetime, timedelta 

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
    
    def current_round_id(self) -> str:

        id = cast(
            SharedState, self.context.state
        ).round_sequence.current_round.auto_round_id()

        return id


class TopCryptoListDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):   # pylint: disable=too-many-ancestors
    """
    Behaviour for collecting and storing top cryptocurrency list.
    
    Responsibilities:
    - Fetch top cryptocurrencies from CoinGecko API
    - Store the list in IPFS
    - Share the list with other agents
    """

    matching_round: Type[AbstractRound] = TopCryptoListDataCollectionRound
    
    def async_act(self) -> Generator:
        """
        Perform the async action of collecting and sharing top crypto list.
        
        Steps:
        1. Fetch top cryptocurrencies
        2. Store list in IPFS
        3. Create and send payload for consensus
        """
        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():

                sender = self.context.agent_address
                self.sleep(10)

                self.context.logger.info(f"self.synchronized_data.top_crypto_currencies_ipfs_hash: {self.current_round_id()}")

                top_crypto_currencies = yield from self.get_top_crypto_list_specs()
                
                # If no cryptocurrencies fetched after all retries
                if not top_crypto_currencies:
                    self.context.logger.warning("No cryptocurrencies fetched after all retries. Setting done.")
                    self.set_done()
                    return

                # Store the top crypto token list in IPFS
                try:
                    top_crypto_currencies_ipfs_hash = yield from self.send_top_crypto_to_ipfs(top_crypto_currencies)
                except Exception as ipfs_error:
                    self.context.logger.error(f"IPFS storage failed: {ipfs_error}")
                    return

                # Prepare the payload to be shared with other agents
                payload = TopCryptoListDataCollectionPayload(
                    sender=sender,
                    top_crypto_currencies=top_crypto_currencies,
                    top_crypto_currencies_ipfs_hash=top_crypto_currencies_ipfs_hash,
                )
        
            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()
            
            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Critical error in async_act: {e}")
            self.context.logger.exception(traceback.format_exc())
            self.set_done() # Directly set done in case of any unhandled exception
    
    def get_top_crypto_list_specs(self) -> Generator[None, None, Optional[str]]:
        """
        Fetch top cryptocurrencies from CoinGecko API with retry mechanism.
        
        Returns:
            Optional[str]: Comma-separated list of top cryptocurrency IDs
        """
        max_retries = 4  # Total attempts including the initial try
        
        for attempt in range(max_retries):
            try:
                self.context.logger.info(f"Initiating top cryptocurrency list fetch (Attempt {attempt + 1})")
                
                # Validate API specs
                if not hasattr(self, 'coingecko_top_cryptocurrencies_specs'):
                    raise ValueError("CoinGecko API specs are not configured")
    
                # Get the specs
                specs = self.coingecko_top_cryptocurrencies_specs.get_spec()
        
                # Make the HTTP call
                raw_response = yield from self.get_http_response(**specs)
                
                # Validate HTTP response
                if raw_response.status_code != HTTP_OK:
                    error_msg = f"API request failed with status {raw_response.status_code}"
                    self.context.logger.error(error_msg)
                    self.context.logger.error(f"Response body: {raw_response.body}")
                    
                    # If it's a rate limit error, wait and retry
                    if raw_response.status_code == 429 and attempt < max_retries - 1:
                        wait_time = (2 ** attempt)  # Exponential backoff
                        self.context.logger.info(f"Rate limited. Waiting {wait_time} seconds before retry.")
                        yield from self.sleep(wait_time)
                        continue
                    
                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        self.context.logger.error("Max retries reached. Unable to fetch cryptocurrencies.")
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
                    yield from self.sleep(2 ** attempt)
                    continue
    
                # Validate response structure
                if not isinstance(response, list) or len(response) == 0:
                    self.context.logger.warning("Received empty or invalid cryptocurrency list")
                    
                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None
                    
                    # Wait and retry
                    yield from self.sleep(2 ** attempt)
                    continue
        
                # Extract top cryptocurrencies
                try:
                    top_crypto_currencies = ", ".join([
                        crypto.get("id", "") 
                        for crypto in response 
                        if crypto.get("id")
                    ])
                except Exception as extraction_error:
                    self.context.logger.error(f"Error extracting cryptocurrency IDs: {extraction_error}")
                    
                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None
                    
                    # Wait and retry
                    yield from self.sleep(2 ** attempt)
                    continue
    
                # Log and return if successful
                if not top_crypto_currencies:
                    self.context.logger.warning("No valid cryptocurrency IDs found")
                    
                    # If it's the last attempt, return None
                    if attempt == max_retries - 1:
                        return None
                    
                    # Wait and retry
                    yield from self.sleep(2 ** attempt)
                    continue
    
                self.context.logger.info(f"Top Cryptocurrencies: {top_crypto_currencies}")
                return top_crypto_currencies
            
            except Exception as e:
                self.context.logger.error(f"Unexpected error in get_top_crypto_list_specs (Attempt {attempt + 1}): {e}")
                
                # If it's the last attempt, log and exit
                if attempt == max_retries - 1:
                    self.context.logger.exception(traceback.format_exc())
                    return None
                
                # Wait before retrying
                yield from self.sleep(2 ** attempt)
        
        # If all retries fail
        self.context.logger.error("Failed to fetch top cryptocurrencies after all retries")
        return None

    def send_top_crypto_to_ipfs(self, top_crypto_currencies: str) -> Generator[None, None, Optional[str]]:
        """
        Store the top cryptocurrencies list in IPFS.
        
        Args:
            top_crypto_currencies (str): Comma-separated list of cryptocurrency IDs
        
        Returns:
            Optional[str]: IPFS hash of stored data
        
        Raises:
            Exception: For any errors during IPFS storage
        """
        try:
            # Validate input
            if not top_crypto_currencies:
                raise ValueError("Empty cryptocurrency list cannot be stored")

            # Prepare data for IPFS storage
            data = {"top_crypto_currencies": top_crypto_currencies}
            
            # Validate metadata filepath
            if not hasattr(self, 'metadata_filepath'):
                raise AttributeError("Metadata filepath is not configured")

            # Store in IPFS
            top_crypto_currencies_ipfs_hash = yield from self.send_to_ipfs(
                filename=self.metadata_filepath, 
                obj=data, 
                filetype=SupportedFiletype.JSON
            )

            # Log successful storage
            self.context.logger.info(
                f"Top cryptocurrencies stored in IPFS: https://gateway.autonolas.tech/ipfs/{top_crypto_currencies_ipfs_hash}"
            )

            return top_crypto_currencies_ipfs_hash

        except Exception as e:
            self.context.logger.error(f"Error storing top cryptocurrencies in IPFS: {e}")
            self.context.logger.exception(traceback.format_exc())
            raise


class HistoricalDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):
    """HistoricalDataCollectionBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound] = HistoricalDataCollectionRound

    def async_act(self) -> Generator:
        """Do the action."""
        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():

                sender = self.context.agent_address
                # Fetch top cryptocurrencies
                top_crypto_currencies = yield from self.get_top_crypto_currencies_from_ipfs()
                
                # Log the fetched cryptocurrencies
                self.context.logger.info(f"Fetched top cryptocurrencies: {top_crypto_currencies}")
                
                # Fetch OHLC data for the cryptocurrencies
                all_crypto_ohlc_market_data = yield from self.aggregate_crypto_data(top_crypto_currencies)
                
                # Log the fetched OHLC data
                self.context.logger.info(f"Fetched OHLC and Market data for {len(all_crypto_ohlc_market_data)} cryptocurrencies")
                
                # Store the top crypto token list in IPFS
                ohlc_market_data_ipfs_hash = yield from self.send_ohlc_market_data_ipfs(all_crypto_ohlc_market_data)
            
                # Prepare payload
                payload = HistoricalDataCollectionPayload(
                    sender = sender, 
                    ohlc_market_data_ipfs_hash=ohlc_market_data_ipfs_hash   
                )
            
            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in historical data collection: {e}")
            self.context.logger.exception(traceback.format_exc())
    

    def get_top_crypto_currencies_from_ipfs(self) -> Generator[None, None, Optional[list[str]]]:
        """
        Load the top crypto currencies data from IPFS.
        
        Returns:
            A list of top cryptocurrency identifiers.
        
        Raises:
            ValueError: If unable to retrieve or parse cryptocurrency data.
        """
        try:
            self.context.logger.info("Attempting to retrieve top cryptocurrencies from IPFS")

            # Validate IPFS hash
            if not hasattr(self.synchronized_data, 'top_crypto_currencies_ipfs_hash'):
                raise ValueError("IPFS hash for top cryptocurrencies is not available")

            ipfs_hash = self.synchronized_data.top_crypto_currencies_ipfs_hash
            self.context.logger.info(f"IPFS hash retrieved: {ipfs_hash}")

            # Fetch data from IPFS
            top_crypto_currencies = yield from self.get_from_ipfs(
                ipfs_hash=ipfs_hash, 
                filetype=SupportedFiletype.JSON
            )

            # Validate retrieved data
            if not top_crypto_currencies or 'top_crypto_currencies' not in top_crypto_currencies:
                raise ValueError("No cryptocurrency data found in IPFS")

            # Parse and return cryptocurrency list
            crypto_list = top_crypto_currencies.get('top_crypto_currencies', '').split(', ')
            
            if not crypto_list:
                raise ValueError("Empty cryptocurrency list")

            return crypto_list

        except Exception as e:
            self.context.logger.error(f"Error retrieving top cryptocurrencies from IPFS: {e}")
            self.context.logger.exception(traceback.format_exc())
            raise

    def process_crypto_data(
    self, 
    ohlc_data: list[list[float]], 
    market_data: Dict[str, list[list[float]]]) -> Dict[str, list[Dict[str, Any]]]:
        """
        Process cryptocurrency OHLC and market data into a structured format.
        
        Args:
            ohlc_data (List[List[float]]): OHLC data from CoinGecko API
            market_data (Dict[str, List[List[float]]]): Market data including volumes and market caps
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Processed cryptocurrency data
        """
        try:
            # Validate input data
            if not ohlc_data or not market_data:
                raise ValueError("Empty OHLC or market data provided")
    
            # Create DataFrame from OHLC data
            try:
                ohlc_df = pd.DataFrame(ohlc_data, columns=['timestamp', 'open', 'high', 'low', 'close'])
                
                # Convert timestamp to datetime
                ohlc_df['timestamp'] = pd.to_datetime(ohlc_df['timestamp'], unit='ms')
                ohlc_df.set_index('timestamp', inplace=True)
            
            except Exception as e:
                self.context.logger.error(f"Error creating OHLC DataFrame: {e}")
                raise
    
            # Process volume data
            try:
                volume_df = pd.DataFrame(market_data.get('total_volumes', []), columns=['timestamp', 'volume'])
                volume_df['timestamp'] = pd.to_datetime(volume_df['timestamp'], unit='ms')
                volume_df.set_index('timestamp', inplace=True)
            
            except Exception as e:
                self.context.logger.error(f"Error processing volume data: {e}")
                raise
    
            # Process market cap data
            try:
                market_cap_df = pd.DataFrame(market_data.get('market_caps', []), columns=['timestamp', 'market_cap'])
                market_cap_df['timestamp'] = pd.to_datetime(market_cap_df['timestamp'], unit='ms')
                market_cap_df.set_index('timestamp', inplace=True)
            
            except Exception as e:
                self.context.logger.error(f"Error processing market cap data: {e}")
                raise
    
            # Combine dataframes
            try:
                crypto_df = pd.concat([
                    ohlc_df, 
                    volume_df['volume'], 
                    market_cap_df['market_cap']
                ], axis=1)
                
                # Fill any NaN values to ensure data consistency
                crypto_df.fillna(method='ffill', inplace=True)
            
            except Exception as e:
                self.context.logger.error(f"Error combining dataframes: {e}")
                raise
    
            # Log data summary
            self.context.logger.info("Cryptocurrency Data Summary:")
            self.context.logger.info(f"  Date range: {crypto_df.index.min()} to {crypto_df.index.max()}")
            self.context.logger.info(f"  Total data points: {len(crypto_df)}")
            
            # Convert to list of dictionaries for serialization
            try:
                crypto_data = crypto_df.reset_index().to_dict(orient='records')
                return crypto_data
            
            except Exception as e:
                self.context.logger.error(f"Error converting DataFrame to dictionary: {e}")
                raise
    
        except Exception as e:
            self.context.logger.error(f"Comprehensive error in data processing: {e}")
            self.context.logger.exception(traceback.format_exc())
            return []

    def aggregate_crypto_data(
        self, 
        crypto_ids: list[str]) -> Generator[None, None, Dict[str, list[Dict[str, Any]]]]:
            """
            Aggregate cryptocurrency data for multiple cryptocurrencies.
            
            Args:
                crypto_ids (List[str]): List of cryptocurrency identifiers
            
            Returns:
                Dict[str, List[Dict[str, Any]]]: Aggregated cryptocurrency data
            """
            all_crypto_data = {}
            failed_cryptocurrencies = []
        
            for crypto_id in crypto_ids:
                try:
                    # Fetch OHLC data
                    ohlc_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/ohlc?vs_currency=usd&days=365"
                    ohlc_response = yield from self.get_http_response(method="GET", url=ohlc_url)
                    
                    if ohlc_response.status_code != HTTP_OK:
                        self.context.logger.error(f"OHLC data fetch failed for {crypto_id}: {ohlc_response.status_code}")
                        failed_cryptocurrencies.append(crypto_id)
                        continue
                    
                     # Respect API rate limits
                    self.sleep(3)
        
                    # Fetch market data
                    market_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=365"
                    market_response = yield from self.get_http_response(method="GET", url=market_url)
                    
                    if market_response.status_code != HTTP_OK:
                        self.context.logger.error(f"Market data fetch failed for {crypto_id}: {market_response.status_code}")
                        failed_cryptocurrencies.append(crypto_id)
                        continue
        
                    # Parse responses
                    ohlc_data = ohlc_response.json()
                    market_data = market_response.json()
        
                    # Process data
                    processed_data = yield from self.process_crypto_data(ohlc_data, market_data)
                    
                    if processed_data:
                        all_crypto_data[crypto_id] = processed_data
                        self.context.logger.info(f"Successfully processed data for {crypto_id}")
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
                self.context.logger.warning(f"Failed to fetch data for cryptocurrencies: {failed_cryptocurrencies}")
        
            return all_crypto_data
    
    def send_ohlc_market_data_ipfs(self, all_crypto_ohlc_market_data) -> Generator[None, None, Optional[str]]:
        """Store the token price in IPFS"""
        data = {"all_crypto_ohlc_market_data": all_crypto_ohlc_market_data}
        ohlc_market_data_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, obj=data, filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Price data stored in IPFS: https://gateway.autonolas.tech/ipfs/{ohlc_market_data_ipfs_hash}"
        )
        return ohlc_market_data_ipfs_hash


class RecentDataCollectionBehaviour(TokenDataCollectionBaseBehaviour):
    """RecentDataCollectionBehaviour behaviour implementation."""

    matching_round: Type[AbstractRound] = RecentDataCollectionRound

    def async_act(self) -> Generator:
        """Do the action."""
        try:
            with self.context.benchmark_tool.measure(self.behaviour_id).local():
                sender = self.context.agent_address
                
                # Fetch top cryptocurrencies
                top_crypto_currencies = yield from self.get_top_crypto_currencies_from_ipfs()
                
                # Log the fetched cryptocurrencies
                self.context.logger.info(f"Fetched top cryptocurrencies: {top_crypto_currencies}")
                
                # Fetch recent crypto data
                all_crypto_recent_data = yield from self.aggregate_recent_crypto_data(top_crypto_currencies)
                
                # Log the fetched recent data
                self.context.logger.info(f"Fetched recent data for {len(all_crypto_recent_data)} cryptocurrencies")
                
                # Store the recent data in IPFS
                recent_data_ipfs_hash = yield from self.send_recent_data_to_ipfs(all_crypto_recent_data)
            
                # Prepare payload
                payload = RecentDataCollectionPayload(
                    sender=sender, 
                    recent_data_ipfs_hash=recent_data_ipfs_hash   
                )
            
            with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
                yield from self.send_a2a_transaction(payload)
                yield from self.wait_until_round_end()

            self.set_done()

        except Exception as e:
            self.context.logger.error(f"Error in recent data collection: {e}")
            self.context.logger.exception(traceback.format_exc())
    
    def get_top_crypto_currencies_from_ipfs(self) -> Generator[None, None, Optional[list[str]]]:
        """
        Load the top crypto currencies data from IPFS.
        
        Returns:
            A list of top cryptocurrency identifiers.
        
        Raises:
            ValueError: If unable to retrieve or parse cryptocurrency data.
        """
        try:
            self.context.logger.info("Attempting to retrieve top cryptocurrencies from IPFS")

            # Validate IPFS hash
            if not hasattr(self.synchronized_data, 'top_crypto_currencies_ipfs_hash'):
                raise ValueError("IPFS hash for top cryptocurrencies is not available")

            ipfs_hash = self.synchronized_data.top_crypto_currencies_ipfs_hash
            self.context.logger.info(f"IPFS hash retrieved: {ipfs_hash}")

            # Fetch data from IPFS
            top_crypto_currencies = yield from self.get_from_ipfs(
                ipfs_hash=ipfs_hash, 
                filetype=SupportedFiletype.JSON
            )

            # Validate retrieved data
            if not top_crypto_currencies or 'top_crypto_currencies' not in top_crypto_currencies:
                raise ValueError("No cryptocurrency data found in IPFS")

            # Parse and return cryptocurrency list
            crypto_list = top_crypto_currencies.get('top_crypto_currencies', '').split(', ')
            
            if not crypto_list:
                raise ValueError("Empty cryptocurrency list")

            return crypto_list

        except Exception as e:
            self.context.logger.error(f"Error retrieving top cryptocurrencies from IPFS: {e}")
            self.context.logger.exception(traceback.format_exc())
            raise

    def aggregate_recent_crypto_data(
        self, 
        crypto_ids: list[str]) -> Generator[None, None, Dict[str, list[Dict[str, Any]]]]:
        """
        Aggregate recent cryptocurrency data for multiple cryptocurrencies.
        
        Args:
            crypto_ids (List[str]): List of cryptocurrency identifiers
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Aggregated recent cryptocurrency data
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
                    'daily_data': daily_data,
                    'weekly_data': weekly_data,
                    'monthly_data': monthly_data,
                    'market_details': market_details
                }
                
                if processed_data:
                    all_crypto_recent_data[crypto_id] = processed_data
                    self.context.logger.info(f"Successfully processed recent data for {crypto_id}")
                else:
                    failed_cryptocurrencies.append(crypto_id)
                
                # Respect API rate limits
                self.sleep(3)
        
            except Exception as e:
                self.context.logger.error(f"Error processing recent data for {crypto_id}: {e}")
                self.context.logger.exception(traceback.format_exc())
                failed_cryptocurrencies.append(crypto_id)
        
        # Log summary of data collection
        if failed_cryptocurrencies:
            self.context.logger.warning(f"Failed to fetch recent data for cryptocurrencies: {failed_cryptocurrencies}")
        
        return all_crypto_recent_data

    def fetch_daily_data(self, crypto_id: str) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch daily data for the last 30 days.
        
        Args:
            crypto_id (str): Cryptocurrency identifier
        
        Returns:
            List[Dict[str, Any]]: Daily price and market data
        """
        try:
            daily_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=30"
            daily_response = yield from self.get_http_response(method="GET", url=daily_url)
            
            if daily_response.status_code != HTTP_OK:
                self.context.logger.error(f"Daily data fetch failed for {crypto_id}: {daily_response.status_code}")
                return []
            
            daily_data = daily_response.json()
            processed_daily_data = self.process_market_chart_data(daily_data)
            
            return processed_daily_data
        
        except Exception as e:
            self.context.logger.error(f"Error fetching daily data for {crypto_id}: {e}")
            return []

    def fetch_weekly_data(self, crypto_id: str) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch weekly data for the last 6 months.
        
        Args:
            crypto_id (str): Cryptocurrency identifier
        
        Returns:
            List[Dict[str, Any]]: Weekly price and market data
        """
        try:
            weekly_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=180"
            
            
           
            weekly_response = yield from self.get_http_response(method="GET", url=weekly_url)
            
            if weekly_response.status_code != HTTP_OK:
                self.context.logger.error(f"Weekly data fetch failed for {crypto_id}: {weekly_response.status_code}")
                return []
            
            weekly_data = weekly_response.json()
            processed_weekly_data = self.process_market_chart_data(weekly_data, resample='W')
            
            return processed_weekly_data
        
        except Exception as e:
            self.context.logger.error(f"Error fetching weekly data for {crypto_id}: {e}")
            return []

    def fetch_monthly_data(self, crypto_id: str) -> Generator[None, None, list[Dict[str, Any]]]:
        """
        Fetch monthly data for the last year.
        
        Args:
            crypto_id (str): Cryptocurrency identifier
        
        Returns:
            List[Dict[str, Any]]: Monthly price and market data
        """
        try:
            monthly_url = f"{self.params.coingecko_histroy_data_template}{crypto_id}/market_chart?vs_currency=usd&days=365"
            monthly_response = yield from self.get_http_response(method="GET", url=monthly_url)
            
            if monthly_response.status_code != HTTP_OK:
                self.context.logger.error(f"Monthly data fetch failed for {crypto_id}: {monthly_response.status_code}")
                return []
            
            monthly_data = monthly_response.json()
            processed_monthly_data = self.process_market_chart_data(monthly_data, resample='M')
            
            return processed_monthly_data
        
        except Exception as e:
            self.context.logger.error(f"Error fetching monthly data for {crypto_id}: {e}")
            return []

    def fetch_market_details(self, crypto_id: str) -> Generator[None, None, Dict[str, Any]]:
        """
        Fetch additional market details including liquidity and significant events.
        
        Args:
            crypto_id (str): Cryptocurrency identifier
        
        Returns:
            Dict[str, Any]: Market details
        """
        try:
            # Fetch detailed market information
            market_details_url = f"{self.params.coingecko_histroy_data_template}/coins/{crypto_id}"
            market_details_response = yield from self.get_http_response(method="GET", url=market_details_url)
            
            if market_details_response.status_code != HTTP_OK:
                self.context.logger.error(f"Market details fetch failed for {crypto_id}: {market_details_response.status_code}")
                return {}
            
            market_details = market_details_response.json()
            
            # Extract relevant market information
            processed_details = {
                'market_cap_rank': market_details.get('market_cap_rank'),
                'current_price': market_details.get('market_data', {}).get('current_price', {}).get('usd'),
                'total_volume': market_details.get('market_data', {}).get('total_volume', {}).get('usd'),
                'liquidity_score': market_details.get('liquidity_score'),
                'significant_events': self.extract_significant_events(market_details)
            }
            
            return processed_details
        
        except Exception as e:
            self.context.logger.error(f"Error fetching market details for {crypto_id}: {e}")
            return {}

    def extract_significant_events(self, market_details: Dict[str, Any]) -> list[Dict[str, Any]]:
        """
        Extract significant market events from market details.
        
        Args:
            market_details (Dict[str, Any]): Detailed market information
        
        Returns:
            List[Dict[str, Any]]: List of significant events
        """
        # This is a placeholder method. In a real implementation, 
        # parse through the market details to extract 
        # significant events like forks, major updates, etc.
        events = []
        
        # Example of how you might extract events
        if market_details.get('categories'):
            events.append({
                'type': 'category_update',
                'details': market_details['categories']
            })
        
        return events

    def process_market_chart_data(
        self, 
        market_chart_data: Dict[str, list[list[float]]], 
        resample: str = None
    ) -> list[Dict[str, Any]]:
        """
        Process market chart data with optional resampling.
        
        Args:
            market_chart_data (Dict[str, List[List[float]]]): Market chart data
            resample (str, optional): Resampling frequency
        
        Returns:
            List[Dict[str, Any]]: Processed market data
        """
        try:
            # Create DataFrames for prices, volumes, and market caps
            prices_df = pd.DataFrame(market_chart_data.get('prices', []), columns=['timestamp', 'price'])
            volumes_df = pd.DataFrame(market_chart_data.get('total_volumes', []), columns=['timestamp', 'volume'])
            market_caps_df = pd.DataFrame(market_chart_data.get('market_caps', []), columns=['timestamp', 'market_cap'])
            
            # Convert timestamps
            for df in [prices_df, volumes_df, market_caps_df]:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
            
            # Optional resampling
            if resample:
                prices_df = prices_df.resample(resample).mean()
                volumes_df = volumes_df.resample(resample).sum()
                market_caps_df = market_caps_df.resample(resample).mean()
            
            # Combine data
            combined_df = pd.concat([prices_df, volumes_df, market_caps_df], axis=1)
            combined_df.reset_index(inplace=True)
            
            # Convert to list of dictionaries
            processed_data = combined_df.to_dict(orient='records')
            
            return processed_data
        
        except Exception as e:
            self.context.logger.error(f"Error processing market chart data: {e}")
            return []

    def send_recent_data_to_ipfs(self, all_crypto_recent_data) -> Generator[None, None, Optional[str]]:
        """
        Store the recent cryptocurrency data in IPFS.
        
        Args:
            all_crypto_recent_data (Dict): Cryptocurrency recent data
        
        Returns:
            Optional[str]: IPFS hash of stored data
        """
        data = {"all_crypto_recent_data": all_crypto_recent_data}
        recent_data_ipfs_hash = yield from self.send_to_ipfs(
            filename=self.metadata_filepath, 
            obj=data, 
            filetype=SupportedFiletype.JSON
        )
        self.context.logger.info(
            f"Recent data stored in IPFS: https://gateway.autonolas.tech/ipfs/{recent_data_ipfs_hash}"
        )
        return recent_data_ipfs_hash


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
