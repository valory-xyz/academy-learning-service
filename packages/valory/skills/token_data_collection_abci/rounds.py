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

"""This module contains the rounds definitions for token_data_collection_abci."""
import typing as t

from enum import Enum
from typing import Dict, FrozenSet, Optional, Set, Tuple

from packages.valory.skills.abstract_round_abci.base import (
    AbciApp,
    AbciAppTransitionFunction,
    AppState,
    BaseSynchronizedData,
    CollectSameUntilThresholdRound,
    CollectDifferentUntilThresholdRound,
    CollectionRound,
    DegenerateRound,
    DeserializedCollection,
    EventToTimeout,
    get_name,
)
from packages.valory.skills.token_data_collection_abci.payloads import (
    CryptoTxPreparationPayload,
    ComprehensiveCryptoInsightsPayload,
    HistoricalDataCollectionPayload,
    RealTimeDataStreamingPayload,
    RecentDataCollectionPayload,
    TopCryptoListDataCollectionPayload,
)


class Event(Enum):
    """Event enum for state transitions."""

    DONE = "done"
    ERROR = "error"
    NO_MAJORITY = "no_majority"
    ROUND_TIMEOUT = "round_timeout"


class SynchronizedData(BaseSynchronizedData):
    """
    Provides synchronized access to blockchain data related to cryptocurrency transactions.
    This class inherits from BaseSynchronizedData to utilize core synchronization functionality.
    """

    def _get_deserialized(self, key: str) -> DeserializedCollection:
        """
        Retrieve and deserialize a collection from the database using a specific key.

        Parameters:
            key (str): The key associated with the collection to retrieve and deserialize.

        Returns:
            DeserializedCollection: The collection after deserialization.
        """
        serialized = self.db.get_strict(key)
        return CollectionRound.deserialize_collection(serialized)

    @property
    def top_crypto_currencies(self) -> Optional[str]:
        """Return the cached list of top cryptocurrencies if available."""
        return self.db.get("top_crypto_currencies", None)

    @property
    def top_crypto_currencies_ipfs_hash(self) -> Optional[str]:
        """Return the IPFS hash of the top cryptocurrencies data."""
        return self.db.get("top_crypto_currencies_ipfs_hash", None)

    @property
    def participant_to_data_round(self) -> DeserializedCollection:
        """Return the mapping of participants to their corresponding data rounds."""
        return self._get_deserialized("participant_to_data_round")

    @property
    def ohlc_market_data_ipfs_hash(self) -> Optional[str]:
        """Return the IPFS hash for OHLC (Open, High, Low, Close) market data."""
        return self.db.get("ohlc_market_data_ipfs_hash", None)

    @property
    def participant_to_historical_round(self) -> DeserializedCollection:
        """Return the mapping of participants to their historical data rounds."""
        return self._get_deserialized("participant_to_historical_round")

    @property
    def recent_data_ipfs_hash(self) -> Optional[str]:
        """Return the IPFS hash for the most recent cryptocurrency data."""
        return self.db.get("recent_data_ipfs_hash", None)

    @property
    def participant_to_recent_data_round(self) -> DeserializedCollection:
        """Return the mapping of participants to their recent data rounds."""
        return self._get_deserialized("participant_to_recent_data_round")

    @property
    def content(self) -> str:
        """Return generic content related to cryptocurrencies."""
        return self.db.get("content", None)

    @property
    def participant_to_real_time_data_round(self) -> DeserializedCollection:
        """Return the mapping of participants to their real-time data rounds."""
        return self._get_deserialized("participant_to_real_time_data_round")

    @property
    def comprehensive_crypto_currencies_insights(self) -> Optional[str]:
        """Return the comprehensive insights about cryptocurrencies."""
        return self.db.get("comprehensive_crypto_currencies_insights", None)

    @property
    def comprehensive_crypto_currencies_insights_ipfs_hash(self) -> Optional[str]:
        """Return the IPFS hash for comprehensive insights on cryptocurrencies."""
        return self.db.get("comprehensive_crypto_currencies_insights_ipfs_hash", None)

    @property
    def participant_to_comprehensive_crypto_currencies_insights_data_round(
        self,
    ) -> DeserializedCollection:
        """Return the mapping of participants to their rounds providing comprehensive crypto insights."""
        return self._get_deserialized(
            "participant_to_comprehensive_crypto_currencies_insights_data_round"
        )

    @property
    def most_voted_tx_hash(self) -> Optional[float]:
        """Return the transaction hash with the highest number of votes."""
        return self.db.get("most_voted_tx_hash", None)

    @property
    def participant_to_tx_round(self) -> DeserializedCollection:
        """Return the mapping of participants to their transaction rounds."""
        return self._get_deserialized("participant_to_tx_round")

    @property
    def tx_submitter(self) -> str:
        """Return the identifier of the participant who submitted a transaction to the settlement system."""
        return str(self.db.get_strict("tx_submitter"))


class TopCryptoListDataCollectionRound(CollectSameUntilThresholdRound):
    """
    Defines a data collection round specifically for gathering top cryptocurrency data.
    This class inherits from CollectSameUntilThresholdRound to handle the collection and synchronization
    of data across different agents until a predefined threshold is met.

    Attributes:
        payload_class (class): Specifies the type of payload this round will handle, defined by TopCryptoListDataCollectionPayload.
        synchronized_data_class (class): Specifies the class for synchronized data operations, facilitating data handling and storage.
        done_event (Event): Event to signal the completion of the data collection round.
        collection_key (str): Key under which the agent-to-payload mappings are stored in synchronized data.
        selection_key (tuple): Keys used to extract data from each agent's payload and determine where it should be stored in synchronized data.
    """

    payload_class = TopCryptoListDataCollectionPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # Collection key specifies where in the synchronized data the agento to payload mapping will be stored
    collection_key = get_name(SynchronizedData.participant_to_data_round)

    # Selection key specifies how to extract all the different objects from each agent's payload
    # and where to store it in the synchronized data. Notice that the order follows the same order
    # from the payload class.
    selection_key = (
        get_name(SynchronizedData.top_crypto_currencies),
        get_name(SynchronizedData.top_crypto_currencies_ipfs_hash),
    )


class HistoricalDataCollectionRound(CollectDifferentUntilThresholdRound):
    """
    Implementation of a data collection round tailored for gathering historical cryptocurrency market data.
    This round inherits from CollectDifferentUntilThresholdRound, which allows it to handle the aggregation
    and synchronization of varying types of data from different agents until a set threshold is reached.

    Attributes:
        payload_class (class): Defines the specific type of payload this round will process, as specified by
                               HistoricalDataCollectionPayload.
        synchronized_data_class (class): Indicates the class used for synchronized data management and operations.
        done_event (Event): Event to indicate the completion of the data collection round.
        collection_key (str): Key for storing the mapping of agents to their payloads in the synchronized data.
        selection_key (tuple): Specifies the keys for extracting OHLC (Open, High, Low, Close) market data hashes
                               from each agent's payload and determining where this data is stored in the synchronized data.
    """

    payload_class = HistoricalDataCollectionPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # This key is used to access the part of the synchronized data where agent-to-payload mappings for historical data are stored.
    collection_key = get_name(SynchronizedData.participant_to_historical_round)

    # Defines the key used to extract and store the IPFS hash for OHLC market data from each participant's payload.
    selection_key = (get_name(SynchronizedData.ohlc_market_data_ipfs_hash),)


class RecentDataCollectionRound(CollectDifferentUntilThresholdRound):
    """
    Implementation of a data collection round focused on gathering the most recent cryptocurrency market data.
    Inherits from CollectDifferentUntilThresholdRound, allowing for the aggregation of different data types from various agents
    until a specific threshold of data is collected, ensuring robust and comprehensive data gathering.

    Attributes:
        payload_class (class): Specifies the type of payload this round will handle, which is defined by
                               RecentDataCollectionPayload.
        synchronized_data_class (class): Specifies the class that manages synchronized data, facilitating data operations.
        done_event (Event): Event to signal the completion of this data collection round.
        collection_key (str): Identifies the key under which the mapping of agents to their payloads will be stored in
                              synchronized data.
        selection_key (tuple): Defines the keys used for extracting recent data hashes from each agent's payload and
                               determining where to store it in synchronized data. This ensures consistent and organized data storage.
    """

    payload_class = RecentDataCollectionPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # Specifies where to store the mapping of agents to their respective payloads in the synchronized data,
    # which is vital for tracking and managing data contributions from different sources.
    collection_key = get_name(SynchronizedData.participant_to_recent_data_round)

    # Specifies the key for extracting the IPFS hash of recent market data from each participant's payload,
    # allowing for consistent and structured storage of this data within the synchronized data system.
    selection_key = (get_name(SynchronizedData.recent_data_ipfs_hash),)


class RealTimeDataStreamingRound(CollectDifferentUntilThresholdRound):
    """
    Implements a data collection round focused on gathering real-time cryptocurrency market data.
    Inherits from CollectDifferentUntilThresholdRound, which is designed to collect varying data types from different agents
    until a predetermined threshold is met. This approach is crucial for real-time data scenarios where immediate and
    synchronous data collection is required for accurate analysis.

    Attributes:
        payload_class (class): Defines the type of payload that this round will process, specifically RealTimeDataStreamingPayload,
                               which handles live data streams.
        synchronized_data_class (class): Specifies the class used to manage and synchronize data among different components of the system.
        done_event (Event): An event to signal the successful completion of the data collection round.
        collection_key (str): The key used to store mappings from agents to their payloads in the synchronized data,
                              essential for organizing real-time contributions efficiently.
        selection_key (tuple): Determines how data elements are extracted from each agent's payload and subsequently stored in the
                               synchronized data, maintaining order and structure as defined by the payload class.
    """

    payload_class = RealTimeDataStreamingPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # Key for storing the agent-to-payload mappings in the synchronized data, which is crucial for the management and retrieval
    # of real-time data entries from various agents.
    collection_key = get_name(SynchronizedData.participant_to_real_time_data_round)

    # Defines the key used to extract content (potentially streaming content such as live price feeds or alerts) from each
    # participant's payload and store it in an orderly and accessible manner within the synchronized data.
    selection_key = (get_name(SynchronizedData.content),)


class ComprehensiveCryptoInsightsRound(CollectDifferentUntilThresholdRound):
    """
    Implements a data collection round specifically for gathering comprehensive insights on cryptocurrency markets.
    Inherits from CollectDifferentUntilThresholdRound, enabling the aggregation of diverse data types from various agents
    until a set threshold is reached, which is critical for detailed and holistic market analysis.

    Attributes:
        payload_class (class): Specifies the type of payload this round will manage, defined by ComprehensiveCryptoInsightsPayload,
                               which focuses on collecting detailed market insights.
        synchronized_data_class (class): Indicates the class used for handling synchronized data, ensuring consistency and
                                         accuracy across data points.
        done_event (Event): Event to denote the completion of the collection round.
        collection_key (str): The key under which the mapping of agents to their payloads is stored in synchronized data,
                              crucial for tracking and managing comprehensive insight contributions.
        selection_key (tuple): Specifies the keys for extracting comprehensive market insights and their corresponding IPFS hashes
                               from each agent's payload, guiding where to store this data in the synchronized system.
    """

    payload_class = ComprehensiveCryptoInsightsPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # Key to store agent-to-payload mappings for rounds collecting comprehensive cryptocurrency insights,
    # facilitating structured and reliable data gathering and accessibility.
    collection_key = get_name(
        SynchronizedData.participant_to_comprehensive_crypto_currencies_insights_data_round
    )

    # Defines the selection keys used to extract comprehensive market insights and their IPFS hashes from each participant’s payload,
    # ensuring the orderly storage and retrieval of crucial data for further analysis.
    selection_key = (
        get_name(SynchronizedData.comprehensive_crypto_currencies_insights),
        get_name(SynchronizedData.comprehensive_crypto_currencies_insights_ipfs_hash),
    )


class CryptoTxPreparationRound(CollectSameUntilThresholdRound):
    """
    Implements a data collection round designed to prepare for cryptocurrency transactions. This class extends
    CollectSameUntilThresholdRound, which collects and synchronizes the same type of data from various agents until
    a predefined threshold is met, ensuring readiness for transaction execution.

    Attributes:
        payload_class (class): Defines the type of payload managed by this round, specified by CryptoTxPreparationPayload,
                               which deals with the preparation data necessary for cryptocurrency transactions.
        synchronized_data_class (class): Specifies the class used for synchronized data operations, which facilitates
                                         the aggregation and management of transaction-related data across different agents.
        done_event (Event): Event to signal the completion of the transaction preparation round.
        collection_key (str): Identifies the key under which mappings of agents to their transaction preparation payloads
                              are stored in synchronized data. This is crucial for organizing data necessary for transactions.
        selection_key (tuple): Specifies the keys used to extract the transaction submitter's identifier and the most voted
                               transaction hash from each agent's payload. This information is critical for finalizing transactions.
    """

    payload_class = CryptoTxPreparationPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE

    # Key for storing the mappings of agents to their respective transaction preparation payloads in the synchronized data,
    # which is vital for tracking readiness and contributions of each agent towards upcoming cryptocurrency transactions.
    collection_key = get_name(SynchronizedData.participant_to_tx_round)

    # Defines the keys used to extract the identity of the transaction submitter and the hash of the most voted transaction
    # from each participant's payload. This organized extraction and storage support the final decision-making process
    # in transaction execution.
    selection_key = (
        get_name(SynchronizedData.tx_submitter),
        get_name(SynchronizedData.most_voted_tx_hash),
    )


class FinishedRealTimeDataStreamingRound(DegenerateRound):
    """
    Represents the completion phase of a real-time data streaming round in a cryptocurrency data collection system.
    This class extends DegenerateRound, indicating that no further data collection or synchronization is required,
    effectively marking the end of an active real-time data streaming cycle. It is typically used to perform clean-up
    operations, finalize data states, or trigger subsequent processes following the conclusion of real-time data streaming.

    The FinishedRealTimeDataStreamingRound is crucial for ensuring that the system correctly transitions from an active
    data collection state to a stable, post-collection state, allowing for accurate finalization of the collected data
    and preparation for any further analysis or reporting tasks.
    """


class TokenDataCollectionAbciApp(AbciApp[Event]):
    """
    A blockchain-based application that orchestrates various rounds of cryptocurrency data collection and processing.
    This application transitions between different rounds based on events that signify the completion of tasks or
    errors and timeouts. It uses a finite state machine model to manage state transitions, ensuring robust handling
    of blockchain-related operations and data integrity.

    Attributes:
        initial_round_cls (AppState): The initial round class which kickstarts the application; in this case, it starts with
                                      the TopCryptoListDataCollectionRound.
        initial_states (Set[AppState]): A set containing the initial state of the application, facilitating the starting point.
        transition_function (AbciAppTransitionFunction): A dictionary mapping each round to its possible next states based on
                                                         the events that occur in the current state (e.g., DONE, NO_MAJORITY).
        final_states (Set[AppState]): States in which the application can terminate, indicating the end of the processing cycle.
        event_to_timeout (EventToTimeout): Defines timeout behaviors for specific events, enhancing fault tolerance.
        cross_period_persisted_keys (FrozenSet[str]): Identifiers for data elements that should persist across different periods
                                                      or sessions, maintaining state continuity.
        db_pre_conditions (Dict[AppState, Set[str]]): Conditions that must be met before entering a specific round, ensuring
                                                      prerequisites are satisfied.
        db_post_conditions (Dict[AppState, Set[str]]): Conditions that must be established after exiting a specific round, ensuring
                                                       that necessary outcomes are achieved for state progression.
    """

    initial_round_cls: AppState = TopCryptoListDataCollectionRound
    initial_states: Set[AppState] = {
        TopCryptoListDataCollectionRound,
    }
    transition_function: AbciAppTransitionFunction = {
        HistoricalDataCollectionRound: {
            Event.DONE: RecentDataCollectionRound,
            Event.NO_MAJORITY: HistoricalDataCollectionRound,
            Event.ROUND_TIMEOUT: HistoricalDataCollectionRound,
            Event.ERROR: HistoricalDataCollectionRound,
        },
        ComprehensiveCryptoInsightsRound: {
            Event.DONE: CryptoTxPreparationRound,
            Event.NO_MAJORITY: ComprehensiveCryptoInsightsRound,
            Event.ROUND_TIMEOUT: ComprehensiveCryptoInsightsRound,
            Event.ERROR: ComprehensiveCryptoInsightsRound,
        },
        CryptoTxPreparationRound: {
            Event.DONE: FinishedRealTimeDataStreamingRound,
            Event.NO_MAJORITY: CryptoTxPreparationRound,
            Event.ROUND_TIMEOUT: CryptoTxPreparationRound,
            Event.ERROR: CryptoTxPreparationRound,
        },
        RealTimeDataStreamingRound: {
            Event.DONE: ComprehensiveCryptoInsightsRound,
            Event.NO_MAJORITY: RealTimeDataStreamingRound,
            Event.ROUND_TIMEOUT: RealTimeDataStreamingRound,
            Event.ERROR: RealTimeDataStreamingRound,
        },
        RecentDataCollectionRound: {
            Event.DONE: RealTimeDataStreamingRound,
            Event.NO_MAJORITY: RecentDataCollectionRound,
            Event.ROUND_TIMEOUT: RecentDataCollectionRound,
            Event.ERROR: RecentDataCollectionRound,
        },
        TopCryptoListDataCollectionRound: {
            Event.DONE: HistoricalDataCollectionRound,
            Event.NO_MAJORITY: TopCryptoListDataCollectionRound,
            Event.ROUND_TIMEOUT: TopCryptoListDataCollectionRound,
            Event.ERROR: TopCryptoListDataCollectionRound,
        },
        FinishedRealTimeDataStreamingRound: {},
    }
    final_states: Set[AppState] = {
        FinishedRealTimeDataStreamingRound,
    }
    event_to_timeout: EventToTimeout = {}
    cross_period_persisted_keys: FrozenSet[str] = frozenset()
    db_pre_conditions: Dict[AppState, Set[str]] = {
        TopCryptoListDataCollectionRound: set(),
    }
    db_post_conditions: Dict[AppState, Set[str]] = {
        FinishedRealTimeDataStreamingRound: {
            get_name(SynchronizedData.most_voted_tx_hash)
        },
    }
