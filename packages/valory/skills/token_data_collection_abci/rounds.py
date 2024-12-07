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

"""This module contains the rounds definitions for valory_abci."""
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
    """Synchronised data."""

    def _get_deserialized(self, key: str) -> DeserializedCollection:
        """Strictly get a collection and return it deserialized."""
        serialized = self.db.get_strict(key)
        return CollectionRound.deserialize_collection(serialized)
    
    @property
    def top_crypto_currencies(self) -> Optional[str]:
        """Get the top_crypto_currencies."""
        return self.db.get("top_crypto_currencies", None)
    
    @property
    def top_crypto_currencies_ipfs_hash(self) -> Optional[str]:
        """Get the top_crypto_currencies."""
        return self.db.get("top_crypto_currencies_ipfs_hash", None)
       
    @property
    def participant_to_data_round(self) -> DeserializedCollection:
        """Agent to payload mapping for the DataPullRound."""
        return self._get_deserialized("participant_to_data_round")
    
    @property
    def participant_to_historical_round(self) -> DeserializedCollection:
        """Agent to payload mapping for the DataPullRound."""
        return self._get_deserialized("participant_to_historical_round")

class TopCryptoListDataCollectionRound(CollectDifferentUntilThresholdRound):
    """TopCryptoListDataCollectionRound round implementation."""

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
    """HistoricalDataCollectionRound round implementation."""

    payload_class = HistoricalDataCollectionPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    collection_key = get_name(SynchronizedData.participant_to_historical_round)
    selection_key = "content"


class RealTimeDataStreamingRound(CollectDifferentUntilThresholdRound):
    """RealTimeDataStreamingRound round implementation."""

    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    payload_class = RealTimeDataStreamingPayload
    selection_key = "content"
    collection_key = "content"


class RecentDataCollectionRound(CollectDifferentUntilThresholdRound):
    """RecentDataCollectionRound round implementation."""

    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    payload_class = RecentDataCollectionPayload
    selection_key = "content"
    collection_key = "content"

class FinishedRealTimeDataStreamingRound(DegenerateRound):
    """FinishedRealTimeDataStreamingRound"""

class TokenDataCollectionAbciApp(AbciApp[Event]):
    """ContractReadAbciApp definition"""

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
        RealTimeDataStreamingRound: {
            Event.DONE: FinishedRealTimeDataStreamingRound,
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
        FinishedRealTimeDataStreamingRound: set(),
    }