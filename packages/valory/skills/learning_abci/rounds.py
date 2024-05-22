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

"""This package contains the rounds of LearningAbciApp."""

from enum import Enum
from typing import Dict, FrozenSet, Optional, Set

from packages.valory.skills.learning_abci.payloads import LearningPayload
from packages.valory.skills.abstract_round_abci.base import (
    AbciApp,
    AbciAppTransitionFunction,
    AppState,
    BaseSynchronizedData,
    CollectSameUntilThresholdRound,
    CollectionRound,
    DegenerateRound,
    DeserializedCollection,
    EventToTimeout,
    get_name,
)


class Event(Enum):
    """LearningAbciApp Events"""

    DONE = "done"
    NO_MAJORITY = "no_majority"
    ROUND_TIMEOUT = "round_timeout"


class SynchronizedData(BaseSynchronizedData):
    """
    Class to represent the synchronized data.

    This data is replicated by the tendermint application.
    """

    def _get_deserialized(self, key: str) -> DeserializedCollection:
        """Strictly get a collection and return it deserialized."""
        serialized = self.db.get_strict(key)
        return CollectionRound.deserialize_collection(serialized)

    @property
    def learning_data(self) -> Optional[str]:
        """Get the learning_data."""
        return self.db.get("learning_data", None)

    @property
    def participant_to_learning_round(self) -> DeserializedCollection:
        """Get the participants to the learning round."""
        return self._get_deserialized("participant_to_learning_round")


class LearningRound(CollectSameUntilThresholdRound):
    """LearningRound"""

    payload_class = LearningPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    no_majority_event = Event.NO_MAJORITY
    collection_key = get_name(SynchronizedData.participant_to_learning_round)
    selection_key = get_name(SynchronizedData.learning_data)

    # Event.ROUND_TIMEOUT  # this needs to be mentioned for static checkers


class FinishedLearningRound(DegenerateRound):
    """FinishedLearningRound"""


class LearningAbciApp(AbciApp[Event]):
    """LearningAbciApp"""

    initial_round_cls: AppState = LearningRound
    initial_states: Set[AppState] = {
        LearningRound,
    }
    transition_function: AbciAppTransitionFunction = {
        LearningRound: {
            Event.NO_MAJORITY: LearningRound,
            Event.ROUND_TIMEOUT: LearningRound,
            Event.DONE: FinishedLearningRound,
        },
        FinishedLearningRound: {},
    }
    final_states: Set[AppState] = {
        FinishedLearningRound,
    }
    event_to_timeout: EventToTimeout = {}
    cross_period_persisted_keys: FrozenSet[str] = frozenset()
    db_pre_conditions: Dict[AppState, Set[str]] = {
        LearningRound: set(),
    }
    db_post_conditions: Dict[AppState, Set[str]] = {
        FinishedLearningRound: set(),
    }
