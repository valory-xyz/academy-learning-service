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
from typing import Dict, FrozenSet, Optional, Set, Tuple

from packages.valory.skills.abstract_round_abci.base import (
    AbciApp,
    AbciAppTransitionFunction,
    AppState,
    BaseSynchronizedData,
    CollectSameUntilThresholdRound,
    DegenerateRound,
    EventToTimeout,
    get_name,
)
from packages.valory.skills.contract_read_abci.payloads import (
    ContractDataReadPayload,
)


class Event(Enum):
    """ContractReadAbciApp Events"""

    DONE = "done"
    ERROR = "error"
    NO_MAJORITY = "no_majority"
    ROUND_TIMEOUT = "round_timeout"


class SynchronizedData(BaseSynchronizedData):
    """
    Class to represent the synchronized data.

    This data is replicated by the tendermint application, so all the agents share the same data.
    """



class ContractDataReadRound(CollectSameUntilThresholdRound):
    """ContractDataReadRound"""
    
    payload_class = ContractDataReadPayload
    synchronized_data_class = SynchronizedData

    # Since we need to execute some actions after consensus, we override the end_block method
    # instead of just setting the selection and collection keys
    def end_block(self) -> Optional[Tuple[BaseSynchronizedData, Event]]:
        """Process the end of the block."""

        return self.synchronized_data, Event.DONE



class FinishedContractDataReadRound(DegenerateRound):
    """FinishedContractDataReadRound"""


class ContractReadAbciApp(AbciApp[Event]):
    """ContractReadAbciApp definition"""

    initial_round_cls: AppState = ContractDataReadRound
    initial_states: Set[AppState] = {
        ContractDataReadRound,
    }
    transition_function: AbciAppTransitionFunction = {
        ContractDataReadRound: {
            Event.NO_MAJORITY: ContractDataReadRound,
            Event.ROUND_TIMEOUT: ContractDataReadRound,
            Event.DONE: FinishedContractDataReadRound,
            Event.ERROR: ContractDataReadRound,
        },
        FinishedContractDataReadRound: {},
    }
    final_states: Set[AppState] = {
        FinishedContractDataReadRound,
    }
    event_to_timeout: EventToTimeout = {}
    cross_period_persisted_keys: FrozenSet[str] = frozenset()
    db_pre_conditions: Dict[AppState, Set[str]] = {
        ContractDataReadRound: set(),
    }
    db_post_conditions: Dict[AppState, Set[str]] = {
        FinishedContractDataReadRound: set(),
    }
