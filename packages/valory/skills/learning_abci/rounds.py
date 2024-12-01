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
    CollectionRound,
    DegenerateRound,
    DeserializedCollection,
    EventToTimeout,
    get_name,
)
from packages.valory.skills.learning_abci.payloads import (
    DataPullPayload,
    DecisionMakingPayload,
    EvaluationPayload,
    TxPreparationPayload,
)


class Event(Enum):
    """LearningAbciApp Events"""

    DONE = "done"
    ERROR = "error"
    TRANSACT = "transact"
    NO_MAJORITY = "no_majority"
    ROUND_TIMEOUT = "round_timeout"


class SynchronizedData(BaseSynchronizedData):
    """
    Class to represent the synchronized data.

    This data is replicated by the tendermint application, so all the agents share the same data.
    """

    def _get_deserialized(self, key: str) -> DeserializedCollection:
        """Strictly get a collection and return it deserialized."""
        serialized = self.db.get_strict(key)
        return CollectionRound.deserialize_collection(serialized)

    @property
    def price(self) -> Optional[float]:
        """Get the token price."""
        return self.db.get("price", None)

    @property
    def price_ipfs_hash(self) -> Optional[str]:
        """Get the price_ipfs_hash."""
        return self.db.get("price_ipfs_hash", None)

    @property
    def native_balance(self) -> Optional[float]:
        """Get the native balance."""
        return self.db.get("native_balance", None)

    @property
    def erc20_balance(self) -> Optional[float]:
        """Get the erc20 balance."""
        return self.db.get("erc20_balance", None)

    @property
    def participant_to_data_round(self) -> DeserializedCollection:
        """Agent to payload mapping for the DataPullRound."""
        return self._get_deserialized("participant_to_data_round")
    
    @property
    def historical_data_ipfshash(self) -> Optional[str]:
        """Get the IPFS hash of historical data."""
        return self.db.get("historical_data_ipfshash", None)
    
    @property
    def comparison_data(self) -> Optional[bool]:
        """Get the comparison result of current vs. historical prices."""
        return self.db.get("comparison_data", None)
    
    @property
    def participant_to_evaluation_round(self) -> DeserializedCollection:
        """Agent to payload mapping for the DataPullRound."""
        return self._get_deserialized("participant_to_evaluation_round")
    

    @property
    def most_voted_tx_hash(self) -> Optional[float]:
        """Get the token most_voted_tx_hash."""
        return self.db.get("most_voted_tx_hash", None)

    @property
    def participant_to_tx_round(self) -> DeserializedCollection:
        """Get the participants to the tx round."""
        return self._get_deserialized("participant_to_tx_round")

    @property
    def tx_submitter(self) -> str:
        """Get the round that submitted a tx to transaction_settlement_abci."""
        return str(self.db.get_strict("tx_submitter"))


class DataPullRound(CollectSameUntilThresholdRound):
    """DataPullRound"""

    payload_class = DataPullPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    no_majority_event = Event.NO_MAJORITY

    # Collection key specifies where in the synchronized data the agento to payload mapping will be stored
    collection_key = get_name(SynchronizedData.participant_to_data_round)

    # Selection key specifies how to extract all the different objects from each agent's payload
    # and where to store it in the synchronized data. Notice that the order follows the same order
    # from the payload class.
    selection_key = (
        get_name(SynchronizedData.price),
        get_name(SynchronizedData.price_ipfs_hash),
        get_name(SynchronizedData.native_balance),
        get_name(SynchronizedData.erc20_balance),
    )

    # Event.ROUND_TIMEOUT  # this needs to be referenced for static checkers


class DecisionMakingRound(CollectSameUntilThresholdRound):
    """DecisionMakingRound"""

    payload_class = DecisionMakingPayload
    synchronized_data_class = SynchronizedData

    # Since we need to execute some actions after consensus, we override the end_block method
    # instead of just setting the selection and collection keys
    def end_block(self) -> Optional[Tuple[BaseSynchronizedData, Event]]:
        """Process the end of the block."""

        if self.threshold_reached:
            event = Event(self.most_voted_payload)
            return self.synchronized_data, event

        if not self.is_majority_possible(
            self.collection, self.synchronized_data.nb_participants
        ):
            return self.synchronized_data, Event.NO_MAJORITY

        return None

    # Event.DONE, Event.ERROR, Event.TRANSACT, Event.ROUND_TIMEOUT  # this needs to be referenced for static checkers

class EvaluationRound(CollectSameUntilThresholdRound):
    """
    EvaluationRound is designed to compare current and historical prices, an essential step in scenarios
    where decisions are based on market trends. This round involves agents reaching a consensus on the 
    current price and comparing it to previously synchronized historical prices to assess market conditions.
    """

    payload_class = EvaluationPayload  
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    error_event = Event.ERROR
    no_majority_event = Event.NO_MAJORITY

    # Adjust the collection key to use the appropriate collection for this round
    collection_key = get_name(SynchronizedData.participant_to_evaluation_round)  # Adjusted
    
    # Selection key should map directly to the payload data correctly
    selection_key = (
        get_name(SynchronizedData.historical_data_ipfshash),
        get_name(SynchronizedData.comparison_data),
    )

    def end_block(self) -> Optional[Tuple[BaseSynchronizedData, Event]]:
        """
        This method is called at the end of the round to finalize the operations based on the data collected.
        Since this is a simplified example, it automatically concludes the round by returning the synchronized
        data along with a 'DONE' event indicating the round has completed successfully without errors or 
        the inability to reach a majority.
        
        Returns:
            Optional[Tuple[BaseSynchronizedData, Event]]: Returns a tuple containing the current state of 
            synchronized data along with the 'DONE' event to signal the successful end of the round.
        """
        print("self.synchronized_data",self.synchronized_data)
        return self.synchronized_data, Event.DONE



class TxPreparationRound(CollectSameUntilThresholdRound):
    """TxPreparationRound"""

    payload_class = TxPreparationPayload
    synchronized_data_class = SynchronizedData
    done_event = Event.DONE
    no_majority_event = Event.NO_MAJORITY
    collection_key = get_name(SynchronizedData.participant_to_tx_round)
    selection_key = (
        get_name(SynchronizedData.tx_submitter),
        get_name(SynchronizedData.most_voted_tx_hash),
    )

    # Event.ROUND_TIMEOUT  # this needs to be referenced for static checkers


class FinishedDecisionMakingRound(DegenerateRound):
    """FinishedDecisionMakingRound"""


class FinishedTxPreparationRound(DegenerateRound):
    """FinishedLearningRound"""


class LearningAbciApp(AbciApp[Event]):
    """LearningAbciApp"""

    initial_round_cls: AppState = DataPullRound
    initial_states: Set[AppState] = {
        DataPullRound,
    }
    transition_function: AbciAppTransitionFunction = {
        DataPullRound: {
            Event.NO_MAJORITY: DataPullRound,
            Event.ROUND_TIMEOUT: DataPullRound,
            Event.DONE: DecisionMakingRound,
        },
        DecisionMakingRound: {
            Event.NO_MAJORITY: DecisionMakingRound,
            Event.ROUND_TIMEOUT: DecisionMakingRound,
            Event.DONE: FinishedDecisionMakingRound,
            Event.ERROR: FinishedDecisionMakingRound,
            Event.TRANSACT: EvaluationRound,
        },
        EvaluationRound: {
            Event.NO_MAJORITY: EvaluationRound,
            Event.ROUND_TIMEOUT: EvaluationRound,
            Event.DONE: TxPreparationRound,
            Event.ERROR: FinishedDecisionMakingRound,
        },
        TxPreparationRound: {
            Event.NO_MAJORITY: TxPreparationRound,
            Event.ROUND_TIMEOUT: TxPreparationRound,
            Event.DONE: FinishedTxPreparationRound,
        },
        FinishedDecisionMakingRound: {},
        FinishedTxPreparationRound: {},
    }
    final_states: Set[AppState] = {
        FinishedDecisionMakingRound,
        FinishedTxPreparationRound,
    }
    event_to_timeout: EventToTimeout = {}
    cross_period_persisted_keys: FrozenSet[str] = frozenset()
    db_pre_conditions: Dict[AppState, Set[str]] = {
        DataPullRound: set(),
    }
    db_post_conditions: Dict[AppState, Set[str]] = {
        FinishedDecisionMakingRound: set(),
        FinishedTxPreparationRound: {get_name(SynchronizedData.most_voted_tx_hash)},
    }
