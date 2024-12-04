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

"""This package contains round behaviours of ContractReadAbciApp."""

import json
from abc import ABC
from pathlib import Path
from tempfile import mkdtemp
from typing import Dict, Generator, Optional, Set, Type, cast

from packages.valory.contracts.erc20.contract import ERC20
from packages.valory.contracts.erc20_new.contract import ERC20_NEW

from packages.valory.protocols.contract_api import ContractApiMessage
from packages.valory.protocols.ledger_api import LedgerApiMessage
from packages.valory.skills.abstract_round_abci.base import AbstractRound
from packages.valory.skills.abstract_round_abci.behaviours import (
    AbstractRoundBehaviour,
    BaseBehaviour,
)
from packages.valory.skills.abstract_round_abci.io_.store import SupportedFiletype
from packages.valory.skills.contract_read_abci.models import (
    Params,
    SharedState,
    Coingeckopricehistorydataspecs,
)
from packages.valory.skills.contract_read_abci.payloads import (
   ContractDataReadPayload,
)
from packages.valory.skills.contract_read_abci.rounds import (
    ContractDataReadRound,
    ContractReadAbciApp,
    Event,
    SynchronizedData,
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

class ContractReadBaseBehaviour(BaseBehaviour, ABC):  # pylint: disable=too-many-ancestors
    """Base behaviour for the contract_read_abci behaviours."""

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
    def coingecko_pricehistorydata_specs(self) -> Coingeckopricehistorydataspecs:
        """Get the Coingecko api specs."""
        return self.context.coingecko_pricehistorydata_specs

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


class ContractDataReadBehaviour(ContractReadBaseBehaviour):
    """Base behaviour for the learning_abci behaviours."""

    matching_round: Type[AbstractRound] = ContractDataReadRound

    def async_act(self) -> Generator:
        """Do the act, supporting asynchronous execution."""
        
        with self.context.benchmark_tool.measure(self.behaviour_id).local():
            sender = self.context.agent_address

            payload =ContractDataReadPayload(sender=sender,event=Event.DONE.value)

        with self.context.benchmark_tool.measure(self.behaviour_id).consensus():
            yield from self.send_a2a_transaction(payload)
            yield from self.wait_until_round_end()

        self.set_done()

class ContractReadRoundBehaviour(AbstractRoundBehaviour):
    """ContractReadRoundBehaviour"""

    initial_behaviour_cls = ContractDataReadBehaviour
    abci_app_cls = ContractReadAbciApp  # type: ignore
    behaviours: Set[Type[BaseBehaviour]] = [  # type: ignore
        ContractDataReadBehaviour,
    ]
