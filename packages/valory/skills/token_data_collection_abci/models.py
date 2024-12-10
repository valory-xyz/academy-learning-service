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

"""This module contains the shared state for the abci skill of TokenDataCollectionAbciApp."""


import datetime
from typing import Any
from packages.valory.skills.token_data_collection_abci.rounds import (
    TokenDataCollectionAbciApp,  # type: ignore[import]
)
from packages.valory.skills.abstract_round_abci.models import (  # type: ignore[import] # noqa: F401
    ApiSpecs,
    BaseParams,
)
from packages.valory.skills.abstract_round_abci.models import (
    BenchmarkTool as BaseBenchmarkTool,  # type: ignore[import]
)
from packages.valory.skills.abstract_round_abci.models import (
    Requests as BaseRequests,  # type: ignore[import]
)
from packages.valory.skills.abstract_round_abci.models import (
    SharedState as BaseSharedState,  # type: ignore[import]
)


Requests = BaseRequests
BenchmarkTool = BaseBenchmarkTool


class SharedState(BaseSharedState):
    """
    Represents the current shared state of the ABcI skill within the TokenDataCollectionAbciApp.
    It extends the BaseSharedState, allowing for shared state variables and functionalities across various
    components of the application.

    Attributes:
        abci_app_cls (TokenDataCollectionAbciApp): The ABcI application class this shared state is associated with.
    """

    abci_app_cls = TokenDataCollectionAbciApp


class Params(BaseParams):
    """
    Parameters model for ABcI applications, managing configurations across multiple applications.
    Provides a structured way to handle parameter passing and default values.

    Attributes:
        coingecko_histroy_data_template (str): Template string for fetching historical data from the Coingecko API.
        transfer_target_address (Optional[str]): Target address for transferring assets, if applicable.
        multisend_address (Optional[str]): Address used for multi-send transactions, retained for compatibility with other skills.
        ipfs_storage_contract (str): The contract address for IPFS storage interactions.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the parameters with defaults and required fields checking."""

        self.coingecko_histroy_data_template = self._ensure(
            "coingecko_histroy_data_template", kwargs, str
        )

        self.transfer_target_address = kwargs.get("transfer_target_address", None)

        # multisend address is used in other skills, so we cannot pop it using _ensure
        self.multisend_address = kwargs.get("multisend_address", None)

        self.ipfs_storage_contract = self._ensure("ipfs_storage_contract", kwargs, str)

        super().__init__(*args, **kwargs)


class CoingeckoTopCryptocurrenciesSpecs(ApiSpecs):
    """Model that encapsulates API specifications for interfacing with the Coingecko API."""
