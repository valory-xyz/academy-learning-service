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

"""This module contains the transaction payloads for common apps."""
from dataclasses import dataclass, field
from typing import Optional

from packages.valory.skills.abstract_round_abci.base import BaseTxPayload


@dataclass(frozen=True)
class TopCryptoListDataCollectionPayload(BaseTxPayload):
    """
    Payload for collecting data about top cryptocurrencies. This class represents the data structure
    used to store and transmit information about top cryptocurrencies during the data collection round.

    Attributes:
        top_crypto_currencies (Optional[str]): A string containing the list of top cryptocurrencies.
        top_crypto_currencies_ipfs_hash (Optional[str]): The IPFS hash of the data pertaining to top cryptocurrencies.
    """

    top_crypto_currencies: Optional[str]
    top_crypto_currencies_ipfs_hash: Optional[str]


@dataclass(frozen=True)
class HistoricalDataCollectionPayload(BaseTxPayload):
    """
    Payload used for collecting historical market data. This payload carries the IPFS hash of the
    historical OHLC (Open, High, Low, Close) market data.

    Attributes:
        ohlc_market_data_ipfs_hash (Optional[str]): The IPFS hash of the OHLC market data.
    """

    ohlc_market_data_ipfs_hash: Optional[str]


@dataclass(frozen=True)
class RecentDataCollectionPayload(BaseTxPayload):
    """
    Payload for collecting recent cryptocurrency market data. It contains the IPFS hash which can be used
    to retrieve the most recent data from an IPFS node.

    Attributes:
        recent_data_ipfs_hash (Optional[str]): The IPFS hash of the recent market data.
    """

    recent_data_ipfs_hash: Optional[str]


@dataclass(frozen=True)
class RealTimeDataStreamingPayload(BaseTxPayload):
    """
    Payload class for real-time data streaming. This class is used in rounds where live data needs to be
    streamed and processed immediately.

    Attributes:
        content (str): The actual content of the real-time data being streamed.
    """

    content: str


@dataclass(frozen=True)
class ComprehensiveCryptoInsightsPayload(BaseTxPayload):
    """
    Payload for rounds collecting comprehensive insights about cryptocurrencies. This class encapsulates
    both the insights themselves, typically structured as a dictionary, and the corresponding IPFS hash.

    Attributes:
        comprehensive_crypto_currencies_insights (Optional[dict]): Dictionary containing detailed insights on cryptocurrencies.
        comprehensive_crypto_currencies_insights_ipfs_hash (Optional[str]): IPFS hash of the comprehensive insights data.
    """

    comprehensive_crypto_currencies_insights: Optional[dict]
    comprehensive_crypto_currencies_insights_ipfs_hash: Optional[str]


@dataclass(frozen=True)
class CryptoTxPreparationPayload(BaseTxPayload):
    """
    Payload for preparing cryptocurrency transactions. This payload contains necessary details for transaction
    submission and confirmation.

    Attributes:
        tx_submitter (Optional[str]): Identifier of the transaction submitter, if applicable.
        tx_hash (Optional[str]): Hash of the transaction being prepared.
    """

    tx_submitter: Optional[str] = None
    tx_hash: Optional[str] = None
