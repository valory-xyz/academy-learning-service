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
    """TopCryptoListDataCollectionPayload class."""

    content: str
    top_crypto_currencies: Optional[str]
    top_crypto_currencies_ipfs_hash: Optional[str]

@dataclass(frozen=True)
class HistoricalDataCollectionPayload(BaseTxPayload):
    """HistoricalDataCollectionPayload class."""

    content: str

@dataclass(frozen=True)
class RecentDataCollectionPayload(BaseTxPayload):
    """RecentDataCollectionPayload class."""

    content: str

@dataclass(frozen=True)
class RealTimeDataStreamingPayload(BaseTxPayload):
    """RealTimeDataStreamingPayload class."""

    content: str
