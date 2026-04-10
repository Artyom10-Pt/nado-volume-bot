"""TradeXYZ (Hyperliquid) client wrapper."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

try:
    import requests as _requests
    from eth_account import Account as EthAccount
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    _SDK_AVAILABLE = True
except ImportError as _e:
    logger.warning("hyperliquid SDK not available: %s", _e)
    _SDK_AVAILABLE = False


def _safe_spot_meta(base_url: str) -> dict:
    """Fetch spot_meta and filter out entries with out-of-range token indices."""
    resp = _requests.post(f"{base_url}/info", json={"type": "spotMeta"}, timeout=15)
    spot_meta = resp.json()
    tokens_len = len(spot_meta.get("tokens", []))
    valid_universe = [
        u for u in spot_meta.get("universe", [])
        if len(u.get("tokens", [])) == 2
        and u["tokens"][0] < tokens_len
        and u["tokens"][1] < tokens_len
    ]
    spot_meta["universe"] = valid_universe
    return spot_meta


class TradeXYZClient:
    """Wrapper around Hyperliquid Python SDK for TradeXYZ.
    
    Supports both default HL perp (SOL, BTC, ETH...) and xyz dex (xyz:GOLD, xyz:SILVER...).
    Symbol format:
      - HL crypto perp: "SOL", "BTC", "ETH"...
      - xyz dex perp:   "xyz:GOLD", "xyz:SILVER", "xyz:CL"...
    """

    def __init__(self, api_url: str, private_key: str, wallet_address: str,
                 dry_run: bool = False) -> None:
        self.api_url = api_url
        self.dry_run = dry_run
        self.wallet_address = wallet_address

        if not _SDK_AVAILABLE:
            raise RuntimeError("hyperliquid SDK not installed")

        spot_meta = _safe_spot_meta(api_url)
        wallet = EthAccount.from_key(private_key)

        # perp_dexs=["", "xyz"]:
        #   ""  → default HL perp (SOL, BTC, ETH... at asset offset 0)
        #   "xyz" → xyz dex (xyz:GOLD, xyz:SILVER, xyz:CL... at offset 110000+)
        self._info = Info(
            base_url=api_url,
            skip_ws=True,
            spot_meta=spot_meta,
            perp_dexs=["", "xyz"],
        )
        self._exchange = Exchange(
            wallet=wallet,
            base_url=api_url,
            account_address=wallet_address or None,
            perp_dexs=["", "xyz"],
            spot_meta=spot_meta,
        )

    def set_leverage(self, symbol: str, leverage: int, is_cross: bool = True) -> bool:
        """Set leverage for a symbol on TradeXYZ. Returns True on success."""
        if self.dry_run:
            logger.info("[DRY RUN] set_leverage %s %dx cross=%s", symbol, leverage, is_cross)
            return True
        try:
            result = self._exchange.update_leverage(leverage, symbol, is_cross)
            logger.info("tradexyz: set_leverage %s %dx result: %s", symbol, leverage, result)
            return True
        except Exception as exc:
            logger.error("tradexyz: set_leverage %s failed: %s", symbol, exc)
            return False

    def get_balance_usd(self) -> float | None:
        """Returns total account value across all dexs (HL perp + xyz dex).

        user_state(address) without dex= returns only HL perp state.
        xyz dex balance (where Silver/WTI positions live) is separate and must be summed.
        """
        try:
            hl_state = self._info.user_state(self.wallet_address, dex="")
            hl_value = float(hl_state.get("marginSummary", {}).get("accountValue", 0))
            xyz_state = self._info.user_state(self.wallet_address, dex="xyz")
            xyz_value = float(xyz_state.get("marginSummary", {}).get("accountValue", 0))
            return hl_value + xyz_value
        except Exception as exc:
            logger.error("tradexyz get_balance failed: %s", exc)
            return None

    def place_market_order(self, symbol: str, is_buy: bool, size: float) -> dict[str, Any]:
        """Place a market order. Symbol can be HL perp (SOL) or xyz dex (xyz:GOLD)."""
        if self.dry_run:
            logger.info("[DRY RUN] market_open %s %s %.6f",
                        symbol, "BUY" if is_buy else "SELL", size)
            return {"dry_run": True, "symbol": symbol, "is_buy": is_buy, "size": size}

        logger.info("tradexyz: market_open %s %s %.6f",
                    symbol, "BUY" if is_buy else "SELL", size)
        result = self._exchange.market_open(symbol, is_buy, size)
        logger.info("tradexyz: market_open result: %s", result)
        return result

    def close_position(self, symbol: str) -> dict[str, Any] | None:
        """Close entire position for a symbol."""
        if self.dry_run:
            logger.info("[DRY RUN] market_close %s", symbol)
            return {"dry_run": True, "symbol": symbol}
        try:
            logger.info("tradexyz: market_close %s", symbol)
            result = self._exchange.market_close(symbol)
            logger.info("tradexyz: market_close result: %s", result)
            return result
        except Exception as exc:
            logger.error("tradexyz close_position failed for %s: %s", symbol, exc)
            return None

    def get_position(self, symbol: str) -> float:
        """Returns current position size (positive=long, negative=short). 0 if none."""
        try:
            state = self._info.user_state(self.wallet_address)
            for pos in state.get("assetPositions", []):
                item = pos.get("position", {})
                if item.get("coin") == symbol:
                    return float(item.get("szi", 0))
        except Exception as exc:
            logger.error("tradexyz get_position failed: %s", exc)
        return 0.0

    def get_all_positions(self) -> dict[str, float]:
        """Returns all open positions as {symbol: size}. Excludes zero positions."""
        try:
            state = self._info.user_state(self.wallet_address)
            result = {}
            for pos in state.get("assetPositions", []):
                item = pos.get("position", {})
                coin = item.get("coin")
                szi = float(item.get("szi", 0))
                if coin and szi != 0.0:
                    result[coin] = szi
            return result
        except Exception as exc:
            logger.error("tradexyz get_all_positions failed: %s", exc)
        return {}
