"""Nado REST + WebSocket client.

Handles:
- Orderbook queries (REST)
- Market price queries (REST)
- Subaccount balance queries (REST)
- WS authentication (EIP-712)
- WS order placement (EIP-712 signed)
- WS order cancellation (EIP-712 signed)
- WS fill subscription
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine

import aiohttp
from decimal import Decimal
import websockets
from eth_account import Account
from eth_account.messages import encode_typed_data

logger = logging.getLogger(__name__)

PRICE_X18 = 10 ** 18


# ── Helpers ────────────────────────────────────────────────────────────────────

def bytes32_from_hex(h: str) -> bytes:
    return bytes.fromhex(h.removeprefix("0x").lower().zfill(64))


def gen_nonce() -> int:
    """Generate order nonce: (recv_time_ms << 20) + random.
    
    The server decodes recv_time from nonce as (nonce >> 20).
    recv_time must be in the future or server returns error 2011.
    """
    recv_time_ms = int(time.time() * 1000) + 90_000  # +90 seconds
    return (recv_time_ms << 20) + random.randint(0, 999)


def order_verifying_contract(product_id: int) -> str:
    """Product-specific verifying contract: address(product_id) = 20-byte big-endian."""
    return "0x" + product_id.to_bytes(20, "big").hex()


def round_price(price: float, tick_size: float) -> float:
    """Round price to tick_size precision."""
    return round(round(price / tick_size) * tick_size, 10)


def round_qty(qty: float, lot_size: float) -> float:
    """Round qty down to lot_size precision (float-safe)."""
    import math
    # Use round(..., 9) before floor to suppress floating-point noise
    # e.g. 0.141 / 0.001 = 140.99999... → round gives 141 → correct
    steps = math.floor(round(qty / lot_size, 9))
    # Determine decimal places in lot_size for final rounding
    s = f"{lot_size:.10f}".rstrip("0")
    decimals = len(s.split(".")[1]) if "." in s else 0
    result = round(steps * lot_size, decimals)
    return result


# ── EIP-712 signing ────────────────────────────────────────────────────────────

def _build_domain(chain_id: int, verifying_contract: str) -> dict:
    return {
        "name": "Nado",
        "version": "0.0.1",
        "chainId": chain_id,
        "verifyingContract": verifying_contract,
    }


def _eip712_domain_types() -> list:
    return [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ]


def sign_stream_auth(private_key: str, sender: str, expiration_ms: int,
                     chain_id: int, endpoint_addr: str) -> str:
    sender_bytes = bytes32_from_hex(sender)
    data = {
        "domain": _build_domain(chain_id, endpoint_addr),
        "types": {
            "EIP712Domain": _eip712_domain_types(),
            "StreamAuthentication": [
                {"name": "sender", "type": "bytes32"},
                {"name": "expiration", "type": "uint64"},
            ],
        },
        "primaryType": "StreamAuthentication",
        "message": {"sender": sender_bytes, "expiration": expiration_ms},
    }
    sig = encode_typed_data(full_message=data)
    return Account.sign_message(sig, private_key=private_key).signature.hex()


def sign_order(private_key: str, sender: str, product_id: int,
               price_x18: int, amount: int, expiration_sec: int,
               nonce: int, appendix: int,
               chain_id: int) -> str:
    sender_bytes = bytes32_from_hex(sender)
    verify_contract = order_verifying_contract(product_id)
    data = {
        "domain": _build_domain(chain_id, verify_contract),
        "types": {
            "EIP712Domain": _eip712_domain_types(),
            "Order": [
                {"name": "sender",     "type": "bytes32"},
                {"name": "priceX18",   "type": "int128"},
                {"name": "amount",     "type": "int128"},
                {"name": "expiration", "type": "uint64"},
                {"name": "nonce",      "type": "uint64"},
                {"name": "appendix",   "type": "uint128"},
            ],
        },
        "primaryType": "Order",
        "message": {
            "sender":     sender_bytes,
            "priceX18":   price_x18,
            "amount":     amount,
            "expiration": expiration_sec,
            "nonce":      nonce,
            "appendix":   appendix,
        },
    }
    sig = encode_typed_data(full_message=data)
    return Account.sign_message(sig, private_key=private_key).signature.hex()


def sign_cancel(private_key: str, sender: str, product_ids: list[int],
                digests: list[str], nonce: int,
                chain_id: int, endpoint_addr: str) -> str:
    sender_bytes = bytes32_from_hex(sender)
    digest_bytes = [bytes32_from_hex(d) for d in digests]
    data = {
        "domain": _build_domain(chain_id, endpoint_addr),
        "types": {
            "EIP712Domain": _eip712_domain_types(),
            "Cancellation": [
                {"name": "sender",     "type": "bytes32"},
                {"name": "productIds", "type": "uint32[]"},
                {"name": "digests",    "type": "bytes32[]"},
                {"name": "nonce",      "type": "uint64"},
            ],
        },
        "primaryType": "Cancellation",
        "message": {
            "sender":     sender_bytes,
            "productIds": product_ids,
            "digests":    digest_bytes,
            "nonce":      nonce,
        },
    }
    sig = encode_typed_data(full_message=data)
    return Account.sign_message(sig, private_key=private_key).signature.hex()


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class OrderbookLevel:
    price: float
    qty: float


@dataclass
class Orderbook:
    bids: list[OrderbookLevel]  # sorted highest first
    asks: list[OrderbookLevel]  # sorted lowest first


@dataclass
class NadoFill:
    order_digest: str
    product_id: int
    side: str       # "buy" or "sell"
    qty: float
    price: float
    subaccount: str


FillCallback = Callable[[NadoFill], Coroutine[Any, Any, None]]


# ── REST client ────────────────────────────────────────────────────────────────

class NadoRestClient:
    """Async REST client for Nado gateway."""

    HEADERS = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
    }

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.HEADERS)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _query(self, payload: dict) -> dict:
        session = await self._get_session()
        async with session.post(f"{self.base_url}/query", json=payload) as resp:
            data = await resp.json(content_type=None)
        if data.get("status") != "success":
            raise RuntimeError(f"Nado query error: {data}")
        return data["data"]

    async def get_market_price(self, product_id: int) -> tuple[float, float]:
        """Returns (best_bid, best_ask) in USD."""
        data = await self._query({"type": "market_price", "product_id": product_id})
        bid = int(data["bid_x18"]) / PRICE_X18
        ask = int(data["ask_x18"]) / PRICE_X18
        return bid, ask

    async def get_orderbook(self, product_id: int, depth: int = 10) -> Orderbook:
        """Returns orderbook with prices in USD."""
        data = await self._query({
            "type": "market_liquidity",
            "product_id": product_id,
            "depth": depth,
        })
        bids = [OrderbookLevel(int(p) / PRICE_X18, int(q) / PRICE_X18)
                for p, q in data["bids"]]
        asks = [OrderbookLevel(int(p) / PRICE_X18, int(q) / PRICE_X18)
                for p, q in data["asks"]]
        return Orderbook(bids=bids, asks=asks)

    async def get_subaccount_balance_usd(self, subaccount: str) -> float:
        """Returns cross-margin health (approx USD balance)."""
        data = await self._query({"type": "subaccount_info", "subaccount": subaccount})
        # healths[0].health is the most conservative (initial margin) health
        health_x18 = int(data["healths"][0]["health"])
        return health_x18 / PRICE_X18

    async def get_open_orders(self, sender: str, product_id: int) -> list[dict]:
        data = await self._query({
            "type": "subaccount_orders",
            "sender": sender,
            "product_id": product_id,
        })
        return data.get("orders", [])

    async def get_nado_positions(self, subaccount: str) -> dict[int, float]:
        """Returns open perp positions as {product_id: qty}.
        Positive = long, negative = short. Zero positions excluded.
        Checks both cross-margin (subaccount_info) and isolated positions."""
        positions = {}
        # Cross-margin positions
        data = await self._query({"type": "subaccount_info", "subaccount": subaccount})
        for bal in data.get("perp_balances", []):
            pid = bal["product_id"]
            amount_x18 = int(bal["balance"]["amount"])
            if amount_x18 != 0:
                positions[pid] = amount_x18 / PRICE_X18
        # Isolated positions
        try:
            isol_data = await self._query({"type": "isolated_positions", "subaccount": subaccount})
            for pos in isol_data.get("isolated_positions", []):
                base = pos.get("base_balance", {})
                pid = base.get("product_id")
                amount_x18 = int(base.get("balance", {}).get("amount", 0))
                if pid is not None and amount_x18 != 0:
                    positions[pid] = amount_x18 / PRICE_X18
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("get_nado_positions: isolated_positions failed: %s", exc)
        return positions

    async def get_funding_rate(self, product_id: int) -> float | None:
        """Nado perp_products do not expose funding_rate — always returns None."""
        return None


    async def get_trading_status(self, product_id: int) -> str:
        """Returns trading_status for a perp instrument.
        Possible values: live, post_only, soft_reduce_only, reduce_only, inactive.
        Returns unknown on error."""
        try:
            data = await self._query({"type": "symbols"})
            for symbol_data in data.get("symbols", {}).values():
                if (symbol_data.get("type") == "perp"
                        and symbol_data.get("product_id") == product_id):
                    return symbol_data.get("trading_status", "unknown")
        except Exception as exc:
            logger.warning("get_trading_status pid=%d failed: %s", product_id, exc)
        return "unknown"

# ── WS execute client ──────────────────────────────────────────────────────────

class NadoWSClient:
    """REST-based execute client for Nado order placement/cancellation.
    
    Named NadoWSClient for compatibility, but uses /v1/execute REST endpoint.
    (Nado /v1/ws auth format changed — REST is now the correct execute path.)
    """

    def __init__(self, ws_url: str, subaccount: str, private_key: str,
                 chain_id: int, endpoint_addr: str) -> None:
        # ws_url kept for interface compat; we derive REST base from it
        self.rest_url = ws_url.replace("wss://", "https://").replace("/ws", "").replace("/subscribe", "")
        self.subaccount = subaccount
        self.private_key = private_key
        self.chain_id = chain_id
        self.endpoint_addr = endpoint_addr
        self._connected = True   # REST is always "connected"
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers={
                "Accept-Encoding": "gzip, deflate, br",
                "Content-Type": "application/json",
            })
        return self._session

    async def connect(self) -> None:
        """No-op — REST needs no persistent connection."""
        logger.info("nado_execute: REST client ready at %s/execute", self.rest_url)
        self._connected = True

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._connected = False

    async def _post_execute(self, payload: dict) -> dict:
        session = await self._get_session()
        async with session.post(
            f"{self.rest_url}/execute",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            return await resp.json(content_type=None)

    async def place_order(self, product_id: int, side: str,
                          price: float, qty: float,
                          tick_size: float, lot_size: float,
                          isolated: bool = False, margin_usd: float = 0.0) -> str:
        """Place a limit order via REST. Returns order digest."""
        price = round_price(price, tick_size)
        qty = round_qty(qty, lot_size)
        if qty <= 0:
            raise ValueError(f"Invalid qty after rounding: {qty}")

        # Use Decimal to avoid float precision errors in x18 conversion
        price_x18 = int(Decimal(str(price)) * Decimal(10**18))
        amount = int(Decimal(str(qty)) * Decimal(10**18))
        if side == "sell":
            amount = -amount

        expiration_sec = int(time.time()) + 3600
        nonce = gen_nonce()
        # appendix encodes order flags:
        #   bits 0-7: version (1 = GTC limit)
        #   bit 8: isolated margin flag
        #   bits 64-127: margin_x6 (margin in USD * 1e6, only for isolated open)
        if isolated:
            margin_x6 = int(margin_usd * 1_000_000)
            appendix = 1 | (1 << 8) | (margin_x6 << 64)
        else:
            appendix = 1  # version=1, GTC limit, cross margin

        sig = sign_order(
            self.private_key, self.subaccount, product_id,
            price_x18, amount, expiration_sec, nonce, appendix,
            self.chain_id,
        )

        payload = {
            "place_order": {
                "product_id": product_id,
                "order": {
                    "sender":     self.subaccount,
                    "priceX18":   str(price_x18),
                    "amount":     str(amount),
                    "expiration": str(expiration_sec),
                    "nonce":      str(nonce),
                    "appendix":   str(appendix),
                },
                "signature": sig,
            }
        }

        resp = await self._post_execute(payload)
        logger.info("nado place_order response: %s", resp)

        if resp.get("status") == "failure":
            raise RuntimeError(f"place_order failed [{resp.get('error_code')}]: {resp.get('error')}")

        # Extract digest from response
        data = resp.get("data") or resp.get("result") or resp
        digest = (data.get("digest") or data.get("order_digest") or "")
        if not digest:
            logger.warning("place_order: no digest in response: %s", resp)
        return digest

    async def cancel_order(self, product_id: int, digest: str) -> bool:
        """Cancel a specific order via REST."""
        nonce = gen_nonce()
        sig = sign_cancel(
            self.private_key, self.subaccount,
            [product_id], [digest], nonce,
            self.chain_id, self.endpoint_addr,
        )
        payload = {
            "cancel_orders": {
                "tx": {
                    "sender":     self.subaccount,
                    "productIds": [product_id],
                    "digests":    [digest],
                    "nonce":      str(nonce),
                },
                "signature": sig,
            }
        }
        resp = await self._post_execute(payload)
        logger.info("nado cancel_order response: %s", resp)
        return resp.get("status") == "success"


# ── Fill subscriber ────────────────────────────────────────────────────────────

class NadoFillSubscriber:
    """Subscribes to fill events via the subscribe WS endpoint."""

    def __init__(self, subscribe_url: str, subaccount: str, private_key: str,
                 chain_id: int, endpoint_addr: str) -> None:
        self.url = subscribe_url
        self.subaccount = subaccount
        self.private_key = private_key
        self.chain_id = chain_id
        self.endpoint_addr = endpoint_addr
        self._running = False
        self._callbacks: list[FillCallback] = []

    def on_fill(self, cb: FillCallback) -> None:
        self._callbacks.append(cb)

    async def run(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                await self._connect_and_listen()
                delay = 1.0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("fill_sub: error: %s — retrying in %.1fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def stop(self) -> None:
        self._running = False

    async def _connect_and_listen(self) -> None:
        logger.info("fill_sub: connecting to %s", self.url)
        async with websockets.connect(
            self.url,
            additional_headers={"Accept-Encoding": "gzip, deflate, br"},
            ping_interval=30, ping_timeout=10,
        ) as ws:
            # Authenticate
            exp_ms = int(time.time() * 1000) + 90_000
            sig = sign_stream_auth(
                self.private_key, self.subaccount, exp_ms,
                self.chain_id, self.endpoint_addr,
            )
            await ws.send(json.dumps({
                "method": "authenticate", "id": 0,
                "tx": {"sender": self.subaccount, "expiration": str(exp_ms)},
                "signature": sig,
            }))
            auth_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
            if auth_resp.get("error"):
                raise RuntimeError(f"fill_sub auth failed: {auth_resp['error']}")
            logger.info("fill_sub: authenticated")

            # Subscribe
            await ws.send(json.dumps({
                "method": "subscribe", "id": 1,
                "stream": {"type": "fill", "subaccount": self.subaccount},
            }))
            sub_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
            if sub_resp.get("error"):
                raise RuntimeError(f"fill_sub subscribe failed: {sub_resp['error']}")
            logger.info("fill_sub: subscribed")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                # Only process fill messages
                msg_type = str(msg.get("channel") or msg.get("type") or "")
                if "fill" not in msg_type.lower():
                    continue

                fill = self._parse_fill(msg)
                if fill:
                    for cb in self._callbacks:
                        await cb(fill)

    def _parse_fill(self, msg: dict) -> NadoFill | None:
        try:
            d = msg.get("data", msg)
            digest = str(d.get("order_digest") or d.get("digest", ""))
            product_id = int(d.get("product_id", -1))
            qty_raw = d.get("filled_qty") or d.get("base_filled", "0")
            price_raw = d.get("price", "0")
            qty = int(qty_raw) / PRICE_X18
            price = int(price_raw) / PRICE_X18
            side = "buy" if d.get("is_bid", True) else "sell"
            subaccount = str(d.get("subaccount", ""))
            return NadoFill(
                order_digest=digest,
                product_id=product_id,
                side=side,
                qty=qty,
                price=price,
                subaccount=subaccount,
            )
        except Exception as exc:
            logger.warning("fill_sub parse error: %s", exc)
            return None
