"""Volume trading engine.

Architecture:
- Bot places limit orders on Nado automatically (volume generation)
- ANY fill received from Nado WS (bot or manual) triggers a hedge on TradeXYZ
- Only ONE position open at a time across all instruments
- If close fails or is partial, next cycle retries close before opening new

Fill handling:
- During open-wait: any fill on the expected product_id is accepted (not just bot digest)
- Between cycles: fills queued in _fill_events are processed at start of next cycle
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Optional

from app.config import InstrumentConfig, Settings
from app.nado import NadoFill, NadoRestClient, NadoWSClient, round_qty
from app.telegram import TelegramAlerter

logger = logging.getLogger(__name__)

NADO_MIN_NOTIONAL_USD = 100.0


@dataclass
class UnclosedPosition:
    """A position that was opened (Nado filled + HL hedged) but not yet closed."""
    instrument: InstrumentConfig
    filled_qty: float
    nado_open_side: str   # "buy" or "sell" — the OPEN direction on Nado
    mirror_is_buy: bool   # HL hedge direction (True = long on HL)


class BalanceBlocker:
    def __init__(self, block_sec: int) -> None:
        self._block_sec = block_sec
        self._blocked_until: float = 0.0

    def block(self) -> None:
        self._blocked_until = time.time() + self._block_sec
        logger.warning("balance_blocker: trading blocked for %.1f hours",
                       self._block_sec / 3600)

    def is_blocked(self) -> bool:
        blocked = time.time() < self._blocked_until
        if blocked:
            logger.info("balance_blocker: still blocked, %.1f min remaining",
                        (self._blocked_until - time.time()) / 60)
        return blocked


class VolumeEngine:
    def __init__(self, settings: Settings, tg: TelegramAlerter | None = None) -> None:
        self.settings = settings
        self._tg = tg or TelegramAlerter()
        self.rest = NadoRestClient(settings.nado_rest_url)
        self._ws: Optional[NadoWSClient] = None
        self._balance_blocker = BalanceBlocker(settings.risk.balance_block_sec)
        self._fill_events: asyncio.Queue[NadoFill] = asyncio.Queue()
        self._hl_positions: dict[str, float] = {}
        # Tracks the currently open position (set after open-hedge, cleared after close)
        self._unclosed: Optional[UnclosedPosition] = None
        # Per-instrument trading status block (product_id → blocked_until timestamp)
        self._status_blocked_until: dict[int, float] = {}
        # Dust positions that cannot be closed programmatically (below Nado min notional)
        self._dust_positions: set[int] = set()

    async def _get_ws(self) -> NadoWSClient:
        if self._ws is None or not self._ws._connected:
            self._ws = NadoWSClient(
                ws_url=self.settings.nado_ws_url,
                subaccount=self.settings.nado_subaccount,
                private_key=self.settings.nado_private_key,
                chain_id=self.settings.nado_chain_id,
                endpoint_addr=self.settings.nado_endpoint_addr,
            )
            await self._ws.connect()
        return self._ws

    async def on_fill(self, fill: NadoFill) -> None:
        """Called by fill subscriber for every fill on the account."""
        await self._fill_events.put(fill)

    def _instrument_by_product(self, product_id: int) -> Optional[InstrumentConfig]:
        for i in self.settings.instruments:
            if i.product_id == product_id:
                return i
        return None

    async def _hedge_fill(self, tradexyz_client, fill: NadoFill,
                          instrument: InstrumentConfig) -> bool:
        """Open a reverse hedge on TradeXYZ for a given Nado fill.
        Returns True on success."""
        # Nado BUY → we are long on Nado → hedge SHORT on HL (sell)
        # Nado SELL → we are short on Nado → hedge LONG on HL (buy)
        mirror_is_buy = fill.side == "sell"
        mirror_side_str = "buy" if mirror_is_buy else "sell"
        logger.info("hedge: Nado %s %.6f %s @ %.4f → TradeXYZ %s",
                    fill.side, fill.qty, instrument.symbol, fill.price, mirror_side_str)
        try:
            result = tradexyz_client.place_market_order(
                symbol=instrument.symbol,
                is_buy=mirror_is_buy,
                size=fill.qty,
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
            if result.get("dry_run") or (
                    statuses and ("resting" in statuses[0] or "filled" in statuses[0])):
                logger.info("hedge: TradeXYZ hedge placed successfully")
                sym = instrument.symbol
                delta = fill.qty if mirror_is_buy else -fill.qty
                self._hl_positions[sym] = self._hl_positions.get(sym, 0.0) + delta
                logger.info("hedge: HL position %s = %.4f", sym, self._hl_positions[sym])
                return True
            else:
                logger.error("hedge: TradeXYZ order failed: %s", result)
                return False
        except Exception as exc:
            logger.error("hedge: TradeXYZ place failed: %s", exc)
            return False

    async def _check_and_hedge_incoming_fills(self, tradexyz_client) -> bool:
        """Drain fill queue and hedge any fills that arrived between cycles.
        Returns True if a new unclosed position was created."""
        fills_found = False
        while not self._fill_events.empty():
            try:
                fill = self._fill_events.get_nowait()
            except Exception:
                break
            instrument = self._instrument_by_product(fill.product_id)
            if instrument is None:
                logger.warning("incoming_fill: unknown product_id=%d, skipping", fill.product_id)
                continue
            logger.info("incoming_fill: unprocessed fill %s %.6f %s @ %.4f",
                        fill.side, fill.qty, instrument.symbol, fill.price)
            ok = await self._hedge_fill(tradexyz_client, fill, instrument)
            if ok:
                mirror_is_buy = fill.side == "sell"
                self._unclosed = UnclosedPosition(
                    instrument=instrument,
                    filled_qty=fill.qty,
                    nado_open_side=fill.side,
                    mirror_is_buy=mirror_is_buy,
                )
                fills_found = True
                # Only handle one fill at a time — rest will be processed next cycle
                break
        return fills_found

    async def _decide_direction(self, instrument: InstrumentConfig) -> str:
        if self.settings.strategy.direction_mode == "funding":
            try:
                funding_rate = await self.rest.get_funding_rate(instrument.product_id)
                if funding_rate is not None:
                    direction = "sell" if funding_rate > 0 else "buy"
                    logger.info("direction: funding_rate=%.6f → %s", funding_rate, direction)
                    return direction
            except Exception as exc:
                logger.warning("funding rate fetch failed: %s — using random", exc)
        return random.choice(["buy", "sell"])

    async def _check_slippage(self, instrument: InstrumentConfig,
                               side: str, notional_usd: float) -> tuple[bool, float]:
        ob = await self.rest.get_orderbook(instrument.product_id, depth=20)
        levels = ob.bids if side == "buy" else ob.asks
        if not levels:
            logger.warning("slippage_check: empty orderbook for %s side=%s",
                           instrument.symbol, side)
            return False, 0.0
        best_price = levels[0].price
        target_qty = notional_usd / best_price
        filled_usd = 0.0
        for level in levels:
            available_qty = min(level.qty, target_qty - filled_usd / best_price)
            if available_qty <= 0:
                break
            filled_usd += available_qty * level.price
            if filled_usd >= notional_usd:
                break
        if filled_usd < notional_usd * 0.5:
            logger.warning("slippage_check: insufficient liquidity for %s", instrument.symbol)
            return False, best_price
        avg_price = filled_usd / (filled_usd / best_price) if filled_usd > 0 else best_price
        slippage_bps = abs(avg_price - best_price) / best_price * 10000
        ok = slippage_bps <= self.settings.strategy.max_slippage_bps
        if not ok:
            logger.warning("slippage_check: %.2f bps > max %.1f — skipping",
                           slippage_bps, self.settings.strategy.max_slippage_bps)
        else:
            logger.info("slippage_check: OK %.2f bps best=%.4f", slippage_bps, best_price)
        return ok, best_price

    def _calc_limit_price(self, best_price: float, side: str, tick_size: float) -> float:
        offset = best_price * self.settings.strategy.price_offset_bps / 10000
        raw = (best_price - offset) if side == "buy" else (best_price + offset)
        from app.nado import round_price
        return round_price(raw, tick_size)

    async def _do_close_loop(self, tradexyz_client,
                              unclosed: UnclosedPosition) -> bool:
        """Close Nado position (limit loop) then close HL hedge (market).
        Returns True if fully closed on both sides."""
        instrument = unclosed.instrument
        close_nado_side = "sell" if unclosed.nado_open_side == "buy" else "buy"
        remaining_qty = unclosed.filled_qty
        total_close_filled = 0.0

        # Drain stale fills before close loop
        while not self._fill_events.empty():
            try:
                self._fill_events.get_nowait()
            except Exception:
                break

        # Check actual Nado position before placing close orders
        # If Nado already closed (e.g. closed manually), skip Nado loop
        try:
            real_positions = await self.rest.get_nado_positions(
                self.settings.nado_subaccount)
            real_qty = abs(real_positions.get(instrument.product_id, 0.0))
            if real_qty < instrument.lot_size / 2:
                logger.info("close: Nado position already closed (real=%.6f) — skipping Nado loop",
                            real_qty)
                remaining_qty = 0.0  # skip while loop, go straight to HL close
            else:
                # Use real qty in case it differs from our tracked qty
                remaining_qty = real_qty
                logger.info("close: Nado real position=%.6f (tracked=%.6f)",
                            real_qty, unclosed.filled_qty)
        except Exception as exc:
            logger.warning("close: could not fetch Nado position (%s) — using tracked qty", exc)

        while remaining_qty > instrument.lot_size / 2:
            bid, ask = await self.rest.get_market_price(instrument.product_id)
            close_best_price = bid if close_nado_side == "sell" else ask
            close_limit_price = self._calc_limit_price(
                close_best_price, close_nado_side, instrument.tick_size)
            close_qty = round_qty(remaining_qty, instrument.lot_size)

            if close_qty <= 0:
                logger.warning("close: qty rounds to 0 — treating as dust, done")
                remaining_qty = 0.0
                break

            # Ensure notional >= min (Nado rejects if abs(qty)*price < 100 USDC)
            # For dust: inflate limit price so notional passes — order still fills at market
            min_price_for_notional = NADO_MIN_NOTIONAL_USD / close_qty * 1.02
            if close_limit_price < min_price_for_notional:
                from app.nado import round_price
                close_limit_price = round_price(min_price_for_notional, instrument.tick_size)
                logger.info("close: dust — inflated price to %.4f to meet min notional",
                            close_limit_price)

            logger.info("close: placing Nado %s qty=%.6f @ %.4f remaining=%.6f (%s)",
                        close_nado_side, close_qty, close_limit_price,
                        remaining_qty, instrument.symbol)

            close_digest = None
            try:
                ws = await self._get_ws()
                close_digest = await ws.place_order(
                    product_id=instrument.product_id,
                    side=close_nado_side,
                    price=close_limit_price,
                    qty=close_qty,
                    tick_size=instrument.tick_size,
                    lot_size=instrument.lot_size,
                    isolated=instrument.isolated_only,
                    margin_usd=(close_limit_price * close_qty) / self.settings.strategy.leverage * 1.2 if instrument.isolated_only else 0.0,
                )
                logger.info("close: order placed digest=%s", close_digest)
            except RuntimeError as exc:
                err_str = str(exc)
                logger.error("close: order rejected: %s", exc)
                # Dust: can't close due to min notional + price range constraints
                if "2094" in err_str or "2007" in err_str or "too small" in err_str.lower():
                    self._dust_positions.add(instrument.product_id)
                    msg = (f"[DUST] Can't close {instrument.symbol} qty={remaining_qty:.6f} "
                           f"— below Nado min notional. Close manually on Nado UI!")
                    logger.warning(msg)
                    await self._tg.send(msg)
                    remaining_qty = 0.0  # stop retrying
                break
            except Exception as exc:
                logger.error("close: connection error: %s", exc)
                if self._ws:
                    await self._ws.close()
                self._ws = None
                break

            if not close_digest:
                break

            close_deadline = time.time() + 300
            attempt_filled = 0.0
            while time.time() < close_deadline:
                try:
                    remaining_wait = max(0, close_deadline - time.time())
                    fill = await asyncio.wait_for(
                        self._fill_events.get(), timeout=min(5.0, remaining_wait))
                    # Accept any fill for this product (bot close or manual close)
                    if fill.product_id == instrument.product_id:
                        attempt_filled += fill.qty
                        total_close_filled += fill.qty
                        remaining_qty -= fill.qty
                        logger.info("close: fill +%.6f total=%.6f remaining=%.6f",
                                    fill.qty, total_close_filled, remaining_qty)
                        if remaining_qty <= instrument.lot_size / 2:
                            break
                    else:
                        logger.debug("close: discarding fill for other product %d",
                                     fill.product_id)
                except asyncio.TimeoutError:
                    continue

            if remaining_qty > instrument.lot_size / 2:
                logger.warning("close: timeout after %.6f filled, cancelling, retrying %.6f",
                               attempt_filled, remaining_qty)
                try:
                    ws = await self._get_ws()
                    await ws.cancel_order(instrument.product_id, close_digest)
                except Exception as exc:
                    logger.error("close: cancel failed: %s", exc)

        fully_closed = remaining_qty <= instrument.lot_size / 2
        logger.info("close: Nado close done — filled=%.6f remaining=%.6f fully_closed=%s",
                    total_close_filled, remaining_qty, fully_closed)

        # Close HL hedge — use market_close() which handles oracle price correctly
        # If Nado was already closed before (total_close_filled=0), still try to close HL
        # since the hedge might be open from a previous cycle
        hl_close_attempted = total_close_filled > 0 or unclosed.filled_qty > 0
        if hl_close_attempted:
            logger.info("close: closing TradeXYZ position for %s via market_close",
                        instrument.symbol)
            try:
                if tradexyz_client.dry_run:
                    logger.info("close: [DRY RUN] TradeXYZ close skipped")
                else:
                    close_result = tradexyz_client.close_position(instrument.symbol)
                    if close_result is None:
                        # None = no open position on TradeXYZ — already clear
                        logger.warning("close: TradeXYZ market_close returned None — no position, treating as clear")
                        self._hl_positions[instrument.symbol] = 0.0
                    else:
                        close_statuses = (close_result.get("response", {})
                                          .get("data", {}).get("statuses", [{}]))
                        if close_statuses and (
                                "resting" in close_statuses[0]
                                or "filled" in close_statuses[0]):
                            logger.info("close: TradeXYZ position closed successfully")
                            self._hl_positions[instrument.symbol] = 0.0
                        elif close_result.get("dry_run"):
                            pass
                        elif close_statuses and "error" in close_statuses[0]:
                            err = close_statuses[0]["error"]
                            # "No position to close" means TradeXYZ side already clear
                            if "position" in err.lower() or "no open" in err.lower():
                                logger.warning("close: TradeXYZ no position to close — already clear")
                                self._hl_positions[instrument.symbol] = 0.0
                            else:
                                logger.error("close: TradeXYZ close error: %s", err)
                                fully_closed = False
                        else:
                            logger.error("close: TradeXYZ close failed: %s", close_result)
                            fully_closed = False
            except Exception as exc:
                logger.error("close: TradeXYZ close failed: %s", exc)
                fully_closed = False
        else:
            logger.warning("close: nothing filled on Nado — TradeXYZ hedge remains open!")
            fully_closed = False

        return fully_closed

    async def _close_orphaned_tradexyz(self, tradexyz_client) -> None:
        """Close any TradeXYZ positions that have no corresponding Nado position.
        Called when Nado shows no open positions — cleans up leftover hedges."""
        try:
            hl_positions = tradexyz_client.get_all_positions()
        except Exception as exc:
            logger.warning("orphan_check: get_all_positions failed: %s", exc)
            return

        enabled_symbols = {i.symbol for i in self.settings.enabled_instruments}
        for symbol, size in hl_positions.items():
            if symbol not in enabled_symbols:
                continue
            logger.warning("orphan_check: TradeXYZ has %s %.6f with no Nado position — closing",
                           symbol, size)
            try:
                result = tradexyz_client.close_position(symbol)
                if result is None:
                    logger.warning("orphan_check: %s already clear", symbol)
                else:
                    statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                    if statuses and ("filled" in statuses[0] or "resting" in statuses[0]):
                        logger.info("orphan_check: %s closed successfully", symbol)
                    else:
                        logger.error("orphan_check: %s close failed: %s", symbol, result)
            except Exception as exc:
                logger.error("orphan_check: close %s failed: %s", symbol, exc)

    async def _sync_from_nado(self, tradexyz_client) -> bool:
        """Check real open positions on Nado. If any found and _unclosed is not set,
        reconstruct _unclosed so the close loop can handle them.
        Returns True if an open position was found."""
        try:
            positions = await self.rest.get_nado_positions(self.settings.nado_subaccount)
        except Exception as exc:
            logger.error("sync: failed to fetch Nado positions: %s", exc)
            return False

        # Filter to only enabled instruments
        open_positions = {
            pid: qty for pid, qty in positions.items()
            if self._instrument_by_product(pid) is not None
        }

        if not open_positions:
            if self._unclosed is not None:
                # _unclosed set but Nado shows nothing — position already closed
                logger.info("sync: _unclosed was set but Nado shows no position — clearing")
                self._unclosed = None
            # Check TradeXYZ for orphaned hedges (Nado clear but TradeXYZ still open)
            await self._close_orphaned_tradexyz(tradexyz_client)
            return False

        # Filter out known dust positions (can't close programmatically)
        non_dust = {pid: qty for pid, qty in open_positions.items()
                    if pid not in self._dust_positions}
        if not non_dust:
            logger.debug("sync: only known dust positions remain — skipping")
            return False

        # Pick the first open position (should be only one)
        pid, qty = next(iter(non_dust.items()))
        instrument = self._instrument_by_product(pid)

        logger.warning("sync: Nado has open position pid=%d qty=%.6f — must close first", pid, qty)

        if self._unclosed is None:
            # Reconstruct from real data
            # qty > 0 means long on Nado (we bought), so hedge was short on HL
            nado_open_side = "buy" if qty > 0 else "sell"
            mirror_is_buy = qty < 0  # if Nado is short, HL hedge is long
            self._unclosed = UnclosedPosition(
                instrument=instrument,
                filled_qty=abs(qty),
                nado_open_side=nado_open_side,
                mirror_is_buy=mirror_is_buy,
            )
            logger.info("sync: reconstructed _unclosed from Nado: side=%s qty=%.6f",
                        nado_open_side, abs(qty))
        return True

    async def run_cycle(self, tradexyz_client) -> bool:
        """One complete trading cycle. Returns True if fully opened+closed."""
        strat = self.settings.strategy
        risk = self.settings.risk

        # === PRIORITY 0: sync real state from Nado (catches reboots, manual trades) ===
        await self._sync_from_nado(tradexyz_client)

        # === PRIORITY 1: close unclosed position from previous cycle ===
        if self._unclosed is not None:
            logger.warning("run_cycle: unclosed position %s qty=%.6f — closing first",
                           self._unclosed.instrument.symbol, self._unclosed.filled_qty)
            fully_closed = await self._do_close_loop(tradexyz_client, self._unclosed)
            if fully_closed:
                self._unclosed = None
                logger.info("run_cycle: unclosed position resolved")
            else:
                logger.warning("run_cycle: still could not close — retrying next cycle")
            return fully_closed

        # === PRIORITY 2: process any fills that arrived between cycles ===
        # (manual trades on Nado, or fills we missed while sleeping)
        if not self._fill_events.empty():
            logger.info("run_cycle: found unprocessed fills in queue — hedging first")
            found = await self._check_and_hedge_incoming_fills(tradexyz_client)
            if found:
                logger.info("run_cycle: manual fill hedged, will close next cycle")
                return False
            # Queue was drained (unknown products etc.) — proceed normally

        # === OPEN PHASE ===
        instruments = self.settings.enabled_instruments
        if not instruments:
            logger.error("No enabled instruments in config")
            return False

        instrument = random.choice(instruments)
        logger.info("=== CYCLE START: %s ===", instrument.symbol)

        if self._balance_blocker.is_blocked():
            return False

        balance = tradexyz_client.get_balance_usd()
        if balance is None:
            logger.warning("cycle: TradeXYZ balance check failed — skipping")
            return False
        logger.info("tradexyz balance: $%.2f", balance)

        if balance < risk.min_balance_usd:
            logger.warning("cycle: balance $%.2f < min $%.2f — blocking",
                           balance, risk.min_balance_usd)
            self._balance_blocker.block()
            await self._tg.send(
                f"[BALANCE BLOCKER] TradeXYZ balance ${balance:.2f} < min ${risk.min_balance_usd:.2f}\n"
                f"Trading blocked for {risk.balance_block_sec // 3600}h"
            )
            return False

        # Check trading status before opening (cached 1h to avoid log spam)
        pid = instrument.product_id
        blocked_until = self._status_blocked_until.get(pid, 0)
        if time.time() < blocked_until:
            logger.debug("cycle: %s status-blocked for %.0f more min — skipping",
                         instrument.symbol, (blocked_until - time.time()) / 60)
            return False
        trading_status = await self.rest.get_trading_status(pid)
        if trading_status in ("soft_reduce_only", "reduce_only", "inactive"):
            self._status_blocked_until[pid] = time.time() + 3600
            logger.warning("cycle: %s status=%s — skipping open, rechecking in 1h",
                           instrument.symbol, trading_status)
            return False
        logger.info("cycle: %s trading_status=%s", instrument.symbol, trading_status)

        side = await self._decide_direction(instrument)
        notional_usd = random.uniform(strat.order_min_usd, strat.order_max_usd)
        logger.info("cycle: side=%s notional=$%.0f", side, notional_usd)

        slippage_ok, best_price = await self._check_slippage(instrument, side, notional_usd)
        if not slippage_ok:
            return False

        limit_price = self._calc_limit_price(best_price, side, instrument.tick_size)
        qty = round_qty(notional_usd / limit_price, instrument.lot_size)
        if qty <= 0:
            logger.warning("cycle: qty too small after rounding, skipping")
            return False

        logger.info("cycle: placing limit %s qty=%.6f @ %.4f on Nado (%s)",
                    side, qty, limit_price, instrument.symbol)

        digest = None
        try:
            ws = await self._get_ws()
            margin_usd = (limit_price * qty) / self.settings.strategy.leverage * 1.2 if instrument.isolated_only else 0.0
            digest = await ws.place_order(
                product_id=instrument.product_id,
                side=side,
                price=limit_price,
                qty=qty,
                tick_size=instrument.tick_size,
                lot_size=instrument.lot_size,
                isolated=instrument.isolated_only,
                margin_usd=margin_usd,
            )
            logger.info("cycle: order placed digest=%s", digest)
        except RuntimeError as exc:
            logger.error("cycle: place_order rejected: %s", exc)
            return False
        except Exception as exc:
            logger.error("cycle: place_order connection error: %s", exc)
            if self._ws:
                await self._ws.close()
            self._ws = None
            return False

        # === WAIT FOR FILL ===
        # Accept ANY fill on this product_id — bot order OR manual order by user
        fill_timeout = strat.fill_timeout_sec
        filled_fill: Optional[NadoFill] = None
        deadline = time.time() + fill_timeout
        logger.info("cycle: waiting up to %ds for any fill on %s...",
                    fill_timeout, instrument.symbol)

        while time.time() < deadline:
            try:
                remaining = max(0, deadline - time.time())
                fill = await asyncio.wait_for(
                    self._fill_events.get(), timeout=min(5.0, remaining))
                if fill.product_id == instrument.product_id:
                    # Accept any fill for this product (bot or manual)
                    filled_fill = fill
                    logger.info("cycle: fill received! side=%s qty=%.6f price=%.4f digest=%s",
                                fill.side, fill.qty, fill.price, fill.order_digest)
                    break
                else:
                    # Fill for a different product — put back for next cycle
                    await self._fill_events.put(fill)
                    await asyncio.sleep(0.05)
            except asyncio.TimeoutError:
                continue

        if filled_fill is None:
            # Timeout — cancel bot order if it was placed
            logger.info("cycle: timeout, cancelling order %s", digest)
            if digest:
                try:
                    ws = await self._get_ws()
                    cancelled = await ws.cancel_order(instrument.product_id, digest)
                    logger.info("cycle: order cancelled=%s", cancelled)
                except Exception as exc:
                    logger.error("cycle: cancel failed: %s", exc)
                    if self._ws:
                        await self._ws.close()
                    self._ws = None
            # Drain any race-condition fills
            while not self._fill_events.empty():
                try:
                    stale = self._fill_events.get_nowait()
                    if stale.product_id == instrument.product_id:
                        logger.warning("cycle: discarding race-condition fill: %s",
                                       stale.order_digest)
                    else:
                        await self._fill_events.put(stale)
                        break
                except Exception:
                    break
            return False

        # If fill came from a different order (manual trade) — cancel our bot order
        if digest and filled_fill.order_digest != digest:
            logger.warning(
                "cycle: fill from different order (%s), cancelling our bot order %s",
                filled_fill.order_digest[:12], digest[:12])
            try:
                ws = await self._get_ws()
                await ws.cancel_order(instrument.product_id, digest)
                logger.info("cycle: bot order cancelled (superseded by manual fill)")
            except Exception as exc:
                logger.error("cycle: cancel own order failed: %s", exc)

        # === HEDGE ON TRADEXYZ ===
        ok = await self._hedge_fill(tradexyz_client, filled_fill, instrument)
        if not ok:
            # Emergency: Nado filled but HL hedge failed → close Nado at market
            logger.error("EMERGENCY: hedge failed! Closing Nado at market...")
            await self._tg.send(
                f"[EMERGENCY] TradeXYZ hedge FAILED for {instrument.symbol}!\n"
                f"Nado {filled_fill.side} {filled_fill.qty:.6f} @ {filled_fill.price:.4f}\n"
                f"Attempting emergency Nado close..."
            )
            try:
                ws = await self._get_ws()
                emergency_side = "sell" if filled_fill.side == "buy" else "buy"
                bid, ask = await self.rest.get_market_price(instrument.product_id)
                emergency_price = ask * 1.02 if emergency_side == "buy" else bid * 0.98
                await ws.place_order(
                    product_id=instrument.product_id,
                    side=emergency_side,
                    price=emergency_price,
                    qty=filled_fill.qty,
                    tick_size=instrument.tick_size,
                    lot_size=instrument.lot_size,
                )
                logger.info("EMERGENCY: Nado close order placed")
            except Exception as exc:
                logger.error("EMERGENCY: failed to close Nado: %s", exc)
            return False

        # Mark as unclosed — prevents new opens until this is closed
        mirror_is_buy = filled_fill.side == "sell"
        self._unclosed = UnclosedPosition(
            instrument=instrument,
            filled_qty=filled_fill.qty,
            nado_open_side=filled_fill.side,
            mirror_is_buy=mirror_is_buy,
        )

        # === HOLD ===
        hold_sec = random.uniform(strat.hold_min_sec, strat.hold_max_sec)
        logger.info("cycle: holding %.0fs (%.1f min)...", hold_sec, hold_sec / 60)
        await asyncio.sleep(hold_sec)

        # === CLOSE PHASE ===
        logger.info("=== CLOSE PHASE: %s ===", instrument.symbol)
        fully_closed = await self._do_close_loop(tradexyz_client, self._unclosed)
        if fully_closed:
            self._unclosed = None

        logger.info("=== CYCLE END: %s fully_closed=%s ===",
                    instrument.symbol, fully_closed)
        return fully_closed

    async def cleanup(self) -> None:
        await self.rest.close()
        if self._ws:
            await self._ws.close()
