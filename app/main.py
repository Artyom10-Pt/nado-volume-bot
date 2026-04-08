"""nado-volume-bot main entry point.

Runs continuous volume trading cycles between Nado DEX and TradeXYZ.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import sys

from app.config import Settings
from app.engine import VolumeEngine
from app.nado import NadoFillSubscriber, NadoRestClient
from app.telegram import TelegramAlerter
from app.tradexyz import TradeXYZClient
from app.sheets import build_sheets_reporter

VERSION = "1.0.0"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


async def run(settings: Settings) -> None:
    log = logging.getLogger(__name__)
    tg = TelegramAlerter()
    log.info("nado-volume-bot v%s starting (network=%s)", VERSION, settings.network)

    if not settings.nado_private_key:
        log.error("NADO_WALLET_PRIVATE_KEY not set")
        return
    if not settings.nado_subaccount:
        log.error("NADO_SUBACCOUNT_SENDER not set")
        return
    if not settings.tradexyz_private_key:
        log.error("TRADEXYZ_PRIVATE_KEY not set")
        return

    # Warn if mainnet with placeholder endpoint
    if settings.network == "mainnet" and settings.nado_endpoint_addr.startswith("0x4e59"):
        log.error("MAINNET: nado_endpoint_addr is a placeholder — orders will fail! Update config.py.")
        return

    # Init TradeXYZ client (with retry on startup API unavailability)
    tradexyz = None
    for attempt in range(5):
        try:
            tradexyz = TradeXYZClient(
                api_url=settings.tradexyz_api_url,
                private_key=settings.tradexyz_private_key,
                wallet_address=settings.tradexyz_wallet_address,
                dry_run=settings.risk.dry_run,
            )
            break
        except Exception as exc:
            log.warning("TradeXYZ init attempt %d/5 failed: %s", attempt + 1, exc)
            if attempt < 4:
                await asyncio.sleep(10)
    if tradexyz is None:
        msg = f"[{settings.network.upper()}] nado-volume-bot: TradeXYZ init failed after 5 attempts — stopped"
        log.error(msg)
        await tg.send(msg)
        return

    # Set 10x leverage on TradeXYZ for all enabled instruments
    leverage = settings.strategy.leverage
    log.info("Setting TradeXYZ leverage to %dx for all enabled instruments...", leverage)
    for instrument in settings.enabled_instruments:
        tradexyz.set_leverage(instrument.symbol, leverage, is_cross=not instrument.isolated_only)

    # Init Google Sheets reporter
    sheets = build_sheets_reporter()

    # Init Nado REST client for balance queries
    nado_rest = NadoRestClient(base_url=settings.nado_rest_url)

    # Init engine
    engine = VolumeEngine(settings, tg)

    # Init fill subscriber
    fill_sub = NadoFillSubscriber(
        subscribe_url=settings.nado_subscribe_url,
        subaccount=settings.nado_subaccount,
        private_key=settings.nado_private_key,
        chain_id=settings.nado_chain_id,
        endpoint_addr=settings.nado_endpoint_addr,
    )
    fill_sub.on_fill(engine.on_fill)

    # Run fill subscriber as background task
    sub_task = asyncio.create_task(fill_sub.run())

    # Balance tracker: log to Sheets every 6 hours
    async def balance_tracker_loop():
        log = logging.getLogger(__name__)
        while True:
            try:
                nado_bal = await nado_rest.get_subaccount_balance_usd(settings.nado_subaccount)
                xyz_bal = tradexyz.get_balance_usd() or 0.0
                if sheets:
                    sheets.log_balances(nado_bal, xyz_bal)
                log.info("Balance snapshot: Nado=%.2f TradeXYZ=%.2f", nado_bal, xyz_bal)
            except Exception as exc:
                log.warning("balance_tracker error: %s", exc)
            await asyncio.sleep(6 * 3600)

    balance_task = asyncio.create_task(balance_tracker_loop())

    symbols = ", ".join(i.symbol for i in settings.enabled_instruments)
    await tg.send(
        f"[{settings.network.upper()}] nado-volume-bot v{VERSION} started\n"
        f"Instruments: {symbols}\n"
        f"Leverage: {leverage}x | dry_run: {settings.risk.dry_run}"
    )

    try:
        while True:
            try:
                completed = await engine.run_cycle(tradexyz)
            except Exception as exc:
                log.error("cycle error: %s", exc, exc_info=True)
                await tg.send(
                    f"[{settings.network.upper()}] ERROR: unhandled cycle exception\n"
                    f"{type(exc).__name__}: {exc}"
                )
                completed = False

            if completed:
                # Full cycle done (position opened and closed) — start next immediately
                log.info("cycle completed, starting next immediately...")
                await asyncio.sleep(2)
            else:
                # Skipped cycle (no fill, error, blocked) — short wait before retry
                log.info("sleeping 30s before retry...")
                await asyncio.sleep(30)

    except asyncio.CancelledError:
        log.info("shutdown signal received")
    finally:
        balance_task.cancel()
        await fill_sub.stop()
        sub_task.cancel()
        try:
            await sub_task
        except asyncio.CancelledError:
            pass
        try:
            await balance_task
        except asyncio.CancelledError:
            pass
        await engine.cleanup()
        await tg.send(f"[{settings.network.upper()}] nado-volume-bot stopped")
        log.info("shutdown complete")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Nado volume trading bot")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true",
                        help="Override config dry_run=True (no real orders on TradeXYZ)")
    args = parser.parse_args()

    setup_logging(args.log_level)

    settings = Settings.load(args.config)
    if args.dry_run:
        settings.risk.dry_run = True

    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
