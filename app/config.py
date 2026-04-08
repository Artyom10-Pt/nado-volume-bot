"""Configuration loader for nado-volume-bot."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class InstrumentConfig:
    product_id: int
    symbol: str           # TradeXYZ symbol, e.g. "xyz:GOLD"
    tick_size: float      # Nado price precision
    lot_size: float       # Nado qty precision
    enabled: bool = True
    isolated_only: bool = False


@dataclass
class StrategyConfig:
    direction_mode: str = "random"   # "random" or "funding"
    price_offset_bps: int = 4
    fill_timeout_sec: int = 300
    hold_min_sec: int = 600    # min time to hold open position
    hold_max_sec: int = 1800   # max time to hold open position
    leverage: int = 10             # trading leverage on TradeXYZ; implicit on Nado
    order_min_usd: float = 2000.0
    order_max_usd: float = 10000.0
    max_slippage_bps: float = 10.0


@dataclass
class RiskConfig:
    min_balance_usd: float = 500.0
    balance_block_sec: int = 43200
    dry_run: bool = False


@dataclass
class Settings:
    network: str
    instruments: list[InstrumentConfig]
    strategy: StrategyConfig
    risk: RiskConfig

    # From env
    nado_private_key: str = ""
    nado_subaccount: str = ""
    tradexyz_private_key: str = ""
    tradexyz_wallet_address: str = ""

    @classmethod
    def load(cls, config_path: str | None = None) -> "Settings":
        path = Path(config_path or os.getenv("APP_CONFIG_PATH", "/app/config/config.yaml"))
        cfg: dict[str, Any] = {}
        if path.exists():
            with path.open() as f:
                cfg = yaml.safe_load(f) or {}

        instruments = [
            InstrumentConfig(**i) for i in cfg.get("instruments", [])
        ]

        s = cfg.get("strategy", {})
        strategy = StrategyConfig(
            direction_mode=s.get("direction_mode", "random"),
            price_offset_bps=s.get("price_offset_bps", 4),
            fill_timeout_sec=s.get("fill_timeout_sec", 300),
            hold_min_sec=s.get("hold_min_sec", s.get("cycle_min_sec", 600)),
            hold_max_sec=s.get("hold_max_sec", s.get("cycle_max_sec", 1800)),
            leverage=s.get("leverage", 10),
            order_min_usd=s.get("order_min_usd", 2000.0),
            order_max_usd=s.get("order_max_usd", 10000.0),
            max_slippage_bps=s.get("max_slippage_bps", 10.0),
        )

        r = cfg.get("risk", {})
        risk = RiskConfig(
            min_balance_usd=r.get("min_balance_usd", 500.0),
            balance_block_sec=r.get("balance_block_sec", 43200),
            dry_run=r.get("dry_run", False),
        )

        settings = cls(
            network=cfg.get("network", "testnet"),
            instruments=instruments,
            strategy=strategy,
            risk=risk,
        )

        # Load secrets from env
        settings.nado_private_key = os.environ.get("NADO_WALLET_PRIVATE_KEY", "")
        settings.nado_subaccount = os.environ.get("NADO_SUBACCOUNT_SENDER", "")
        settings.tradexyz_private_key = os.environ.get("TRADEXYZ_PRIVATE_KEY", "")
        settings.tradexyz_wallet_address = os.environ.get("TRADEXYZ_WALLET_ADDRESS", "")

        return settings

    @property
    def enabled_instruments(self) -> list[InstrumentConfig]:
        return [i for i in self.instruments if i.enabled]

    @property
    def nado_ws_url(self) -> str:
        if self.network == "mainnet":
            return "wss://gateway.prod.nado.xyz/v1/ws"
        return "wss://gateway.test.nado.xyz/v1/ws"

    @property
    def nado_subscribe_url(self) -> str:
        if self.network == "mainnet":
            return "wss://gateway.prod.nado.xyz/v1/subscribe"
        return "wss://gateway.test.nado.xyz/v1/subscribe"

    @property
    def nado_rest_url(self) -> str:
        if self.network == "mainnet":
            return "https://gateway.prod.nado.xyz/v1"
        return "https://gateway.test.nado.xyz/v1"

    @property
    def tradexyz_api_url(self) -> str:
        if self.network == "mainnet":
            return "https://api.hyperliquid.xyz"
        return "https://api.hyperliquid-testnet.xyz"

    @property
    def nado_endpoint_addr(self) -> str:
        if self.network == "mainnet":
            return "0x05ec92d78ed421f3d3ada77ffde167106565974e"
        return "0x698D87105274292B5673367DEC81874Ce3633Ac2"

    @property
    def nado_chain_id(self) -> int:
        if self.network == "mainnet":
            return 57073   # ink mainnet
        return 763373      # inkSepolia testnet
