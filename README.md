# nado-volume-bot

Automated volume trading bot that simultaneously places mirrored orders on [Nado DEX](https://nado.xyz) and [TradeXYZ](https://trade.xyz).

## How it works

1. Picks a random instrument and direction (long/short)
2. Places an order on Nado DEX (spot-like DEX)
3. Mirrors the hedge on TradeXYZ (perps)
4. Waits for fills, holds position for a random interval, then closes both sides
5. Logs results to Telegram and Google Sheets

## Setup

### Requirements

- Python 3.11+
- Install dependencies: `pip install -r requirements.txt`

### Environment variables

```
NADO_WALLET_PRIVATE_KEY=0x...
NADO_SUBACCOUNT_SENDER=0x...
TRADEXYZ_PRIVATE_KEY=0x...
TRADEXYZ_WALLET_ADDRESS=0x...
TELEGRAM_BOT_TOKEN=...         # optional
TELEGRAM_CHAT_ID=...           # optional
GOOGLE_SHEETS_CREDS_FILE=...   # optional, path to service account JSON
GOOGLE_SPREADSHEET_ID=...      # optional
```

### Config

Edit `config/config.yaml` to set instruments, strategy parameters, and risk limits.

### Run

```bash
python -m app.main
python -m app.main --dry-run       # no real orders on TradeXYZ
python -m app.main --log-level DEBUG
```

## Disclaimer

This software is for educational purposes. Use at your own risk.
