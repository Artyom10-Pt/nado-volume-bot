# app/sheets.py
import logging
import os
from datetime import datetime, timezone
from typing import Optional

try:
    import gspread
    from google.oauth2.service_account import Credentials
    _SHEETS_AVAILABLE = True
except ImportError:
    _SHEETS_AVAILABLE = False

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

NADO_START = 3600.0
TRADEXYZ_START = 3590.0
TOTAL_START = NADO_START + TRADEXYZ_START


class SheetsReporter:
    def __init__(self, credentials_file: str, spreadsheet_id: str, sheet_name: str):
        creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        try:
            self._ws = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            raise RuntimeError(f"Sheet '{sheet_name}' not found in spreadsheet")
        logger.info("Google Sheets connected: %s → %s", spreadsheet.title, sheet_name)

    def log_balances(self, nado_usd: float, tradexyz_usd: float) -> None:
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            total = nado_usd + tradexyz_usd
            change_from_start = total - TOTAL_START

            # Find last row to compute change from previous
            all_values = self._ws.get_all_values()
            prev_total = None
            for row in reversed(all_values):
                # Look for rows that have numeric data in column D (index 3)
                if len(row) >= 4 and row[3].replace(",", ".").replace("-", "").replace(".", "").isdigit():
                    try:
                        prev_total = float(row[3].replace(",", "."))
                        break
                    except ValueError:
                        continue

            change_from_prev = round(total - prev_total, 2) if prev_total is not None else 0

            self._ws.append_row([
                now,
                round(nado_usd, 2),
                round(tradexyz_usd, 2),
                round(total, 2),
                round(change_from_start, 2),
                change_from_prev,
            ], value_input_option="USER_ENTERED")
            logger.info("Sheets: logged balances Nado=%.2f TradeXYZ=%.2f Total=%.2f",
                        nado_usd, tradexyz_usd, total)
        except Exception as e:
            logger.warning("Sheets log_balances failed: %s", e)


def build_sheets_reporter() -> Optional[SheetsReporter]:
    if not _SHEETS_AVAILABLE:
        logger.warning("gspread not installed — Google Sheets disabled")
        return None
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Nado + TradeXyz_")
    if not creds_file or not spreadsheet_id:
        return None
    try:
        return SheetsReporter(creds_file, spreadsheet_id, sheet_name)
    except Exception as e:
        logger.warning("Google Sheets init failed (disabled): %s", e)
        return None
