from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


DUNE_API_BASE = "https://api.dune.com/api/v1"
DEFAULT_EVM_QUERY_ID = 7325007
DEFAULT_SOL_QUERY_ID = 7325094
DEFAULT_POLL_INTERVAL_SECONDS = 3600
DEFAULT_BOOTSTRAP_LOOKBACK_MINUTES = 10
DEFAULT_TIMEOUT_SECONDS = 30
MAX_SEEN_TRANSACTIONS = 5000
USER_AGENT = "smart-money-tracker/1.0"
EVM_MONITOR_CHAINS = ("ethereum", "bnb", "base")
SOL_MONITOR_CHAIN = "solana"
MAX_WALLETS_PER_BATCH = 40


@dataclass(slots=True)
class WatchAddress:
    address_type: str
    address: str
    label: str
    blockchains: tuple[str, ...]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_address(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise ValueError(f"invalid EVM address: {value}")
    return normalized


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def parse_blockchains(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()

    separators = ["|", ","]
    normalized = value.strip().lower()
    for separator in separators:
        normalized = normalized.replace(separator, ",")

    items = [item.strip() for item in normalized.split(",") if item.strip()]
    return tuple(dict.fromkeys(items))


def infer_label(row: dict[str, str], address: str) -> str:
    for key in ("label", "name", "alias"):
        candidate = (row.get(key) or "").strip()
        if candidate:
            return candidate

    last_active = (row.get("last_active") or "").strip()
    if last_active:
        return f"{address[:8]}... ({last_active})"
    return address


def load_watchlist(csv_path: Path) -> tuple[list[WatchAddress], list[WatchAddress]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"watchlist file not found: {csv_path}")

    evm_watches: list[WatchAddress] = []
    sol_watches: list[WatchAddress] = []
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"address"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"missing required CSV columns: {', '.join(sorted(missing))}")

        for row in reader:
            address_type = (row.get("address_type") or "").strip().lower()
            if address_type not in {"", "evm", "sol"}:
                continue

            enabled = parse_bool(row.get("enabled"), default=True)
            if not enabled:
                continue

            raw_address = (row.get("address") or "").strip()
            if not raw_address:
                continue

            watch_type = address_type or "evm"
            if watch_type == "evm":
                address = normalize_address(raw_address)
                label = infer_label(row, address)
                evm_watches.append(
                    WatchAddress(
                        address_type="evm",
                        address=address,
                        label=label,
                        blockchains=EVM_MONITOR_CHAINS,
                    )
                )
                continue

            label = infer_label(row, raw_address)
            sol_watches.append(
                WatchAddress(
                    address_type="sol",
                    address=raw_address,
                    label=label,
                    blockchains=(SOL_MONITOR_CHAIN,),
                )
            )

    if not evm_watches and not sol_watches:
        raise ValueError("no enabled addresses found in the watchlist CSV")

    return evm_watches, sol_watches


def load_state(state_file: Path, bootstrap_lookback_minutes: int) -> dict[str, Any]:
    if not state_file.exists():
        return {
            "last_checked_at": isoformat_z(utc_now() - timedelta(minutes=bootstrap_lookback_minutes)),
            "seen_tx_hashes": {},
        }

    with state_file.open("r", encoding="utf-8") as handle:
        state = json.load(handle)

    state.setdefault("seen_tx_hashes", {})
    if "last_checked_at" not in state:
        state["last_checked_at"] = isoformat_z(utc_now() - timedelta(minutes=bootstrap_lookback_minutes))
    return state


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=True, indent=2, sort_keys=True)


def prune_seen_transactions(seen_tx_hashes: dict[str, str]) -> dict[str, str]:
    if len(seen_tx_hashes) <= MAX_SEEN_TRANSACTIONS:
        return seen_tx_hashes

    sorted_items = sorted(seen_tx_hashes.items(), key=lambda item: item[1], reverse=True)
    return dict(sorted_items[:MAX_SEEN_TRANSACTIONS])


def http_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "X-Dune-API-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }
    )
    return session


def execute_dune_query(
    session: requests.Session,
    query_id: int,
    addresses: list[str],
    blockchains: set[str],
    start_time: datetime,
    end_time: datetime,
) -> str:
    payload = {
        "performance": "medium",
        "query_parameters": {
            "wallets_csv": ",".join(addresses),
            "blockchains_csv": ",".join(sorted(blockchains)),
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    }
    response = session.post(
        f"{DUNE_API_BASE}/query/{query_id}/execute",
        json=payload,
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    data = response.json()
    execution_id = data.get("execution_id")
    if not execution_id:
        raise RuntimeError(f"missing execution_id in Dune response: {data}")
    return execution_id


def batch_addresses(addresses: list[str], batch_size: int = MAX_WALLETS_PER_BATCH) -> list[list[str]]:
    return [addresses[index:index + batch_size] for index in range(0, len(addresses), batch_size)]


def execute_sol_dune_query(
    session: requests.Session,
    query_id: int,
    addresses: list[str],
    start_time: datetime,
    end_time: datetime,
) -> str:
    payload = {
        "performance": "medium",
        "query_parameters": {
            "wallets_csv": ",".join(addresses),
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    }
    response = session.post(
        f"{DUNE_API_BASE}/query/{query_id}/execute",
        json=payload,
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    data = response.json()
    execution_id = data.get("execution_id")
    if not execution_id:
        raise RuntimeError(f"missing execution_id in Dune response: {data}")
    return execution_id


def wait_for_results(session: requests.Session, execution_id: str) -> list[dict[str, Any]]:
    deadline = time.time() + DEFAULT_TIMEOUT_SECONDS
    last_state = None

    while time.time() < deadline:
        response = session.get(
            f"{DUNE_API_BASE}/execution/{execution_id}/results",
            params={"limit": 100},
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        state = data.get("state")
        last_state = state

        if state == "QUERY_STATE_COMPLETED":
            return data.get("result", {}).get("rows", []) or data.get("data", {}).get("rows", []) or []
        if state in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXPIRED"}:
            raise RuntimeError(f"Dune execution failed with state {state}: {data}")

        time.sleep(2)

    raise TimeoutError(f"Dune execution {execution_id} did not finish in time, last state={last_state}")


def format_amount(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(number) >= 1:
        return f"{number:,.4f}".rstrip("0").rstrip(".")
    return f"{number:.8f}".rstrip("0").rstrip(".")


def classify_matches(row: dict[str, Any], watch_map: dict[str, WatchAddress]) -> list[WatchAddress]:
    matches: list[WatchAddress] = []
    for candidate in (row.get("taker"), row.get("tx_from"), row.get("tx_to"), row.get("trader_id")):
        if not candidate:
            continue
        watched = watch_map.get(str(candidate).lower())
        if watched and watched not in matches:
            matches.append(watched)
    return matches


def tx_identifier(row: dict[str, Any]) -> str:
    tx_hash = str(row.get("tx_hash") or "").strip().lower()
    if tx_hash:
        return f"evm:{tx_hash}"

    tx_id = str(row.get("tx_id") or "").strip()
    if tx_id:
        return f"sol:{tx_id}"

    return ""


def format_alert(row: dict[str, Any], matches: list[WatchAddress]) -> str:
    labels = ", ".join(f"{item.label} ({item.address})" for item in matches)
    blockchain = row.get("blockchain", "unknown")
    project = row.get("project", "unknown")
    version = row.get("version_name") or row.get("version") or "unknown"
    sold_symbol = row.get("token_sold_symbol") or "unknown"
    bought_symbol = row.get("token_bought_symbol") or "unknown"
    sold_amount = format_amount(row.get("token_sold_amount"))
    bought_amount = format_amount(row.get("token_bought_amount"))
    amount_usd = format_amount(row.get("amount_usd"))
    block_time = row.get("block_time", "unknown")
    tx_ref = row.get("tx_hash") or row.get("tx_id") or "unknown"
    return (
        f"[{block_time}] swap detected on {blockchain}/{project} ({version})\n"
        f"watched wallet(s): {labels}\n"
        f"trade: {sold_amount} {sold_symbol} -> {bought_amount} {bought_symbol} (usd={amount_usd})\n"
        f"tx: {tx_ref}"
    )


def append_alert_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        handle.write("\n\n")


def send_telegram_alert(bot_token: str, chat_id: str, message: str) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": message},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def send_webhook(url: str, message: str) -> None:
    response = requests.post(url, json={"text": message, "content": message}, timeout=DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()


def dispatch_alerts(message: str, log_file: Path) -> None:
    print(message, flush=True)
    append_alert_log(log_file, message)

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if telegram_bot_token and telegram_chat_id:
        send_telegram_alert(telegram_bot_token, telegram_chat_id, message)

    for env_name in ("SLACK_WEBHOOK_URL", "DISCORD_WEBHOOK_URL", "GENERIC_WEBHOOK_URL"):
        webhook_url = os.getenv(env_name, "").strip()
        if webhook_url:
            send_webhook(webhook_url, message)


def run_once(
    session: requests.Session,
    evm_query_id: int,
    sol_query_id: int,
    csv_path: Path,
    state_file: Path,
    alert_log_file: Path,
    bootstrap_lookback_minutes: int,
) -> None:
    state = load_state(state_file, bootstrap_lookback_minutes=bootstrap_lookback_minutes)
    start_time = parse_iso_datetime(state["last_checked_at"])
    end_time = utc_now()

    evm_watchlist, sol_watchlist = load_watchlist(csv_path)
    all_watches = evm_watchlist + sol_watchlist
    watch_map = {watch.address.lower(): watch for watch in all_watches}

    rows: list[dict[str, Any]] = []
    if evm_watchlist:
        evm_addresses = [watch.address for watch in evm_watchlist]
        for address_batch in batch_addresses(evm_addresses):
            evm_execution_id = execute_dune_query(
                session=session,
                query_id=evm_query_id,
                addresses=address_batch,
                blockchains=set(EVM_MONITOR_CHAINS),
                start_time=start_time,
                end_time=end_time,
            )
            rows.extend(wait_for_results(session, evm_execution_id))

    if sol_watchlist:
        sol_addresses = [watch.address for watch in sol_watchlist]
        for address_batch in batch_addresses(sol_addresses):
            sol_execution_id = execute_sol_dune_query(
                session=session,
                query_id=sol_query_id,
                addresses=address_batch,
                start_time=start_time,
                end_time=end_time,
            )
            rows.extend(wait_for_results(session, sol_execution_id))

    seen_tx_hashes: dict[str, str] = state.get("seen_tx_hashes", {})
    fresh_rows: list[tuple[dict[str, Any], list[WatchAddress]]] = []
    for row in rows:
        identifier = tx_identifier(row)
        if not identifier or identifier in seen_tx_hashes:
            continue

        matches = classify_matches(row, watch_map)
        if not matches:
            continue

        fresh_rows.append((row, matches))
        seen_tx_hashes[identifier] = isoformat_z(end_time)

    for row, matches in reversed(fresh_rows):
        dispatch_alerts(format_alert(row, matches), alert_log_file)

    state["last_checked_at"] = isoformat_z(end_time)
    state["seen_tx_hashes"] = prune_seen_transactions(seen_tx_hashes)
    save_state(state_file, state)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor smart money swap activity.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single polling cycle and exit. Useful for hosted cron jobs.",
    )
    parser.add_argument(
        "--bootstrap-lookback-minutes",
        type=int,
        default=None,
        help="Lookback window to use when no prior state file exists.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()

    api_key = os.getenv("DUNE_API_KEY", "").strip()
    if not api_key:
        print("DUNE_API_KEY is required.", file=sys.stderr)
        return 1

    evm_query_id = int(os.getenv("DUNE_EVM_QUERY_ID", os.getenv("DUNE_QUERY_ID", str(DEFAULT_EVM_QUERY_ID))))
    sol_query_id = int(os.getenv("DUNE_SOL_QUERY_ID", str(DEFAULT_SOL_QUERY_ID)))
    csv_path = Path(os.getenv("SMART_MONEY_CSV", "smart_money_active.csv")).expanduser().resolve()
    state_file = Path(os.getenv("STATE_FILE", "monitor_state.json")).expanduser().resolve()
    alert_log_file = Path(os.getenv("ALERT_LOG_FILE", "alerts.log")).expanduser().resolve()
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS)))
    bootstrap_lookback_minutes = args.bootstrap_lookback_minutes
    if bootstrap_lookback_minutes is None:
        bootstrap_lookback_minutes = max(DEFAULT_BOOTSTRAP_LOOKBACK_MINUTES, poll_interval // 60)

    session = http_session(api_key)
    print(
        f"watching {csv_path} with Dune queries evm={evm_query_id}, sol={sol_query_id}; polling every {poll_interval}s",
        flush=True,
    )

    while True:
        try:
            run_once(
                session=session,
                evm_query_id=evm_query_id,
                sol_query_id=sol_query_id,
                csv_path=csv_path,
                state_file=state_file,
                alert_log_file=alert_log_file,
                bootstrap_lookback_minutes=bootstrap_lookback_minutes,
            )
            if args.once:
                return 0
        except KeyboardInterrupt:
            print("monitor stopped by user", flush=True)
            return 0
        except Exception as exc:
            error_message = f"[{isoformat_z(utc_now())}] monitor error: {exc}"
            print(error_message, file=sys.stderr, flush=True)
            append_alert_log(alert_log_file, error_message)
            if args.once:
                return 1

        time.sleep(poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
