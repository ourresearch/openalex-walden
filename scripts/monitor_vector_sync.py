#!/usr/bin/env python3
"""
Monitor Databricks Vector Search index sync progress.

Polls the index status at regular intervals, logs progress, and sends
Slack notifications on completion or failure.

Usage:
    python monitor_vector_sync.py [--interval 300] [--slack-channel alerts]

Environment variables required:
    DATABRICKS_HOST - Databricks workspace URL
    DATABRICKS_TOKEN - Databricks PAT token
    SLACK_BOT_TOKEN - Slack bot token (optional, for notifications)

Example:
    # Monitor every 5 minutes, notify #alerts channel
    python monitor_vector_sync.py --interval 300 --slack-channel alerts

    # Monitor every minute, no Slack (just logs)
    python monitor_vector_sync.py --interval 60
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
INDEX_NAME = "openalex.vector_search.work_embeddings_index"
ENDPOINT_NAME = "openalex-vector-search"


def get_index_status(host: str, token: str) -> dict:
    """Fetch current index status from Databricks API."""
    url = f"https://{host}/api/2.0/vector-search/indexes/{INDEX_NAME}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_status(status_data: dict) -> dict:
    """Parse the status response into a simpler structure."""
    status = status_data.get("status", {})

    result = {
        "state": status.get("detailed_state", "UNKNOWN"),
        "ready": status.get("ready", False),
        "indexed_row_count": status.get("indexed_row_count"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Get sync progress from various possible locations
    sync_progress = None

    # Check triggered update progress
    triggered = status.get("triggered_update_status", {})
    triggered_progress = triggered.get("triggered_update_progress", {})
    if triggered_progress:
        sync_progress = triggered_progress.get("sync_progress_completion")

    # Check initial sync progress
    if sync_progress is None:
        prov = status.get("provisioning_status", {})
        initial = prov.get("initial_pipeline_sync_progress", {})
        if initial:
            sync_progress = initial.get("sync_progress_completion")

    if sync_progress is not None:
        result["sync_progress_pct"] = round(sync_progress * 100, 2)

    # Check for failure info
    failed = status.get("failed_status", {})
    if failed:
        result["error_code"] = failed.get("error_code")
        result["error_message"] = failed.get("error_message")
        result["last_processed_version"] = failed.get("last_processed_commit_version")

    return result


def send_slack_message(channel: str, message: str, token: str) -> bool:
    """Send a message to Slack channel."""
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": channel,
        "text": message,
        "unfurl_links": False
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        result = response.json()
        if not result.get("ok"):
            logger.error(f"Slack API error: {result.get('error')}")
            return False
        return True
    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
        return False


def format_slack_message(status: dict, event_type: str) -> str:
    """Format a status update for Slack."""
    emoji = {
        "progress": ":hourglass_flowing_sand:",
        "complete": ":white_check_mark:",
        "failed": ":x:",
        "started": ":rocket:"
    }.get(event_type, ":information_source:")

    if event_type == "complete":
        return (
            f"{emoji} *Vector Search Index Sync Complete*\n"
            f"State: `{status['state']}`\n"
            f"Indexed rows: {status.get('indexed_row_count', 'N/A'):,}\n"
            f"Ready: {status['ready']}"
        )
    elif event_type == "failed":
        return (
            f"{emoji} *Vector Search Index Sync Failed*\n"
            f"State: `{status['state']}`\n"
            f"Error: `{status.get('error_code', 'UNKNOWN')}`\n"
            f"Message: {status.get('error_message', 'No details')}\n"
            f"Last processed version: {status.get('last_processed_version', 'N/A')}"
        )
    elif event_type == "started":
        return (
            f"{emoji} *Vector Search Index Sync Started*\n"
            f"Monitoring sync progress..."
        )
    else:  # progress
        progress = status.get('sync_progress_pct', 0)
        return (
            f"{emoji} *Vector Search Sync Progress*: {progress}%\n"
            f"State: `{status['state']}`"
        )


def monitor_sync(
    host: str,
    token: str,
    interval_seconds: int = 300,
    slack_channel: Optional[str] = None,
    slack_token: Optional[str] = None,
    progress_interval: int = 10  # Report progress every N% change
):
    """
    Monitor the sync status until completion or failure.

    Args:
        host: Databricks workspace hostname
        token: Databricks PAT token
        interval_seconds: How often to poll (default 5 minutes)
        slack_channel: Slack channel ID to notify (optional)
        slack_token: Slack bot token (optional)
        progress_interval: Report to Slack every N% progress (default 10%)
    """
    logger.info(f"Starting sync monitor for {INDEX_NAME}")
    logger.info(f"Poll interval: {interval_seconds}s")

    last_progress = -1
    last_state = None
    iteration = 0

    # Initial status check
    try:
        status_data = get_index_status(host, token)
        status = parse_status(status_data)
        last_state = status["state"]

        logger.info(f"Initial state: {status['state']}")
        logger.info(f"Ready: {status['ready']}")

        if "sync_progress_pct" in status:
            logger.info(f"Sync progress: {status['sync_progress_pct']}%")
            last_progress = int(status['sync_progress_pct'])

        # Send start notification
        if slack_channel and slack_token:
            msg = format_slack_message(status, "started")
            send_slack_message(slack_channel, msg, slack_token)

    except Exception as e:
        logger.error(f"Failed to get initial status: {e}")
        return

    # Main monitoring loop
    while True:
        iteration += 1
        time.sleep(interval_seconds)

        try:
            status_data = get_index_status(host, token)
            status = parse_status(status_data)

            current_state = status["state"]
            current_progress = status.get("sync_progress_pct", 0)

            # Log current status
            logger.info(
                f"[{iteration}] State: {current_state}, "
                f"Progress: {current_progress}%, "
                f"Ready: {status['ready']}"
            )

            # Check for state changes
            if current_state != last_state:
                logger.info(f"State changed: {last_state} -> {current_state}")
                last_state = current_state

            # Check for completion
            if current_state == "ONLINE" and "TRIGGERED_UPDATE" not in current_state:
                logger.info("Sync complete!")
                if slack_channel and slack_token:
                    msg = format_slack_message(status, "complete")
                    send_slack_message(slack_channel, msg, slack_token)
                break

            # Check for failure
            if "FAILED" in current_state:
                logger.error(f"Sync failed: {status.get('error_message')}")
                if slack_channel and slack_token:
                    msg = format_slack_message(status, "failed")
                    send_slack_message(slack_channel, msg, slack_token)
                break

            # Report significant progress
            progress_bucket = int(current_progress / progress_interval) * progress_interval
            last_bucket = int(last_progress / progress_interval) * progress_interval

            if progress_bucket > last_bucket and slack_channel and slack_token:
                msg = format_slack_message(status, "progress")
                send_slack_message(slack_channel, msg, slack_token)

            last_progress = current_progress

        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
            break
        except Exception as e:
            logger.error(f"Error fetching status: {e}")
            # Continue monitoring even on transient errors


def main():
    parser = argparse.ArgumentParser(
        description="Monitor Databricks Vector Search index sync"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Poll interval in seconds (default: 300)"
    )
    parser.add_argument(
        "--slack-channel",
        type=str,
        default=None,
        help="Slack channel ID to notify (e.g., 'CRRBCGH36' for #alerts)"
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=10,
        help="Report to Slack every N%% progress (default: 10)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Check status once and exit (don't loop)"
    )

    args = parser.parse_args()

    # Get credentials from environment
    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").replace("http://", "")
    token = os.environ.get("DATABRICKS_TOKEN")
    slack_token = os.environ.get("SLACK_BOT_TOKEN")

    if not host or not token:
        logger.error("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables required")
        sys.exit(1)

    if args.slack_channel and not slack_token:
        logger.warning("SLACK_BOT_TOKEN not set, Slack notifications disabled")
        args.slack_channel = None

    if args.once:
        # Single status check
        try:
            status_data = get_index_status(host, token)
            status = parse_status(status_data)
            print(json.dumps(status, indent=2))
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            sys.exit(1)
    else:
        # Continuous monitoring
        monitor_sync(
            host=host,
            token=token,
            interval_seconds=args.interval,
            slack_channel=args.slack_channel,
            slack_token=slack_token,
            progress_interval=args.progress_interval
        )


if __name__ == "__main__":
    main()
