import requests
import json
import time
import os
from datetime import datetime, timezone

import redis

WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
FATHOM_API_URL = "https://api.fathom.ai/external/v1/meetings"

redis_client = redis.from_url(os.environ["REDIS_URL"])


def get_accounts():
    accounts = os.environ.get("FATHOM_ACCOUNTS", "greg")
    return [a.strip() for a in accounts.split(",")]


def get_api_key(account):
    return os.environ[f"FATHOM_API_KEY_{account.upper()}"]


def load_state(account):
    key = f"fathom:last_processed_at:{account}"
    value = redis_client.get(key)
    if value:
        return {"last_processed_at": value.decode()}
    now = datetime.now(timezone.utc).isoformat()
    save_state(account, {"last_processed_at": now})
    return {"last_processed_at": now}


def save_state(account, state):
    key = f"fathom:last_processed_at:{account}"
    redis_client.set(key, state["last_processed_at"])


def is_already_processed(account, recording_id):
    key = f"fathom:processed_ids:{account}"
    return redis_client.sismember(key, str(recording_id))


def mark_as_processed(account, recording_id):
    key = f"fathom:processed_ids:{account}"
    redis_client.sadd(key, str(recording_id))


def fetch_new_meetings(api_key, created_after, retries=3, backoff=10):
    meetings = []
    cursor = None

    while True:
        params = {"limit": 10, "created_after": created_after, "include_transcript": "true", "include_summary": "true"}
        if cursor:
            params["cursor"] = cursor

        for attempt in range(retries):
            try:
                response = requests.get(
                    FATHOM_API_URL,
                    headers={"X-Api-Key": api_key},
                    params=params,
                )
                response.raise_for_status()
                break
            except requests.HTTPError as e:
                if attempt < retries - 1 and response.status_code in (502, 503, 504):
                    wait = backoff * (2 ** attempt)
                    print(f"Tijdelijke fout ({response.status_code}), opnieuw proberen in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

        data = response.json()
        meetings.extend(data["items"])
        cursor = data.get("next_cursor")

        if not cursor:
            break

    return meetings


def format_transcript(transcript):
    if not transcript:
        return None
    return "\n".join(f"[{t['timestamp']}] {t['speaker']['display_name']}: {t['text']}" for t in transcript)


def send_webhook(payload):
    response = requests.post(WEBHOOK_URL, json=payload)
    if not response.ok:
        print(f"Webhook fout {response.status_code}: {response.text}")
    response.raise_for_status()
    return response


def process_meetings(account, meetings):
    total_sent = 0

    for meeting in meetings:
        recording_id = meeting.get("recording_id")

        if is_already_processed(account, recording_id):
            print(f"  Overgeslagen (duplicaat): {meeting['meeting_title']} ({recording_id})")
            continue

        if meeting.get("calendar_invitees_domains_type") == "only_internal":
            print(f"  Overgeslagen (intern): {meeting['meeting_title']}")
            mark_as_processed(account, recording_id)
            continue

        payload = {
            "account": account,
            "meeting_datetime": meeting.get("recording_start_time"),
            "recording_id": recording_id,
            "share_url": meeting.get("share_url"),
            "meeting_title": meeting.get("meeting_title"),
            "meeting_transcript": format_transcript(meeting.get("transcript")),
            "meeting_summary": meeting.get("default_summary", {}).get("markdown_formatted") if meeting.get("default_summary") else None,
            "invitees": [
                {"name": i.get("name"), "email": i.get("email")}
                for i in meeting.get("calendar_invitees", [])
            ],
        }

        print(f"  → {meeting['meeting_title']} ({len(payload['invitees'])} invitees)")
        send_webhook(payload)
        mark_as_processed(account, recording_id)
        total_sent += 1
        time.sleep(0.2)

    return total_sent


if __name__ == "__main__":
    accounts = get_accounts()
    print(f"Accounts: {', '.join(accounts)}\n")

    for account in accounts:
        print(f"[{account}] Meetings ophalen...")
        api_key = get_api_key(account)
        state = load_state(account)
        last_processed_at = state["last_processed_at"]

        print(f"[{account}] Ophalen vanaf: {last_processed_at}")
        meetings = fetch_new_meetings(api_key, created_after=last_processed_at)

        if not meetings:
            print(f"[{account}] Geen nieuwe meetings gevonden.")
        else:
            print(f"[{account}] {len(meetings)} nieuwe meeting(s) gevonden.")
            total = process_meetings(account, meetings)
            print(f"[{account}] {total} webhooks verstuurd.")

            latest = max(meetings, key=lambda m: m["created_at"])
            state["last_processed_at"] = latest["created_at"]
            save_state(account, state)
            print(f"[{account}] State bijgewerkt naar: {state['last_processed_at']}")

        print()
