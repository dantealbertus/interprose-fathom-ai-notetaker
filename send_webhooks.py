import requests
import json
import time
import os
from datetime import datetime, timezone

import redis

FATHOM_API_KEY = os.environ["FATHOM_API_KEY_GREG"]
FATHOM_ACCOUNT = os.environ.get("FATHOM_ACCOUNT", "greg")
WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
FATHOM_API_URL = "https://api.fathom.ai/external/v1/meetings"
REDIS_KEY = f"fathom:last_processed_at:{FATHOM_ACCOUNT}"

redis_client = redis.from_url(os.environ["REDIS_URL"])


def load_state():
    value = redis_client.get(REDIS_KEY)
    if value:
        return {"last_processed_at": value.decode()}
    # Eerste keer: sla huidige tijd op als startpunt, verwerk geen oude meetings
    now = datetime.now(timezone.utc).isoformat()
    save_state({"last_processed_at": now})
    return {"last_processed_at": now}


def save_state(state):
    redis_client.set(REDIS_KEY, state["last_processed_at"])


def fetch_new_meetings(created_after, retries=3, backoff=5):
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
                    headers={"X-Api-Key": FATHOM_API_KEY},
                    params=params,
                )
                response.raise_for_status()
                break
            except requests.HTTPError as e:
                if attempt < retries - 1 and response.status_code in (502, 503, 504):
                    print(f"Tijdelijke fout ({response.status_code}), opnieuw proberen in {backoff}s...")
                    time.sleep(backoff)
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
    response.raise_for_status()
    return response


def process_meetings(meetings):
    total_sent = 0

    for meeting in meetings:
        invitees = meeting.get("calendar_invitees", [])

        for invitee in invitees:
            payload = {
                "account": FATHOM_ACCOUNT,
                "meeting_datetime": meeting.get("recording_start_time"),
                "recording_id": meeting.get("recording_id"),
                "share_url": meeting.get("share_url"),
                "invitee_name": invitee.get("name"),
                "invitee_email": invitee.get("email"),
                "meeting_title": meeting.get("meeting_title"),
                "meeting_transcript": format_transcript(meeting.get("transcript")),
                "meeting_summary": meeting.get("default_summary", {}).get("markdown_formatted") if meeting.get("default_summary") else None,
                "invitees": ", ".join(i.get("name") for i in meeting.get("calendar_invitees", [])),
            }

            print(f"  → {meeting['meeting_title']} | {invitee['email']}")
            send_webhook(payload)
            total_sent += 1
            time.sleep(0.2)

    return total_sent


if __name__ == "__main__":
    state = load_state()
    last_processed_at = state["last_processed_at"]

    print(f"Nieuwe meetings ophalen vanaf: {last_processed_at}")
    meetings = fetch_new_meetings(created_after=last_processed_at)

    if not meetings:
        print("Geen nieuwe meetings gevonden.")
    else:
        print(f"{len(meetings)} nieuwe meeting(s) gevonden.\n")
        total = process_meetings(meetings)
        print(f"\n{total} webhooks verstuurd.")

        # Sla de created_at van de nieuwste meeting op als nieuw startpunt
        latest = max(meetings, key=lambda m: m["created_at"])
        state["last_processed_at"] = latest["created_at"]
        save_state(state)
        print(f"State bijgewerkt naar: {state['last_processed_at']}")
