import requests
import json
import time
import os
from datetime import datetime, timezone
from pathlib import Path

FATHOM_API_KEY = os.environ["FATHOM_API_KEY_GREG"]
WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
FATHOM_API_URL = "https://api.fathom.ai/external/v1/meetings"
STATE_FILE = Path(__file__).parent / "state.json"


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    # Eerste keer: sla huidige tijd op als startpunt, verwerk geen oude meetings
    now = datetime.now(timezone.utc).isoformat()
    state = {"last_processed_at": now}
    save_state(state)
    return state


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def fetch_new_meetings(created_after):
    meetings = []
    cursor = None

    while True:
        params = {"limit": 10, "created_after": created_after, "include_transcript": "true", "include_summary": "true"}
        if cursor:
            params["cursor"] = cursor

        response = requests.get(
            FATHOM_API_URL,
            headers={"X-Api-Key": FATHOM_API_KEY},
            params=params,
        )
        response.raise_for_status()
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
                "meeting_datetime": meeting.get("recording_start_time"),
                "recording_id": meeting.get("recording_id"),
                "share_url": meeting.get("share_url"),
                "invitee_name": invitee.get("name"),
                "invitee_email": invitee.get("email"),
                "meeting_title": meeting.get("meeting_title"),
                "meeting_transcript": format_transcript(meeting.get("transcript")),
                "meeting_summary": meeting.get("default_summary", {}).get("markdown_formatted") if meeting.get("default_summary") else None,
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
