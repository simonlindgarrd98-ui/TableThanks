# Table review agent

Texts guests 2 hours after their check is paid, never between 00:00 and 08:00 (held until 08:00),
asks how it went, and handles replies with Claude. Complaints and health issues alert your team.

## Setup
1. `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. `cp .env.example .env`, fill it in (the app loads `.env` itself; on a host, enter the values in its Variables page instead)
3. `uvicorn app:app --host 0.0.0.0 --port 8000` (needs a public HTTPS URL: set it as `PUBLIC_URL`)
4. In Twilio, set your number's "A message comes in" webhook to `PUBLIC_URL/webhook/twilio-inbound` (HTTP POST).

## Connect your POS (table done)
Have your POS, or a Zapier/Make step watching it, POST when a check is paid:

    curl -X POST $PUBLIC_URL/webhook/table-done \
      -H "X-Webhook-Secret: $WEBHOOK_SECRET" -H "Content-Type: application/json" \
      -d '{"first_name":"Sam","phone":"555 123 4567","consent":true}'

`consent` must be true: only send it for guests who agreed to a follow-up text
(reservation form, QR code, or checkout). No consent means no text.

## Rules built in
- Quiet hours enforced in code (scheduler will not send in the window), not just in the prompt
- Same Google link for everyone, no review gating
- STOP and natural-language stop requests go on a permanent opt-out list, checked before every send
- One follow-up at most, only when `READ_RECEIPTS=true` and the channel shows delivered-but-not-read
  (plain SMS can't, so by default no follow-ups are sent)
- `FLAG:` and `OPTOUT` lines from the agent are stripped before anything reaches the guest
- Replies to a guest's own message can go out any time; quiet hours apply to messages we start

## Test without sending
Set `TWILIO_VALIDATE=false` and watch the logs. Use a Twilio test number first.
Note: this is a starter. Add logging/monitoring and a database backup before relying on it.
