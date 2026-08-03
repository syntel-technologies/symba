# 09 — Cron schedules

Symba has a built-in deterministic cron scheduler (§6.5): one elected engine ticks,
submits due jobs, and dedups on `cron:{id}:{next_fire}` so a schedule fires **once** per
window even across multiple engines — and it never backfills missed fires.

```bash
./run.sh
```

**What it shows**

- `GET /v1/cron` lists schedules with their last/next fire and enabled flag.
- `PUT /v1/cron/{id} {enabled}` toggles a schedule on/off. While disabled, windows that
  pass are simply skipped (no backfill storm when you re-enable).

The script no-ops gracefully if no schedules are defined. Schedules are managed via the
admin plane / seed migrations; the operator UI's Cron view drives these same endpoints.
