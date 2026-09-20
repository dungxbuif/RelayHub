# OCR provisioning (2026-09-20)

An existing password-authenticated admin created the following production-stage
integration resources through the public API, using session cookie and CSRF:

| Resource | ID |
| --- | --- |
| ocr-proxy | app_BnJ-pUWlyM3AAv_C_YPZhQ |
| ocr-worker-mac | app_eD8zPc4TOgPHm01asPe8LA |
| ocr-jobs | sub_ef0b92cf-8b8e-4644-bc17-6d83fd22ecae |

All native replicas must use the same subscription. Visibility is 120 seconds,
renewable up to 300 seconds at a time, with a one-hour total lease ceiling.
Retention is seven days, batch size one, max in-flight four and max attempts twenty.
These resources are provisioned but do not indicate that OCR has cut over.

Run `backend/scripts/provision-ocr.mjs` with `RELAYHUB_ADMIN_EMAIL` and a password
on stdin. Credentials are saved outside git with mode 600, never printed.
The default operator path is `~/.config/relayhub/ocr.json`. The script refuses to
duplicate an app when its one-time credentials are missing and logs out afterward.

Before provisioning, a read-only DB inventory confirmed zero application, event,
delivery, queue, function and outbox records, and one admin. No delete/reset was
needed. Preserve admin and real OCR data; cleanup authorization covers tests only.

Verification: backend `go test ./...` and admin UI tests (19/19) pass after migrating
obsolete bearer fixtures to password/session/CSRF. Bearer-rejection tests remain.
Integration source lives in mac-ocr; see its `docs/RELAYHUB_INTEGRATION.md` for
cutover gates. Public API behavior is unchanged by provisioning and fixture updates.
