# Photon Bridge Sidecar

This package is the Bun/TypeScript sidecar for Spacebot's Photon iMessage adapter.

It exists because outbound Photon messaging currently requires the `spectrum-ts` SDK
(vendored `@spacebot/photon-bridge` pins a compatible semver such as **1.8.x** —
`imessage` is imported from **`spectrum-ts/providers/imessage`**, not the package root).

The Rust adapter (`src/messaging/photon.rs`) talks to this sidecar over stdin/stdout
using newline-delimited JSON commands/events.

## Runtime contract

- **Input (stdin):** command envelopes from Rust:
  - `send_text`
  - `send_reply`
  - `send_file`
  - `add_reaction`
  - `remove_reaction`
  - `start_typing`
  - `stop_typing`
  - `health`
  - `shutdown`
- **Output (stdout):**
  - `ready` once startup is complete
  - `inbound` for Photon message events
  - `response` for command acks/errors
  - `log` for structured sidecar logs

`remove_reaction` is currently a provider-level no-op for Photon iMessage
because `spectrum-ts` does not expose a tapback-removal API.

## Environment variables

- `PHOTON_PROJECT_ID` (required)
- `PHOTON_PROJECT_SECRET` (required)
- `PHOTON_ADAPTER_KEY` (optional, for log/event context)
- `PHOTON_DM_ALLOW_LIST` (optional CSV, available for future policy handling)

Inbound **images, files, voice notes**, and other Spectrum `content` payloads (including grouped / reply-wrapped media) are read via the SDK’s `read()` hooks, base64-encoded on the sidecar JSON line, and forwarded to Spacebot as attachments so channel models can use vision / transcription the same way as Discord or Telegram.

## Local dev

```bash
bun install
bun run start
```
