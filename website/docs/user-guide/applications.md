# Applications & Authentication

## The application registry

Pylon supports multiple applications on a single server instance. Each application is identified
by a unique `id` and has its own key/secret pair. Applications are defined in a JSON file — by
default `apps.json` (configurable via [`PYLON_APPS_PATH`](configuration.md)).

### apps.json format

```json
[
  {
    "name": "Example App",
    "id": "<your-app-id>",
    "key": "<your-app-key>",
    "secret": "<your-app-secret>",
    "host": "",
    "path": "",
    "client_messages_enabled": true,
    "capacity": 10000,
    "statistics_enabled": false,
    "webhooks": [
      {
        "url": "https://example.test/pusher/webhooks",
        "event_types": [
          "channel_occupied",
          "channel_vacated",
          "member_added",
          "member_removed",
          "client_event",
          "cache_miss"
        ],
        "headers": { "X-Custom": "value" }
      }
    ]
  }
]
```

### Field reference

| Field | Type | Description |
|---|---|---|
| `name` | string | Human-readable label for this app (not used in the protocol). |
| `id` | string | Unique app identifier. Included in REST API paths (`/apps/{id}/...`). |
| `key` | string | Public app key. Clients use this to identify the app when connecting. |
| `secret` | string | Shared secret for HMAC signing. Never sent to clients. |
| `host` | string | Optional hostname restriction. Empty string means unrestricted. |
| `path` | string | Optional path prefix for the WebSocket endpoint. Empty string means unrestricted. |
| `client_messages_enabled` | boolean | When `true`, clients may publish events to channels via `client_event`. Defaults to `true`. |
| `capacity` | integer | Maximum concurrent WebSocket connections for this app. Connections beyond this limit are refused with WebSocket close code **4004**. |
| `statistics_enabled` | boolean | Reserved for future metrics collection. Has no effect in the current release. |
| `webhooks` | array | Zero or more webhook targets. Each entry has a `url`, an `event_types` list, and an optional `headers` map. See the [Webhooks](webhooks.md) page for the full event-type reference. |

---

## Key and secret usage

**Clients** use the `key` to connect. A Pusher-compatible client library is initialised with the
app key and optionally a cluster/host pointing at your Pylon server:

```js
const pusher = new Pusher("<your-app-key>", {
  wsHost: "pylon.example.com",
  wsPort: 7000,
  forceTLS: false,
  enabledTransports: ["ws"],
});
```

**Server-side code** uses both the `key` and `secret` to authenticate REST calls and to generate
subscription auth tokens for private and presence channels. The Pusher HTTP client libraries
(`pusher-http-node`, `pusher-http-python`, etc.) accept these as `appId`, `key`, and `secret`
constructor parameters.

---

## HMAC authentication model

Pylon uses HMAC-SHA256 for all authentication operations, matching the Pusher v7 protocol.
The `secret` is the shared HMAC key; it never leaves the server.

### Private and presence channels

Clients subscribing to a `private-*` or `presence-*` channel must first obtain an auth token
from your own backend server. The backend signs the subscription using HMAC-SHA256 and returns
a token of the form `<app_key>:<hex_signature>`.

The signing strings are:

- **Private channel**: `HMAC-SHA256(secret, "<socket_id>:<channel>")`
- **Presence channel**: `HMAC-SHA256(secret, "<socket_id>:<channel>:<channel_data>")`
  where `channel_data` is the verbatim JSON string containing at minimum `{"user_id": "..."}`.

Pylon verifies this token with a constant-time comparison before allowing the subscription.

### User sign-in (`pusher:signin`)

The `pusher:signin` flow lets a client authenticate as a named user. The signing string is:

```
HMAC-SHA256(secret, "<socket_id>::user::<user_data>")
```

where `user_data` is the verbatim JSON string the client sends (must contain `"id"`).

### REST authentication

HTTP requests to the Pylon REST API (triggering events, querying channel state) are authenticated
using HMAC-SHA256 over a canonical query string derived from the request method, path, and
parameters. REST signing details are covered on the [Triggering Events](triggering-events.md) page.

---

## Per-app connection capacity

The `capacity` field sets a hard ceiling on concurrent WebSocket connections for that app.
When a new connection would push the count over the limit, Pylon sends a WebSocket close frame
with code **4004** (capacity exceeded) and refuses the connection.

This limit is enforced cluster-wide when using the Redis adapter — each node checks the
cluster-level count, not just its local count.

Set `capacity` to `0` to disable the limit (unrestricted). For most production deployments,
sizing capacity to match your expected peak concurrent users plus a comfortable headroom is
recommended.

---

## Security: keep apps.json out of version control

`apps.json` contains the `secret` for each app — treat it like a password file.

The repo's `.gitignore` already excludes `apps.json`. Keep it that way: **do not commit
`apps.json` to version control**. Use a secrets manager, environment-specific config injection,
or a mounted secret volume to deliver the file at runtime.

If a secret is ever exposed, generate a new key/secret pair and update `apps.json` — all
currently connected clients will need to re-authenticate.
