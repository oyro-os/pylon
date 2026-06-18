# Connecting Clients

Pylon speaks the Pusher Channels v7 WebSocket protocol, so any Pusher-compatible client SDK works
against it. You only need to point the SDK at your Pylon host instead of the hosted Pusher service.

---

=== "pusher-js"

    Install the official Pusher JavaScript client:

    ```bash
    npm install pusher-js
    ```

    Then initialise it with your Pylon host and port:

    ```js
    import Pusher from "pusher-js";

    const pusher = new Pusher("<your-app-key>", {
      wsHost: "127.0.0.1",
      wsPort: 7000,
      forceTLS: false,
      enabledTransports: ["ws"],
      cluster: "",
    });
    ```

    Key options:

    | Option | Value | Notes |
    |---|---|---|
    | `wsHost` | your Pylon host | IP or hostname |
    | `wsPort` | `7000` | Default Pylon port |
    | `forceTLS` | `false` | Set to `true` when using `wss://` |
    | `enabledTransports` | `["ws"]` | `["wss"]` when TLS is active |
    | `cluster` | `""` | Required to prevent pusher-js falling back to hosted Pusher |

    **Private and presence channels** require an auth endpoint on your backend. Configure it via:

    ```js
    const pusher = new Pusher("<your-app-key>", {
      wsHost: "127.0.0.1",
      wsPort: 7000,
      forceTLS: false,
      enabledTransports: ["ws"],
      cluster: "",
      authEndpoint: "/pusher/auth",   // your backend's auth URL
    });
    ```

    **Behind a TLS-terminating proxy** (recommended for production), set `forceTLS: true` and
    `wsPort: 443`:

    ```js
    const pusher = new Pusher("<your-app-key>", {
      wsHost: "pylon.example.com",
      wsPort: 443,
      forceTLS: true,
      enabledTransports: ["wss"],
      cluster: "",
    });
    ```

    Subscribing to channels works exactly as with hosted Pusher:

    ```js
    // Public
    const pub = pusher.subscribe("my-channel");

    // Private (auth required)
    const priv = pusher.subscribe("private-my-channel");

    // Presence (auth + roster)
    const pres = pusher.subscribe("presence-my-room");
    pres.bind("pusher:subscription_succeeded", (members) => {
      members.each((m) => console.log(m.id));
    });
    ```

=== "Laravel Echo"

    Laravel Echo wraps pusher-js for Laravel applications. Add the required packages:

    ```bash
    npm install --save-dev laravel-echo pusher-js
    ```

    #### JavaScript configuration

    In `resources/js/bootstrap.js` (or wherever you initialise Echo), configure the Pusher
    broadcaster pointing at Pylon:

    ```js
    import Echo from "laravel-echo";
    import Pusher from "pusher-js";

    window.Pusher = Pusher;

    window.Echo = new Echo({
      broadcaster: "pusher",
      key: import.meta.env.VITE_PUSHER_APP_KEY,
      cluster: import.meta.env.VITE_PUSHER_APP_CLUSTER ?? "mt1",
      wsHost: import.meta.env.VITE_PUSHER_HOST ?? "127.0.0.1",
      wsPort: import.meta.env.VITE_PUSHER_PORT ?? 7000,
      wssPort: import.meta.env.VITE_PUSHER_PORT ?? 7000,
      forceTLS: (import.meta.env.VITE_PUSHER_SCHEME ?? "http") === "https",
      enabledTransports: ["ws", "wss"],
    });
    ```

    #### Laravel `.env`

    ```ini
    BROADCAST_CONNECTION=pusher

    PUSHER_APP_ID=your-app-id
    PUSHER_APP_KEY=your-app-key
    PUSHER_APP_SECRET=your-app-secret
    PUSHER_HOST=127.0.0.1
    PUSHER_PORT=7000
    PUSHER_SCHEME=http
    PUSHER_APP_CLUSTER=mt1
    ```

    !!! note "Older Laravel versions"
        Laravel 9 and earlier use `BROADCAST_DRIVER` instead of `BROADCAST_CONNECTION`.

    #### `config/broadcasting.php`

    The `pusher` broadcaster entry reads from these env vars automatically. Ensure the `options`
    block in `config/broadcasting.php` does **not** hard-code a Pusher cluster or host that would
    override your `.env` values:

    ```php
    'pusher' => [
        'driver'  => 'pusher',
        'key'     => env('PUSHER_APP_KEY'),
        'secret'  => env('PUSHER_APP_SECRET'),
        'app_id'  => env('PUSHER_APP_ID'),
        'options' => [
            'host'    => env('PUSHER_HOST', '127.0.0.1'),
            'port'    => env('PUSHER_PORT', 7000),
            'scheme'  => env('PUSHER_SCHEME', 'http'),
            'cluster' => env('PUSHER_APP_CLUSTER', 'mt1'),
        ],
    ],
    ```

    #### Subscribing

    Subscribing to public, private, and presence channels works unchanged — Laravel Echo's
    channel API is identical whether the backend is hosted Pusher or Pylon:

    ```js
    // Public
    Echo.channel("my-channel").listen("OrderShipped", (e) => {
      console.log(e.order);
    });

    // Private
    Echo.private("orders").listen("OrderUpdated", (e) => {
      console.log(e);
    });

    // Presence
    Echo.join("chat")
      .here((users) => console.log(users))
      .joining((user) => console.log(user.name, "joined"))
      .leaving((user) => console.log(user.name, "left"))
      .listen("NewMessage", (e) => console.log(e));
    ```
