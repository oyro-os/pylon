# TLS / SSL

Pylon listens on plain `ws://` and HTTP by default. Two approaches are available
for adding TLS — choose one based on your deployment topology.

=== "Reverse proxy (recommended)"

    Terminate TLS at a dedicated proxy and forward plain HTTP/WebSocket to pylon on
    its internal port (default `7000`). This is the standard approach for production,
    cloud load balancers, and Kubernetes.

    **Advantages:** the proxy handles certificate provisioning and renewal, cipher
    negotiation, and HTTP/2 multiplexing without any changes to the pylon process.
    Pylon keeps a lower per-connection memory footprint than native TLS mode.

    ### Caddy — automatic HTTPS

    Caddy provisions and renews certificates via Let's Encrypt automatically. It
    proxies WebSocket Upgrade frames transparently with no special directives needed.

    Requirements:

    - A real, publicly resolvable domain name.
    - Ports 80 and 443 reachable from the internet (ACME HTTP-01 challenge).

    Use the annotated example as your starting point:

    ```bash
    # Install Caddy, then run:
    caddy run --config deploy/tls/Caddyfile.example
    ```

    `deploy/tls/Caddyfile.example` contains the full configuration including an
    optional block to restrict `/metrics` to internal networks.

    ### nginx — manual certificate management

    Use `deploy/tls/nginx.conf.example` as your starting point. Key points:

    1. **Obtain a certificate first:**
       ```bash
       certbot certonly --nginx -d your.domain.example
       ```
       Certbot writes certificates to `/etc/letsencrypt/live/<domain>/` and
       configures automatic renewal.

    2. **WebSocket Upgrade headers** — the most common misconfiguration. These three
       directives are required in every `location` block that proxies to pylon:
       ```nginx
       proxy_http_version 1.1;
       proxy_set_header Upgrade    $http_upgrade;
       proxy_set_header Connection "upgrade";
       ```
       Without them, nginx will not upgrade the HTTP connection to a WebSocket, and
       clients will get an HTTP 101 that immediately drops.

    3. **Long timeouts.** The Pusher client sends a heartbeat every 120 s. nginx's
       default `proxy_read_timeout` of 60 s silently kills idle connections. Set both
       read and send timeouts well above the heartbeat period:
       ```nginx
       proxy_read_timeout  3600s;
       proxy_send_timeout  3600s;
       ```

    4. **Multiple upstream nodes.** Add each pylon node to the `upstream pylon {}`
       block. nginx round-robins requests; the redis adapter (`PYLON_ADAPTER=redis`)
       keeps cluster state consistent across nodes.

    ### Kubernetes — terminate at Ingress

    For Kubernetes deployments, terminate TLS at the Ingress controller using
    cert-manager. The pylon pods and Service stay on plain HTTP — no changes to the
    pylon Deployment are needed.

    ```yaml
    apiVersion: networking.k8s.io/v1
    kind: Ingress
    metadata:
      name: pylon
      annotations:
        cert-manager.io/cluster-issuer: "letsencrypt-prod"
        # Raise read timeout above the Pusher heartbeat period (120 s).
        nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
        nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
        # Required for WebSocket upgrade on nginx-ingress:
        nginx.ingress.kubernetes.io/proxy-http-version: "1.1"
    spec:
      ingressClassName: nginx
      tls:
        - hosts:
            - your.domain.example
          secretName: pylon-tls
      rules:
        - host: your.domain.example
          http:
            paths:
              - path: /
                pathType: Prefix
                backend:
                  service:
                    name: pylon
                    port:
                      number: 7000
    ```

    cert-manager creates the `pylon-tls` Secret and renews it automatically.

=== "Native TLS"

    Pylon can serve `wss://` and the REST API on the same port directly, without
    a proxy in front. This is suitable for single-node deployments or environments
    where adding a proxy layer is not practical.

    ### Enabling native TLS

    Set both environment variables to PEM files:

    ```env
    PYLON_TLS_CERT=/path/to/fullchain.pem
    PYLON_TLS_KEY=/path/to/privkey.pem
    ```

    **Both variables must be set together, or both must be unset.** Setting only one
    is a fatal configuration error — pylon will refuse to start and print which
    variable is missing.

    | Variable | Required | Description |
    |---|---|---|
    | `PYLON_TLS_CERT` | Yes (with KEY) | Path to the PEM certificate chain (leaf first, then intermediates). |
    | `PYLON_TLS_KEY` | Yes (with CERT) | Path to the PEM private key (PKCS#8, RSA, or EC). |
    | `PYLON_TLS_CA` | No | Path to a PEM CA certificate. When set, enables mTLS — every client must present a valid certificate signed by this CA. |

    ### mTLS (mutual TLS)

    To require client certificates, additionally set:

    ```env
    PYLON_TLS_CA=/path/to/ca.pem
    ```

    Pylon will build a `WebPkiClientVerifier` from the CA and reject any connection
    that does not present a valid client certificate signed by that CA. This is
    useful for server-to-server scenarios where the client pool is controlled.

    ### Memory cost

    Native TLS increases per-connection memory relative to plain mode because
    rustls allocates send and receive buffers per TLS session (typically 32–64 KiB
    per connection). For deployments targeting millions of concurrent connections,
    the reverse-proxy approach is preferred — the TLS overhead lives in the proxy
    process rather than pylon, where connection density is maximised.

---

## Protecting /metrics

`/metrics` exposes connection counts, Redis lag, and memory statistics. It should
not be publicly reachable.

- **Caddy:** uncomment the `@metrics_public` matcher block in
  `deploy/tls/Caddyfile.example` and adjust the CIDR.
- **nginx:** uncomment the `location /metrics { allow … ; deny all; }` block in
  `deploy/tls/nginx.conf.example`.
- **Kubernetes:** add a `whitelist-source-range` annotation on a separate Ingress
  rule for `/metrics`, or use a Prometheus `ServiceMonitor` that scrapes the pod IP
  directly (bypassing the Ingress entirely).
