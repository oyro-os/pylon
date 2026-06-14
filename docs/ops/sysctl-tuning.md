# Kernel sysctl tuning for millions of idle WebSocket connections

pylon is designed to hold **millions of mostly-idle** WebSocket connections on a
single host. At that scale the bottleneck is not CPU but **per-connection kernel
memory** and a handful of global kernel counters that default to values sized for
a few thousand sockets. This document explains the settings applied by
[`scripts/tune.sh`](../../scripts/tune.sh), why each one matters, and the kernel
memory floor that dictates how many connections a box can actually hold.

These values are **applied for benchmarks and recommended for production**. Run
the script as root before starting pylon, and persist the values in
`/etc/sysctl.d/` (or a config-management equivalent) so they survive reboots.

```bash
sudo ./scripts/tune.sh
```

## The ~3.2 KB/connection kernel floor

An idle TCP socket is not free. The kernel keeps, per connection:

- a `struct sock` and associated bookkeeping (a few hundred bytes), plus
- a **receive buffer** and a **send buffer**, whose *minimum* sizes are set by the
  first value of `net.ipv4.tcp_rmem` / `net.ipv4.tcp_wmem`.

The send/receive buffers dominate. Linux will not shrink a socket buffer below the
`min` value in those tuples, so the **minimum** you choose is effectively a
per-connection tax paid by *every* connection, idle or not. With:

```
net.ipv4.tcp_rmem = 1024 4096 16384   # min default max
net.ipv4.tcp_wmem = 1024 4096 16384   # min default max
```

each idle socket floors at roughly **1024 + 1024 bytes of buffer minimums plus
~1 KB of `struct sock`/accounting overhead ≈ 3.2 KB/connection**. That number is
the planning constant: **N connections need ≈ N × 3.2 KB of unswappable kernel
memory** before any application-level state. Two million idle connections is
therefore on the order of **~6 GB of kernel memory alone** — independent of
pylon's own per-connection heap. Push the `min` values higher and that floor rises
linearly; this is why the minimums are deliberately small. Idle WebSocket
connections carry no in-flight payload, so a 1 KB minimum buffer is ample; the
`default`/`max` (4096/16384) still allow a healthy window for the rare connection
that bursts.

`net.core.rmem_max` / `net.core.wmem_max` are capped at 16384 to match the
`tcp_*mem` ceilings, so a single misbehaving or `SO_RCVBUF`-setting connection
cannot balloon its buffers and blow the memory budget.

## Settings reference

| Setting | Value | Why |
| --- | --- | --- |
| `net.ipv4.tcp_rmem` | `1024 4096 16384` | Per-socket **receive** buffer (min/default/max). The `min` (1024) is the floor every idle connection pays — keep it small to keep the ~3.2 KB/conn floor low. |
| `net.ipv4.tcp_wmem` | `1024 4096 16384` | Per-socket **send** buffer (min/default/max). Same floor logic as `tcp_rmem`. |
| `net.core.rmem_max` | `16384` | Hard cap on receive buffer size any socket can request, matching the `tcp_rmem` ceiling so no connection can exceed the budget. |
| `net.core.wmem_max` | `16384` | Hard cap on send buffer size, matching `tcp_wmem`. |
| `net.ipv4.tcp_mem` | `10000000 10000000 10000000` | System-wide TCP memory pressure thresholds **in pages** (low/pressure/high). Set high and flat so the kernel never enters TCP memory-pressure mode (which would prune buffers and reset connections) while holding millions of sockets. At 4 KB/page this is ~40 GB of headroom. |
| `net.core.somaxconn` | `65535` | Max length of the completed-connection (accept) queue. The default (often 128/4096) silently drops connections during accept bursts. |
| `net.ipv4.tcp_max_syn_backlog` | `65535` | Max length of the half-open (SYN_RECV) queue. Prevents SYN drops when thousands of clients connect simultaneously. |
| `net.core.netdev_max_backlog` | `10000` | Packets queued per CPU when the NIC delivers faster than the stack drains. Raised to absorb connect/handshake storms. |
| `net.ipv4.tcp_max_orphans` | `262144` | Max orphaned (no userspace fd) sockets before the kernel starts resetting them. Raised so mass disconnects/reconnects don't trigger resets of still-valid sockets. |
| `net.ipv4.tcp_migrate_req` | `1` | Lets the kernel migrate incoming connection requests to another listening socket if the original `SO_REUSEPORT` socket closes — important for the per-core `SO_REUSEPORT` accept model so in-flight handshakes aren't dropped on socket churn. |
| `fs.file-max` | `12000500` | System-wide max open file descriptors. Every socket is an fd; millions of connections need millions of fds available kernel-wide. |
| `fs.nr_open` | `20000500` | Per-process upper bound on fds (the ceiling `ulimit -n` can be raised to). Must exceed your target connection count per process. |

## Per-process fd limit (`ulimit -n`)

`fs.nr_open` only raises the *ceiling*; you still have to raise the actual limit
for the pylon process. **Set `ulimit -n` >= your target connection count per
process** (e.g. `2000000`). In a systemd unit this is:

```ini
[Service]
LimitNOFILE=2000000
```

The tuning script prints a reminder of this because it is the single most common
cause of pylon refusing connections at `EMFILE` (too many open files) well before
the memory budget is reached.

## conntrack: disable tracking on the WebSocket port (NOTRACK)

If the host runs **netfilter connection tracking** (`nf_conntrack`, typical when
iptables/firewalld is active), every connection consumes a conntrack entry. The
conntrack table (`net.netfilter.nf_conntrack_max`) becomes a second, easily-hit
ceiling, and table inserts add per-packet cost — neither of which buys you
anything for a long-lived L7 WebSocket service that does its own connection
management.

The recommendation is to **exempt the pylon WebSocket port from conntrack** with
`NOTRACK` rules in the `raw` table (both directions). Adjust `7000` to your
configured `PYLON_PORT`:

```bash
# Recommended: skip conntrack for the pylon WS port entirely (both directions).
# iptables -t raw -A PREROUTING -p tcp --dport 7000 -j NOTRACK
# iptables -t raw -A OUTPUT     -p tcp --sport 7000 -j NOTRACK
```

These are left commented because they depend on your firewall setup and port; the
intent is documented guidance, not a default-applied change. If you instead need
conntrack on (e.g. for NAT), raise `net.netfilter.nf_conntrack_max` and
`net.netfilter.nf_conntrack_buckets` to comfortably exceed your connection count
and budget the extra ~300 bytes/entry into your memory plan.
