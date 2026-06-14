#!/usr/bin/env bash
# pylon kernel tuning for millions of idle WebSocket connections. Run as root.
set -euo pipefail
sysctl -w net.ipv4.tcp_rmem="1024 4096 16384"
sysctl -w net.ipv4.tcp_wmem="1024 4096 16384"
sysctl -w net.core.rmem_max=16384
sysctl -w net.core.wmem_max=16384
sysctl -w net.ipv4.tcp_mem="10000000 10000000 10000000"
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.core.netdev_max_backlog=10000
sysctl -w net.ipv4.tcp_max_orphans=262144
sysctl -w net.ipv4.tcp_migrate_req=1
sysctl -w fs.file-max=12000500
sysctl -w fs.nr_open=20000500
echo "Set ulimit -n >= target connection count per process (e.g. 2000000) in the unit file."
