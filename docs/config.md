# Configuration

Everything here is read from the process environment. The Python layer
reads one variable; the rest are read by the `epics-ca-rs` and
`epics-pva-rs` crates compiled into the extension. Only variables that the
code in this build reads are listed. A separate section names variables
that the crates read only under Cargo features this build does not
enable.

## The extension

| Variable | Default | Read | Meaning |
| --- | --- | --- | --- |
| `EPICSRS_WORKERS` | `1` | once, when the runtime starts on first use | number of tokio worker threads (named `epicsrs-rt`); values below 1 become 1, an unparsable value is the default. A process that runs a PVA `Server` and a `Context` together saw intermittent `get` timeouts with one worker and none with four (see [pva-client.md](pva-client.md)) |

```python
import os, sys
from pathlib import Path
import epicsrs
epicsrs.context()   # first use starts the runtime
names = [p.read_text().strip() for p in Path("/proc/self/task").glob("*/comm")]
print(os.environ.get("EPICSRS_WORKERS"), names.count("epicsrs-rt"))
```

Run with the variable unset, at `3` and at `0`:

```
None 1
3 3
0 1
```

## CA client

Read by `epics-ca-rs` when the CA context is created (the first
`epicsrs.ca`, `epicsrs.aio`, `epicsrs.pv` call, or `CaContext()`), unless
the "Read" column says otherwise. Every front end shares one context, so a
change after the first call has no effect on it.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EPICS_CA_ADDR_LIST` | `""` | whitespace-separated search targets, `addr[:port]`; the default port is `EPICS_CA_SERVER_PORT` |
| `EPICS_CA_AUTO_ADDR_LIST` | `YES` | add the interface broadcast addresses to the search targets |
| `EPICS_CA_NAME_SERVERS` | `""` | whitespace-separated `host[:port]` TCP name servers; a bare host gets `EPICS_CA_SERVER_PORT` |
| `EPICS_CA_SERVER_PORT` | `5064` | default TCP and UDP server port |
| `EPICS_CA_REPEATER_PORT` | `5065` | beacon repeater port |
| `EPICS_CA_CONN_TMO` | `30.0` | seconds of beacon silence before a server is treated as gone; a non-positive, non-finite or unparsable value falls back to the default and prints two diagnostic lines to stderr |
| `EPICS_CA_MAX_ARRAY_BYTES` | `16384` | receive buffer size for one message, taken as the value plus 24 bytes and never below the protocol minimum; a negative value means the minimum |
| `EPICS_CA_AUTO_ARRAY_BYTES` | `YES` | when the word is `yes` (any case) the receive buffer grows to the size the server announces, and `EPICS_CA_MAX_ARRAY_BYTES` is not a ceiling |
| `EPICS_CA_MAX_SEARCH_PERIOD` | `300.0` | upper bound of the search back-off in seconds; values below 60 and NaN become 60 |
| `EPICS_CA_MCAST_TTL` | `1` | TTL for multicast search targets |
| `EPICS_CA_NAMESERVER_QUEUE_DEPTH` | `256` | queued searches per name server connection; values below 8 become 8 |
| `EPICS_CA_DNS_REFRESH_SECS` | `60` | seconds between re-resolving host names in the address lists; must be greater than 0 |
| `EPICS_CA_MONITOR_QUEUE` | `256` | per-subscription update queue; values below 10 become 10. Read when a subscription is created |
| `EPICS_CA_PUT_TIMEOUT` | `30` | seconds a put with completion waits for the server before failing. Read per put |
| `EPICS_CA_USE_SHELL_VARS` | `YES` | expand `${VAR}` and `$(VAR)` in the address list variables; an unknown name expands to nothing |
| `EPICS_RS_CLIENT_IGNORE` | `""` | whitespace-separated IPv4 addresses (an optional `:port` is stripped) whose beacons are ignored |
| `EPICS_CA_DISCOVERY` | `""` | extra search sources: `static:<addr>[,<addr>...]` adds targets; the `mdns` and `dnssd:<zone>` tokens are accepted but do nothing in this build and log a warning |

`EPICS_CA_BEACON_PERIOD` is read only by the crate's CA server, which the
extension does not run; the same holds for every `EPICS_CAS_*` variable,
`EPICS_RS_HAG_DNS_REFRESH_SECS` and `EPICS_CA_RS_CHAOS`. `EPICS_CLI_TIMEOUT`
belongs to the crate's command-line tools.

The CA client (`epicsrs.ca`, `epicsrs.aio`, `epicsrs.pv`) has no
configuration argument; the environment is the only way to set it.

## PVA client

`Context(conf=..., useenv=...)` supplies five keys (see
[pva-client.md](pva-client.md)). Everything else below is read from the
process environment by `epics-pva-rs` when the context is created,
regardless of `useenv`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EPICS_PVA_ADDR_LIST` | `""` | whitespace-separated search targets: `addr[:port][,ttl][@iface]`; host names are resolved. Also settable through `conf` |
| `EPICS_PVA_AUTO_ADDR_LIST` | `YES` | add the interface broadcast addresses; `YES`/`NO`/`1`/`0` in any case. Also settable through `conf` |
| `EPICS_PVA_BROADCAST_PORT` | `5076` | UDP search port; `0` means the default. Also settable through `conf` |
| `EPICS_PVA_SERVER_PORT` | `5075` | default TCP port for name servers; when unset, `EPICS_PVAS_SERVER_PORT` is tried next. Also settable through `conf` |
| `EPICS_PVA_NAME_SERVERS` | `""` | whitespace-separated `host[:port]` TCP endpoints searched directly. Also settable through `conf` |
| `EPICS_PVA_CONN_TMO` | `30` | TCP idle timeout in seconds. The effective timeout is four thirds of the value (30 gives 40); a non-finite, non-positive or huge value resets to 40 and anything below 2 becomes 2. Echo requests go out every three eighths of the effective timeout, clamped to 1 to 15 s |
| `EPICS_PVA_AUTH_USER` | `$USER`, then `$USERNAME`, then `nobody` | the account presented to servers by the plain authentication method |
| `EPICS_PVA_AUTH_HOST` | the host name, else `invalidhost.` | the host presented to servers |
| `EPICS_PVA_INTF_ADDR_LIST` | `""` | interfaces the client binds its search sockets to |

Port values are read as integers and truncated to 16 bits; surrounding
whitespace is tolerated. Timeouts must be finite and positive; anything
else is ignored and the default stays.

## PVA server

`Server(conf=..., useenv=...)` passes its dict to the crate, which applies
the keys below in this order on top of the environment (`useenv=True`) or
of the built-in defaults (`useenv=False`). `Server(isolate=True)` ignores
all of them.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EPICS_PVAS_SERVER_PORT` | `5075` | TCP listen port |
| `EPICS_PVAS_TLS_PORT`, else `EPICS_PVA_TLS_PORT` | `5076` | TLS listen port, used only when TLS is configured; `0` requests an ephemeral port |
| `EPICS_PVAS_BROADCAST_PORT` | `5076` | UDP search port to listen on |
| `EPICS_PVAS_TLS_OPTIONS`, else `EPICS_PVA_TLS_OPTIONS` | `""` | whitespace-separated `key=value` tokens; the server reads `disable_plaintext=true` or `=false` from it, other tokens are ignored. The first variable present wins, they are not merged |
| `EPICS_PVAS_MAX_CONNECTIONS` | `1024` | accepted TCP connections |
| `EPICS_PVAS_MAX_CHANNELS_PER_CONN` | `1024` | channels one connection may open |
| `EPICS_PVAS_MAX_OPS_PER_CHANNEL` | `64` | operations in flight per channel |
| `EPICS_PVAS_BEACON_PERIOD` | `15` | seconds between beacons at start; the long period is twelve times it unless `EPICS_PVAS_BEACON_PERIOD_LONG` is set, and never less than the short period plus one second |
| `EPICS_PVAS_BEACON_PERIOD_LONG` | 12 × short | see above |
| `EPICS_PVAS_BEACON_ADDR_LIST` | `""` | explicit beacon destinations |
| `EPICS_PVAS_AUTO_BEACON_ADDR_LIST` | `YES` | send beacons to the interface broadcast addresses |
| `EPICS_PVAS_INTF_ADDR_LIST` | all interfaces | interfaces to bind; an entry that does not parse makes the server refuse to start |
| `EPICS_PVAS_IGNORE_ADDR_LIST` | `""` | clients whose searches are ignored |
| `EPICS_PVAS_SEND_TMO` | | seconds a blocked send may take before the connection is dropped |
| `EPICS_PVAS_TLS_HANDSHAKE_TMO` | | seconds allowed for a TLS handshake |
| `EPICS_PVA_CONN_TMO` | `30` | the server side of the idle timeout, scaled as for the client |
| `EPICS_PVAS_TLS_KEYCHAIN`, `EPICS_PVAS_TLS_KEYCHAIN_PASSWORD`, `EPICS_PVA_TLS_KEYCHAIN`, `EPICS_PVA_TLS_KEYCHAIN_PASSWORD`, `EPICS_PVA_TLS_CA_KEYCHAIN`, `EPICS_PVA_TLS_DISABLE` | | TLS keychain files and their passwords (the `PVAS` form wins over the shared one), the CA keychain, and `EPICS_PVA_TLS_DISABLE` (a truthy value turns TLS off even when a keychain is set). The `tls` feature is on in this build; the Python layer offers no TLS arguments, so these variables are the only way to configure it |

## Logging

The crates log through `tracing`. `PVXS_LOG`, `EPICS_PVA_LOG` and
`RUST_LOG` are only consulted by a `tracing` subscriber, and the extension
installs none, so nothing the crates log reaches the terminal. Python-side
diagnostics go to the `logging` loggers `epicsrs.pva` (monitor callback
exceptions) and `epicsrs.pva.server` (handler exceptions), and to stderr
for a `camonitor` callback that raises.

## Not read in this build

The `epicsrs` extension depends on the crates with their default Cargo
features. These variables are read only under features that are off:

| Variable | Feature |
| --- | --- |
| `EPICS_CA_TLS_SNI_MAP`, `EPICS_CA_TLS_HANDSHAKE_TMO`, `EPICS_CA_TLS_SERVER_NAME`, `EPICS_CA_TLS_ROOTS_FILE` | `experimental-rust-tls` |
| `EPICS_CA_BEACON_REQUIRE_SIGNED` | `cap-tokens` |
| `EPICS_CA_DISCOVERY` `mdns` and `dnssd:` tokens | `discovery` |
