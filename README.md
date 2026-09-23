# Couchbase Lite 4.0 — Peer-to-Peer Sync Demo

A single-class Java application demonstrating **peer-to-peer data synchronization** using Couchbase Lite 4.0 Enterprise Edition. Two instances of the same application discover each other on the local network via **DNS-SD** (Bonjour) and replicate documents bidirectionally.

## Prerequisites

- **Java 11+** (JDK)
- **Maven 3.6+**
- **Couchbase Lite Enterprise Edition** license/access (P2P features require EE)
- Both peers must be on the **same local network**

## Build

```bash
mvn clean package
```

This produces a fat JAR at `target/peer-to-peer-1.0.jar`.

## Run

### Terminal 1 — Start the Server (Passive Peer)

```bash
java -jar target/peer-to-peer-1.0.jar --mode server --docs 100
```

### Terminal 2 — Start the Client (Active Peer)

```bash
java -jar target/peer-to-peer-1.0.jar --mode client --docs 50
```

### Arguments

| Argument | Values            | Description                                         |
|----------|-------------------|-----------------------------------------------------|
| `--mode` | `server`/`client` | **Required.** Run as passive listener or active replicator |
| `--docs` | `N` (integer)     | Number of dummy documents to create locally (default: 10) |
| `--size` | `N` (integer)     | Size of each dummy document in KB (default: 500) |
| `--bind` | IP address        | Local IP used for DNS-SD (default: `InetAddress.getLocalHost()`). Use the Wi-Fi IP when the Mac is also on Ethernet and the other peer is a phone on Wi-Fi |

## How It Works

```
┌─────────────────────┐         DNS-SD          ┌─────────────────────┐
│   SERVER (Passive)  │◄────── Discovery ───────│   CLIENT (Active)   │
│                     │                          │                     │
│  URLEndpointListener│◄═══ WebSocket P2P ═════►│     Replicator      │
│   on port 55990     │    Push & Pull Sync      │   (continuous)      │
│                     │                          │                     │
│  Local DB: 100 docs │  ←── Bidirectional ──→   │  Local DB: 50 docs  │
│  After sync: 150    │      Replication         │  After sync: 150    │
└─────────────────────┘                          └─────────────────────┘
```

1. **Server** starts a `URLEndpointListener` on port 55990 and registers itself via DNS-SD
2. **Client** browses for the DNS-SD service, discovers the server automatically
3. Client creates a `Replicator` targeting the server's WebSocket endpoint
4. **Continuous push-and-pull** replication syncs all documents bidirectionally
5. Console shows every replicated document + throughput metrics (docs/sec)

## Console Output Example

```
[18:30:01.234] ══════════════════════════════════════════════════════════
[18:30:01.234]   Couchbase Lite 4.0 — Peer-to-Peer Sync Demo
[18:30:01.234]   Mode : CLIENT
[18:30:01.234]   Docs : 50 dummy documents will be created
[18:30:01.234] ══════════════════════════════════════════════════════════
[18:30:01.567] ✓ Created 50 documents in 120 ms
[18:30:02.890]   ✓ Server resolved: 192.168.1.42:55990
[18:30:03.012] ✓ Replicator started (continuous push & pull)
[18:30:03.456]   📤 PUSH client_doc_1 [OK]
[18:30:03.456]   📥 PULL server_doc_1 [OK]
[18:30:03.789]   📊 Throughput: 45 docs replicated | 123.4 docs/sec | elapsed: 0.8 sec
```

## Architecture

- **`PeerToPeerSync.java`** — Single class with everything:
  - CLI argument parsing
  - Couchbase Lite database initialization
  - Dummy document generation
  - Server mode: `URLEndpointListener` + DNS-SD service registration (JmDNS)
  - Client mode: DNS-SD discovery + `Replicator` with `URLEndpoint`
  - Replication event logging + throughput computation

## Notes

- **Several interfaces on the same LAN** (e.g. Ethernet + Wi-Fi): JmDNS advertises on a single interface, by default the one behind `getLocalHost()`. Mobile peers on Wi-Fi may then never get answers to their mDNS queries. Bind DNS-SD to the Wi-Fi IP: `--bind $(ipconfig getifaddr en0)`. The listener itself still accepts connections on all interfaces.

- TLS is **disabled** for demo simplicity. Enable it for production use.
- Each mode uses a separate database directory to avoid conflicts when running both on the same machine.
- The application runs continuously until stopped with `Ctrl+C`. Final statistics are printed on shutdown.
