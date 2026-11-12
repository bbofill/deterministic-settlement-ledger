# Deterministic Institutional Settlement & Clearing Engine (`BFT` / `DvP`)

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Consensus](https://img.shields.io/badge/Consensus-Leader--Based_BFT_(2f%2B1)-blueviolet?style=flat-square)]()
[![Cryptography](https://img.shields.io/badge/Crypto-Ed25519_%7C_SHA--256_Merkle-success?style=flat-square)]()
[![Persistence](https://img.shields.io/badge/Storage-Binary_WAL_(fsync)-orange?style=flat-square)]()
[![Deployment](https://img.shields.io/badge/Container-Docker_Compose_(5_Nodes)-2496ED?style=flat-square&logo=docker)]()

A zero-dependency, distributed financial clearinghouse and deterministic settlement engine written in **Go**. Engineered to eliminate counterparty settlement risk (*Herstatt Risk*) via **Atomic Delivery-versus-Payment (DvP)** transactions, **Byzantine Fault Tolerant (BFT)** quorum consensus, **fixed-point deterministic state machines**, and **$O(\log N)$ cryptographic Merkle audit proofs**.

---

## 1. Executive Summary & Institutional Motivation

In traditional capital markets, post-trade clearing and settlement often suffer from multi-day finality windows ($T+1$ / $T+2$) and principal risk—where one counterparty delivers an asset while the other defaults before payment clears. Conversely, public Layer-1 blockchains introduce probabilistic finality, non-deterministic gas auctions, and front-running vulnerability (MEV).

This engine implements a **permissioned institutional settlement core** combining the deterministic guarantees of cryptographic state machines with the sub-100ms finality of high-frequency financial infrastructure:

* **Zero Principal Risk (Atomic DvP):** Multi-asset trades (`AssetIn` $\leftrightarrow$ `AssetOut`) require dual cryptographic authorization (`Ed25519` signatures from both Buyer and Seller) and execute as a single indivisible state transition.
* **Instant & Irreversible Finality:** Blocks are committed only when signed by a supermajority BFT quorum ($\lfloor \frac{2N}{3} \rfloor + 1$), guaranteeing zero chain reorganizations or rollbacks.
* **Bit-for-Bit Determinism:** Eliminates IEEE-754 floating-point drift across heterogeneous CPU architectures by enforcing 64-bit fixed-point arithmetic (`int64` scaled by $10^8$).
* **MEV & Replay Immunity:** Enforces strict per-account sequential `Nonce` tracking and canonical transaction ordering (`Nonce ASC`, `TxHash ASC`) prior to block execution.
* **Crash-Safe Durability & Autonomous Catch-Up:** Persists committed blocks to a length-prefixed binary Write-Ahead Log (`ledger.wal`) with hardware `fsync` before acknowledgement, paired with an autonomous peer-to-peer synchronization loop for lagging or restarted nodes.

---

## 2. System Architecture & End-to-End Settlement Pipeline

The system is structured into four strictly decoupled layers (`core`, `storage`, `consensus`, `network`) with **zero external third-party libraries**, relying exclusively on the Go standard library (`crypto/ed25519`, `crypto/sha256`, `net/rpc`, `sync/atomic`).

```text
+-----------------------------------------------------------------------------------+
|                        INSTITUTIONAL TRADING PARTICIPANTS                         |
|         Citadel-Desk-A (Buyer)                     JaneStreet-MM-B (Seller)       |
|      Signs Leg 1 (Ed25519 SenderSig)            Signs Leg 2 (Ed25519 CounterSig)  |
+-----------------------------------------+-----------------------------------------+
                                          |
                          Submit Atomic DvP Swap (TCP RPC)
                                          v
+-----------------------------------------------------------------------------------+
|                         5-NODE BFT VALIDATOR CLUSTER                              |
|                                                                                   |
|   +-------------------+      Gossip Tx       +-------------------------------+    |
|   |   Node 0 (Leader) | -------------------> | Nodes 1, 2, 3, 4 (Validators) |    |
|   +---------+---------+                      +---------------+---------------+    |
|             |                                                |                    |
|             | 1. Canonical Sort (Nonce, TxHash)              |                    |
|             | 2. PreviewBlock() on Cloned State              | 3. Verify Sigs     |
|             | 3. Compute Provisional StateRoot               | 4. Re-compute Root |
|             |                                                | 5. Sign BFT Vote   |
|             |<--------- Collect >= 4/5 Signed Votes ---------+                    |
|             |                                                                     |
|             +---------> Broadcast CommitBlock (Quorum Verified)                   |
+-----------------------------------------+-----------------------------------------+
                                          |
              +---------------------------+---------------------------+
              v                                                       v
+-----------------------------------+           +-----------------------------------+
|   DETERMINISTIC STATE MACHINE     |           |     BINARY WRITE-AHEAD LOG        |
|  - Atomic Balance Mutation        |           |  - Length-Prefixed Gob Encoding   |
|  - Nonce Increment                |           |  - OS Kernel Flush (file.Sync)    |
|  - O(log N) Merkle Tree Rebuild   |           |  - Catch-Up Sync Serving          |
+-----------------------------------+           +-----------------------------------+
```

### Package Structure

```text
deterministic-settlement-ledger/
├── core/
│   ├── types.go                     # Fixed-point scale (10^8), Ed25519 dual-signed Tx & Block structs
│   ├── merkle.go                    # Binary SHA-256 Merkle Tree & O(log N) inclusion proof verifier
│   ├── state.go                     # Thread-safe StateMachine, canonical sorting, Preview/Apply logic
│   └── state_test.go                # Unit tests for determinism, anti-replay, and atomic rollback
├── consensus/
│   ├── mempool.go                   # Concurrent-safe pending transaction buffer & deduplication
│   └── bft.go                       # Leader rotation, proposal signing, and 2f+1 quorum verification
├── storage/
│   ├── store.go                     # Length-prefixed binary WAL with hardware fsync & state recovery
│   └── recovery_byzantine_test.go   # Crash recovery & Byzantine malicious state-root rejection tests
├── network/
│   ├── rpc.go                       # TCP RPC transport, 50ms consensus loop, gossip & Catch-Up Sync
│   └── http.go                      # JSON Observability API (/status, /accounts, /account) & Prometheus
├── benchmarks/
│   └── benchmark_test.go            # 5-node Chaos Engineering & latency/throughput benchmark suite
├── cmd/
│   ├── node/main.go                 # Production validator daemon with graceful signal handling
│   └── cli/main.go                  # Interactive DvP trade simulator ('demo' & continuous 'trade' modes)
├── Dockerfile                       # Multi-stage Alpine build (<15MB static binary image)
└── docker-compose.yml               # 5-validator isolated bridge network topology
```

---

## 3. Core Engineering & Mathematical Guarantees

### 3.1 Fixed-Point Arithmetic (`int64` Scaling)
Standard IEEE-754 floating-point types (`float64`) cannot represent base-10 fractions (e.g., `0.1 + 0.2 != 0.3`) accurately in binary, causing state root divergence across nodes. All asset quantities are stored as signed 64-bit integers scaled by $\text{ScaleFactor} = 10^8$:

$$\text{Amount}_{\text{internal}} = \text{Amount}_{\text{human}} \times 10^8$$

For example, $120,000.00\text{ USD}$ is processed as `12000000000000` and $2.00000000\text{ BTC}$ as `200000000`.

### 3.2 Atomic Delivery-versus-Payment (DvP) & Rollback Safety
During block proposal and validation (`PreviewBlock`), the state machine clones the in-memory account map and validates every transaction in canonical order:
1. **Cryptographic Integrity:** Verifies `ed25519.Verify(Sender, SigningPayload, SenderSig)` and, for `TxAtomicSwap`, `ed25519.Verify(Counterparty, SigningPayload, CounterpartySig)`.
2. **Strict Nonce Sequencing:** Enforces $\text{Tx.Nonce} == \text{Account.Nonce} + 1$. Duplicate or out-of-order transactions are rejected before mutating state.
3. **All-or-Nothing Balance Check:** Both legs of the swap must hold sufficient funds simultaneously:

$$\text{Balance}_{\text{Sender}}[\text{AssetIn}] \ge \text{AmountIn} \quad \land \quad \text{Balance}_{\text{Counterparty}}[\text{AssetOut}] \ge \text{AmountOut}$$

If any single transaction in a block fails validation, the entire provisional state is discarded without side effects.

### 3.3 Byzantine Fault Tolerant (BFT) Consensus ($N = 3f + 1$)
With $N = 5$ validators, the cluster tolerates up to $f = 1$ arbitrary (crash or malicious Byzantine) node failure while preserving safety and liveness. The required quorum size is:

$$Q = \left\lfloor \frac{2N}{3} \right\rfloor + 1 = \left\lfloor \frac{10}{3} \right\rfloor + 1 = 4 \text{ validators}$$

* **Deterministic Leader Rotation:** For block height $H$ and view $V$, the designated proposer is $\text{Leader} = (H + V) \bmod N$.
* **StateRoot Agreement:** Validators do not merely vote on block hashes; each validator independently executes the block's transactions and signs the resulting 32-byte `StateRoot`. If a compromised node votes for a falsified state root, `VerifyQuorum` rejects the vote.

### 3.4 Deterministic Merkle State Root & $O(\log N)$ Audit Proofs
After every block commit, the state machine sorts all account public keys lexicographically, hashes each account (`Address || Nonce || Sorted(Asset:Balance)`), and constructs a balanced binary **SHA-256 Merkle Tree**.
* External auditors can query `GET /account?address=<pubkey>` to retrieve an inclusion proof consisting of $\lceil \log_2(N) \rceil$ sibling hashes, verifying mathematically that an account balance is included in the cluster's consensus `StateRoot` without downloading the full ledger.

### 3.5 Binary Write-Ahead Log (`WAL`) & Autonomous Catch-Up Sync
* **Disk Persistence:** Every committed block is serialized via `encoding/gob` with a 4-byte Big-Endian length prefix (`[uint32 length][payload]`) into `ledger.wal` followed by an explicit OS kernel flush (`file.Sync()`).
* **Catch-Up State Sync:** A background `syncLoop` runs every `150ms` on each validator, querying peer heights via `GetStatus` and fetching missing historical blocks via `GetBlocksFrom` to seamlessly heal network partitions or node restarts.

---

## 4. Empirical Benchmarks & Chaos Engineering Results

The test suite (`benchmarks/benchmark_test.go`) spins up a live 5-node cluster over local TCP sockets, funds **100 institutional accounts**, **kills Node 4 mid-flight** to simulate a sudden server outage, executes **500 concurrent Atomic DvP swaps**, and restarts Node 4 to verify autonomous Catch-Up Synchronization.

```text
=== QUANT SETTLEMENT BENCHMARK & RECOVERY RESULTS (5/5 Synced BFT Nodes) ===
Total Atomic DvP Trades Settled : 500
Throughput (TPS)                : 826.14 trades/sec
Settlement Latency p50          : 57.52ms
Settlement Latency p95          : 95.49ms
Settlement Latency p99          : 95.49ms
Node 4 Catch-Up State Sync      : VERIFIED (Synced to Height 11)
Merkle Proof Verification       : VERIFIED (7 tree steps for 100 accounts)
Final Consensus Merkle StateRoot: 922470a6e127dab5dee759453dbb4cbfac80a106a40c588f9cdf2b45787213d5
```

| Metric | Measured Result | Architectural Driver |
| :--- | :--- | :--- |
| **Throughput** | **~825 – 840 TPS** | Batching up to 50 DvP swaps per 50ms consensus tick |
| **Median Finality (`p50`)** | **57.5 ms** | Single-round TCP RPC quorum voting + in-memory state preview |
| **Tail Finality (`p99`)** | **95.5 ms** | Hardware `fsync` WAL flush + Ed25519 dual-signature verification |
| **Fault Tolerance** | **1 Node Down ($4/5$ Active)** | Zero dropped transactions during simulated validator crash |
| **Merkle Audit Proof Size** | **7 Hashes ($224\text{ bytes}$)** | $\lceil \log_2(100\text{ accounts}) \rceil = 7$ tree levels |

---

## 5. Quick Start & Operational Guide

### 5.1 Run Automated Unit, Byzantine & Chaos Benchmark Tests
Requires Go `1.23+`:

```bash
go test -v ./...
```

### 5.2 Launch the 5-Node Distributed Cluster via Docker Compose
Build the multi-stage Alpine images and start all 5 isolated validator containers (`node0` through `node4`):

```bash
docker compose up --build
```

| Validator Node | Internal / Host TCP RPC Port | Host HTTP & Prometheus API |
| :--- | :--- | :--- |
| **`node0`** | `127.0.0.1:9000` | `http://localhost:8080` |
| **`node1`** | `127.0.0.1:9001` | `http://localhost:8081` |
| **`node2`** | `127.0.0.1:9002` | `http://localhost:8082` |
| **`node3`** | `127.0.0.1:9003` | `http://localhost:8083` |
| **`node4`** | `127.0.0.1:9004` | `http://localhost:8084` |

### 5.3 Execute Institutional DvP Settlements via CLI
Open a second terminal while the Docker cluster is running:

1. **Initialize participant accounts (`1,000,000 USD` & `50 BTC`) and execute an initial Atomic DvP Swap (`1 BTC` $\leftrightarrow$ `60,000 USD`):**
   ```bash
   go run ./cmd/cli -mode=demo
   ```

2. **Stream a burst of 10 consecutive Atomic DvP Swaps (`0.2 BTC` $\leftrightarrow$ `10,000 USD` each) with automatic `Nonce` discovery:**
   ```bash
   go run ./cmd/cli -mode=trade -n=10
   ```

### 5.4 Real-Time Observability, Merkle Auditing & Prometheus Metrics

Query any node's HTTP interface using `curl` or your browser:

* **Cluster Consensus Status (`GET /status`):**
  ```bash
  curl -s [http://127.0.0.1:8080/status](http://127.0.0.1:8080/status) | python3 -m json.tool
  ```
* **Full Ledger Account Table (`GET /accounts`):**
  ```bash
  curl -s [http://127.0.0.1:8080/accounts](http://127.0.0.1:8080/accounts) | python3 -m json.tool
  ```
* **Cryptographic Merkle Audit Proof for a Specific Account (`GET /account?address=<pubkey>`):**
  ```bash
  curl -s "[http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89](http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89)" | python3 -m json.tool
  ```
* **Prometheus Telemetry (`GET /metrics`):**
  ```bash
  curl -s [http://127.0.0.1:8080/metrics](http://127.0.0.1:8080/metrics)
  ```

To stop and clean up the cluster containers:
```bash
docker compose down
```

---

## 6. Author

**Blai Bofill Butinyà**  
Bachelor's Degree in Data Engineering — *Universitat Autònoma de Barcelona (UAB)*
