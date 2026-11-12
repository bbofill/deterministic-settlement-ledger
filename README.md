# Deterministic Institutional Settlement & Clearing Engine

A zero-dependency, distributed financial clearinghouse and deterministic settlement engine written in Go (`1.23+`). Built to eliminate principal settlement risk (*Herstatt Risk*) through **Atomic Delivery-versus-Payment (DvP)** transactions, **Leader-Based Byzantine Fault Tolerant (BFT)** consensus, **64-bit fixed-point deterministic state transitions**, and **$O(\log N)$ cryptographic Merkle audit proofs**.

---

## 1. Overview & Motivation

In institutional capital markets, post-trade clearing and settlement frequently rely on delayed batch cycles ($T+1$ / $T+2$) that expose counterparties to principal risk—where one party delivers an asset before the other settles payment. Conversely, public Layer-1 networks introduce probabilistic finality, non-deterministic execution ordering, and front-running exposure (MEV).

This project implements a permissioned institutional settlement core combining deterministic state replication with sub-100ms finality:

* **Atomic Delivery-versus-Payment (DvP):** Multi-asset swaps (`AssetIn` $\leftrightarrow$ `AssetOut`) require dual `Ed25519` cryptographic signatures from both counterparties and execute as an indivisible state transition.
* **Deterministic Finality:** Blocks are committed strictly upon collecting a supermajority BFT quorum ($\lfloor \frac{2N}{3} \rfloor + 1$), preventing chain reorganizations or post-trade rollbacks.
* **Bit-for-Bit State Determinism:** Eliminates IEEE-754 floating-point divergence across architectures by representing all balances as signed 64-bit integers (`int64`) scaled by $10^8$.
* **Replay & Ordering Protection:** Enforces strict per-account sequential `Nonce` validation and canonical transaction sorting (`Nonce ASC`, `TxHash ASC`) prior to block execution.
* **Crash-Safe Persistence & Autonomous Catch-Up:** Commits blocks to a length-prefixed binary Write-Ahead Log (`ledger.wal`) with explicit OS kernel flushes (`fsync`), paired with an autonomous peer-to-peer synchronization loop to recover restarted or lagging validators.

---

## 2. System Architecture

The codebase is organized into four decoupled layers (`core`, `storage`, `consensus`, `network`) using exclusively the Go standard library (`crypto/ed25519`, `crypto/sha256`, `net/rpc`, `sync/atomic`).

### Settlement Lifecycle

1. **Dual-Signed Order Submission:** Counterparties sign their respective legs of an `AtomicSwap` transaction (`SenderSig` and `CounterpartySig`) and submit the payload to any validator node via TCP RPC.
2. **Gossip & Validation:** The receiving validator validates cryptographic signatures and sequential `Nonce` constraints, inserts the transaction into its concurrent `Mempool`, and gossips it to all peer nodes.
3. **Canonical Batch Proposal:** Every `50ms`, the deterministic round leader (`(Height + View) mod N`) extracts pending transactions, sorts them canonically (`Nonce`, `TxHash`), simulates execution on a cloned state (`PreviewBlock`), and broadcasts a signed `BlockProposal` containing the provisional `StateRoot`.
4. **BFT Quorum Voting:** Each validator independently executes the proposed transactions against its local state. If its computed `StateRoot` matches the proposal, it returns an `Ed25519`-signed approval vote.
5. **Commit & Persistence:** Once at least 4 of 5 validators approve the exact `StateRoot`, the leader broadcasts `CommitBlock`. Each node mutates its live state, appends the block to `ledger.wal` with `file.Sync()`, and updates its binary Merkle Tree.

### Repository Structure

```text
deterministic-settlement-ledger/
├── core/
│   ├── types.go                     # Fixed-point scale (10^8), Ed25519 dual-signed Tx & Block definitions
│   ├── merkle.go                    # Binary SHA-256 Merkle Tree & O(log N) inclusion proof verifier
│   ├── state.go                     # Thread-safe StateMachine, canonical sorting, Preview/Apply logic
│   └── state_test.go                # Unit tests for determinism, anti-replay, and atomic rollback
├── consensus/
│   ├── mempool.go                   # Concurrent-safe pending transaction buffer & deduplication
│   └── bft.go                       # Leader rotation, proposal signing, and 2f+1 quorum verification
├── storage/
│   ├── store.go                     # Length-prefixed binary WAL with fsync & crash state recovery
│   └── recovery_byzantine_test.go   # WAL recovery & Byzantine state-root rejection unit tests
├── network/
│   ├── rpc.go                       # TCP RPC transport, 50ms consensus loop, gossip & Catch-Up Sync
│   └── http.go                      # JSON Observability API (/status, /accounts, /account) & Prometheus
├── benchmarks/
│   └── benchmark_test.go            # 5-node Chaos Engineering & latency/throughput benchmark suite
├── cmd/
│   ├── node/main.go                 # Production validator daemon with graceful signal handling
│   └── cli/main.go                  # Interactive DvP trade simulator ('demo' & continuous 'trade' modes)
├── Dockerfile                       # Multi-stage Alpine build for static node/CLI binaries
└── docker-compose.yml               # 5-validator isolated bridge network topology
```

---

## 3. Core Engineering & Mathematical Guarantees

### 3.1 Fixed-Point Arithmetic (`int64` Scaling)
Standard IEEE-754 floating-point types (`float64`) cannot represent base-10 fractions accurately in binary, risking state root divergence across nodes. All asset quantities are stored as signed 64-bit integers scaled by $\text{ScaleFactor} = 10^8$:

$$\text{Amount}_{\text{internal}} = \text{Amount}_{\text{human}} \times 10^8$$

For example, $120,000.00\text{ USD}$ is stored as `12000000000000` and $2.00000000\text{ BTC}$ as `200000000`.

### 3.2 Atomic DvP & Rollback Safety
During block proposal and verification (`PreviewBlock`), the state machine operates on an isolated clone of the account map and enforces three invariants per transaction:
1. **Dual-Signature Verification:** Confirms `ed25519.Verify(Sender, SigningPayload, SenderSig)` and `ed25519.Verify(Counterparty, SigningPayload, CounterpartySig)`.
2. **Sequential Nonce Enforcement:** Requires $\text{Tx.Nonce} == \text{Account.Nonce} + 1$, preventing replay attacks and out-of-order execution.
3. **Simultaneous Solvency Check:** Both legs of the swap must hold sufficient balance at execution time:

$$\text{Balance}_{\text{Sender}}[\text{AssetIn}] \ge \text{AmountIn} \quad \land \quad \text{Balance}_{\text{Counterparty}}[\text{AssetOut}] \ge \text{AmountOut}$$

If any transaction in a proposed batch fails validation, the provisional state is discarded without mutating live balances.

### 3.3 Byzantine Fault Tolerant Consensus ($N = 3f + 1$)
With $N = 5$ validators, the cluster tolerates up to $f = 1$ arbitrary (crash or malicious Byzantine) node failure while maintaining safety and liveness. The required quorum threshold is:

$$Q = \left\lfloor \frac{2N}{3} \right\rfloor + 1 = \left\lfloor \frac{10}{3} \right\rfloor + 1 = 4 \text{ validators}$$

Validators sign the resulting 32-byte `StateRoot` rather than only the block header; votes endorsing a mismatched state root are rejected by `VerifyQuorum`.

### 3.4 Deterministic Merkle State Root & $O(\log N)$ Audit Proofs
Following every block commit, account public keys are sorted lexicographically, serialized (`Address || Nonce || Sorted(Asset:Balance)`), and hashed into a balanced binary **SHA-256 Merkle Tree**. External auditors can query `GET /account?address=<pubkey>` to obtain an inclusion branch of $\lceil \log_2(N) \rceil$ sibling hashes and verify account solvency against the consensus `StateRoot` without downloading the full ledger.

### 3.5 Binary Write-Ahead Log (`WAL`) & Autonomous Catch-Up Sync
* **Disk Durability:** Committed blocks are encoded via `encoding/gob` with a 4-byte Big-Endian length prefix (`[uint32 length][payload]`) into `ledger.wal` followed by `file.Sync()`.
* **Catch-Up Synchronization:** A background `syncLoop` polls peer heights every `150ms` via `GetStatus` and pulls missing blocks via `GetBlocksFrom` to automatically heal node restarts or transient network partitions.

---

## 4. Benchmarks & Chaos Engineering Verification

The benchmark suite (`benchmarks/benchmark_test.go`) deploys a 5-node cluster over local TCP sockets, funds **100 institutional accounts**, **terminates Node 4 mid-execution** to simulate a validator outage, processes **500 concurrent Atomic DvP swaps**, and restarts Node 4 to verify autonomous Catch-Up Synchronization.

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
| **Merkle Proof Size** | **7 Hashes ($224\text{ bytes}$)** | $\lceil \log_2(100\text{ accounts}) \rceil = 7$ tree levels |

---

## 5. Quick Start & Usage

### 5.1 Run Automated Unit, Byzantine & Chaos Benchmark Tests
Requires Go `1.23+`:

```bash
go test -v ./...
```

### 5.2 Start the 5-Node Cluster via Docker Compose
Build the multi-stage Alpine images and launch all 5 validator containers (`node0` through `node4`):

```bash
docker compose up --build
```

| Validator Node | TCP RPC Endpoint | HTTP & Prometheus Endpoint |
| :--- | :--- | :--- |
| **`node0`** | `127.0.0.1:9000` | `http://localhost:8080` |
| **`node1`** | `127.0.0.1:9001` | `http://localhost:8081` |
| **`node2`** | `127.0.0.1:9002` | `http://localhost:8082` |
| **`node3`** | `127.0.0.1:9003` | `http://localhost:8083` |
| **`node4`** | `127.0.0.1:9004` | `http://localhost:8084` |

### 5.3 Execute Institutional DvP Settlements via CLI
In a second terminal while the cluster is running:

1. **Initialize participant accounts (`1,000,000 USD` & `50 BTC`) and execute an initial Atomic DvP Swap (`1 BTC` $\leftrightarrow$ `60,000 USD`):**
   ```bash
   go run ./cmd/cli -mode=demo
   ```

2. **Execute a stream of 10 consecutive Atomic DvP Swaps (`0.2 BTC` $\leftrightarrow$ `10,000 USD` each) with automatic `Nonce` resolution:**
   ```bash
   go run ./cmd/cli -mode=trade -n=10
   ```

### 5.4 Observability, Merkle Auditing & Prometheus Endpoints

* **Node Consensus Status (`GET /status`):**
  ```bash
  curl -s [http://127.0.0.1:8080/status](http://127.0.0.1:8080/status) | python3 -m json.tool
  ```
* **Full Ledger Account Table (`GET /accounts`):**
  ```bash
  curl -s [http://127.0.0.1:8080/accounts](http://127.0.0.1:8080/accounts) | python3 -m json.tool
  ```
* **Cryptographic Merkle Audit Proof (`GET /account?address=<pubkey>`):**
  ```bash
  curl -s "[http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89](http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89)" | python3 -m json.tool
  ```
* **Prometheus Metrics (`GET /metrics`):**
  ```bash
  curl -s [http://127.0.0.1:8080/metrics](http://127.0.0.1:8080/metrics)
  ```

Stop and remove the containers when finished:
```bash
docker compose down
```

---

## 6. Author

**Blai Bofill Butinyà**  
Bachelor's Degree in Data Engineering — *Universitat Autònoma de Barcelona (UAB)*
