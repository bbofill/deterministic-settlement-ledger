# deterministic-settlement-ledger

A distributed, deterministic Delivery-versus-Payment (DvP) settlement engine written in Go (`1.23+`) using exclusively the standard library.

The system implements a permissioned clearinghouse core where multi-asset atomic swaps settle across a 5-node Byzantine Fault Tolerant (BFT) validator cluster with deterministic 64-bit fixed-point arithmetic, length-prefixed Write-Ahead Logging (`fsync`), and $O(\log N)$ SHA-256 Merkle inclusion proofs.

---

## Architecture & Protocol Specification

The repository is partitioned into four decoupled packages with zero external dependencies (`crypto/ed25519`, `crypto/sha256`, `encoding/gob`, `net/rpc`, `sync/atomic`):

```text
cmd/
  node/          Validator daemon entrypoint, peer mesh wiring, graceful SIGINT/SIGTERM shutdown
  cli/           DvP transaction generator ('demo' initialization and 'trade' continuous stream)
core/            Deterministic state machine, Ed25519 verification, fixed-point math, Merkle tree
consensus/       Thread-safe mempool and leader-based BFT quorum engine
storage/         Append-only binary WAL with hardware fsync and crash state recovery
network/         TCP RPC transport, 50ms consensus loop, gossip, Catch-Up Sync, HTTP/Prometheus API
benchmarks/      5-node local TCP cluster chaos & latency/throughput test harness
```

### 1. Deterministic Execution & Fixed-Point Arithmetic
Floating-point types (`float64`) are prohibited across the state machine to prevent IEEE-754 rounding drift across heterogeneous CPU architectures. All asset quantities are represented as signed 64-bit integers (`int64`) scaled by $\text{ScaleFactor} = 10^8$:

$$\text{Amount}_{\text{internal}} = \text{Amount}_{\text{decimal}} \times 10^8$$

* `1.00000000 BTC` $\rightarrow$ `100000000`
* `120,000.00 USD` $\rightarrow$ `12000000000000`

### 2. Transaction Model & Dual-Signature DvP
The state machine supports two transaction primitives (`core/types.go`):
* `TxMint`: Credits an asset balance to an account (`Nonce == Account.Nonce + 1`), signed by the sender (`SenderSig`).
* `TxAtomicSwap`: Executes an indivisible two-leg asset exchange (`Sender` transfers `AmountIn` of `AssetIn` to `Counterparty`; `Counterparty` transfers `AmountOut` of `AssetOut` to `Sender`).

Both counterparties sign the canonical SHA-256 digest of the transaction fields (`Type || Sender || Counterparty || AssetIn || AmountIn || AssetOut || AmountOut || Nonce`) using `Ed25519`. Before mutating state, `PreviewBlock` enforces:
1. Valid `SenderSig` and `CounterpartySig`.
2. Strict sequential nonce ordering ($\text{Tx.Nonce} = \text{Account.Nonce} + 1$) to prevent replay attacks.
3. Simultaneous solvency on both legs ($\text{Balance}_{\text{Sender}}[\text{AssetIn}] \ge \text{AmountIn}$ and $\text{Balance}_{\text{Counterparty}}[\text{AssetOut}] \ge \text{AmountOut}$).

Transactions within a block are deterministically sorted by `(Nonce ASC, TxHash ASC)` prior to execution to eliminate proposer transaction-reordering bias.

### 3. Consensus Protocol (Leader-Based BFT)
The cluster operates with $N = 5$ validators, tolerating $f = 1$ crash or Byzantine failure under the quorum threshold:

$$Q = \left\lfloor \frac{2N}{3} \right\rfloor + 1 = 4 \text{ validators}$$

* **Round Progression:** Every `50ms`, the deterministic leader for height $H$ and view $V$, computed as $\text{Leader} = (H + V) \bmod N$, drains up to 50 pending transactions from its mempool, computes the provisional `StateRoot` via `PreviewBlock`, signs the block header, and broadcasts `ProposeBlock` to all peers.
* **State-Root Agreement:** Each validator independently executes the proposed batch against a cloned state and returns an `Ed25519`-signed `Vote` endorsing the resulting 32-byte `StateRoot`. Votes signing a divergent state root are rejected by `VerifyQuorum`.
* **Commit Finality:** Upon collecting $Q = 4$ valid signatures for the identical `StateRoot`, the leader broadcasts `CommitBlock`. Finality is immediate and irreversible upon commit.

### 4. Storage Wire Format & Catch-Up Synchronization
Committed blocks are persisted to `ledger.wal` (`storage/store.go`) using framed binary records:

```text
+-----------------------+-----------------------------------------------+
| Length (4 bytes, BE)  | Gob-Encoded Block Payload (Length bytes)      |
+-----------------------+-----------------------------------------------+
```

Each `AppendBlock` call writes the framed record and invokes `file.Sync()` (`fsync`) before returning. On node startup, `RecoverState` replays `ledger.wal` sequentially from genesis and verifies state root continuity.

If a validator falls behind due to a restart or transient network partition, a background `syncLoop` (`150ms` interval) queries peer heights via `GetStatus` and fetches missing blocks via `GetBlocksFrom`, validating quorum signatures before applying each historical block.

### 5. Merkle State Commitment & Inclusion Proofs
After every block commit, account public keys are sorted lexicographically, hashed (`SHA256(Address || Nonce || Sorted(Asset:Balance))`), and reduced into a binary SHA-256 Merkle Tree (`core/merkle.go`).

Auditors can verify account solvency against the consensus `StateRoot` in $O(\log N)$ hashes via `GET /account?address=<pubkey>`:

```json
{
  "account": {
    "Address": "a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89",
    "Balances": {
      "BTC": 300000000,
      "USD": 84000000000000
    },
    "Nonce": 12
  },
  "leaf_hash": "e7207e25f944e1a4dc78e85be7ef3b8c138c85e47e5dd797ad54d61afca8ea0a",
  "merkle_proof_steps": 1,
  "merkle_proof_valid": true,
  "state_root": "ca47089dca3580d35a10514a32e67cfa9ae6350f439a38b966c220ee4e665957"
}
```

---

## Benchmarks & Fault Injection

`benchmarks/benchmark_test.go` executes an end-to-end integration test over local TCP sockets: initializes 5 validators, funds 100 institutional accounts, terminates **Node 4** mid-flight, submits **500 concurrent Atomic DvP swaps** across the remaining 4 nodes, and restarts Node 4 to verify autonomous Catch-Up Synchronization.

```text
Total Atomic DvP Trades Settled : 500
Throughput (TPS)                : 826.14 trades/sec
Settlement Latency p50          : 57.52ms
Settlement Latency p95          : 95.49ms
Settlement Latency p99          : 95.49ms
Node 4 Catch-Up State Sync      : VERIFIED (Synced to Height 11)
Merkle Proof Verification       : VERIFIED (7 tree steps for 100 accounts)
Final Consensus Merkle StateRoot: 922470a6e127dab5dee759453dbb4cbfac80a106a40c588f9cdf2b45787213d5
```

| Metric | Value | Notes |
| :--- | :--- | :--- |
| Sustained Throughput | ~826 TPS | Bounded by 50ms block interval and 50-tx batch cap |
| Median Finality (`p50`) | 57.5 ms | Single-round TCP RPC quorum vote + state preview |
| Tail Finality (`p99`) | 95.5 ms | Includes synchronous `file.Sync()` WAL flush |
| Fault Tolerance | $f = 1$ ($4/5$ nodes) | Zero transaction loss during simulated node crash |
| Merkle Proof Complexity | $\lceil \log_2(100) \rceil = 7$ hashes | 224-byte audit proof for 100 active accounts |

---

## Running the System

### Unit, Byzantine & Benchmark Tests
```bash
go test -v ./...
```

### 5-Node Docker Compose Cluster
Builds static Alpine images and starts `node0`–`node4` on an isolated bridge network:
```bash
docker compose up --build
```

| Node | TCP RPC | HTTP & Prometheus API |
| :--- | :--- | :--- |
| `node0` | `127.0.0.1:9000` | `http://localhost:8080` |
| `node1` | `127.0.0.1:9001` | `http://localhost:8081` |
| `node2` | `127.0.0.1:9002` | `http://localhost:8082` |
| `node3` | `127.0.0.1:9003` | `http://localhost:8083` |
| `node4` | `127.0.0.1:9004` | `http://localhost:8084` |

### CLI Settlement Simulation
In a separate terminal while the cluster is running:
```bash
# Fund initial accounts (1,000,000 USD & 50 BTC) and execute 1 Atomic DvP Swap
go run ./cmd/cli -mode=demo

# Stream 10 consecutive Atomic DvP Swaps (0.2 BTC <-> 10,000 USD) with automatic nonce resolution
go run ./cmd/cli -mode=trade -n=10
```

### HTTP Observability & Prometheus Endpoints
```bash
curl -s [http://127.0.0.1:8080/status](http://127.0.0.1:8080/status)
curl -s [http://127.0.0.1:8080/accounts](http://127.0.0.1:8080/accounts)
curl -s "[http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89](http://127.0.0.1:8080/account?address=a3fb25e72f2986a8c96948f43ae616dd3d06cf0ae044cf539ab4af0020b01a89)"
curl -s [http://127.0.0.1:8080/metrics](http://127.0.0.1:8080/metrics)
```

Stop the cluster:
```bash
docker compose down
```

---

## Architectural Scope & Known Trade-offs

* **Static Validator Set:** The 5-validator public key registry is deterministic at boot time; dynamic validator reconfiguration is out of scope.
* **Full-State Merkle Rebuild:** The binary Merkle tree is recomputed over sorted active accounts on commit rather than persisted as a copy-on-write Sparse Merkle Trie, trading write amplification for implementation simplicity and zero external storage dependencies.
* **WAL Compaction:** `ledger.wal` retains the full block history from genesis without periodic state snapshotting or log truncation.
