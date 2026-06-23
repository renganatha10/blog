+++
title = "Kafka Notes"
date = 2026-06-22
description = "Personal notes on Apache Kafka — brokers, producers, consumers, transactions, schema registry, and Avro serialization."
[taxonomies]
tags = ["kafka"]
+++

## Broker, ZooKeeper, KRaft, and Leader Election

### ZooKeeper Mode (legacy — removed in Kafka 4.0)

- On startup, each broker registers itself in ZooKeeper as an **ephemeral znode** (disappears automatically if the broker disconnects).
- One broker is elected as the **Controller** by competing to create `/controller` in ZooKeeper — first writer wins.
- The Controller watches ZooKeeper for events: brokers joining/leaving, topic changes, ISR changes.
- ZooKeeper runs its own consensus protocol internally (**ZAB — ZooKeeper Atomic Broadcast**) to keep its own replicas consistent — this is separate from Kafka's partition leader election.
- **Partition leader election**: when a broker dies, its ephemeral znode disappears; the Controller detects this and promotes the first replica in the ISR list as the new partition leader, then writes the new leader assignment back to ZooKeeper so all other brokers pick it up.
- **Drawback**: ZooKeeper was a separate cluster to operate; the Controller had to reload all metadata from ZooKeeper on restart (slow); metadata write path went ZooKeeper → Controller → brokers (multiple hops).

### KRaft Mode (production-ready since Kafka 3.3; ZooKeeper fully removed in Kafka 4.0)

- Kafka now manages its own metadata using the **Raft consensus algorithm** — no external ZooKeeper dependency.
- A subset of nodes (3 or 5) form the **KRaft quorum**. These can be dedicated controller nodes or combined broker+controller nodes.
- One controller in the quorum is elected **Active Controller** via Raft leader election (requires majority: 2 of 3, or 3 of 5).
- All metadata changes (topic creation, partition reassignments, leader elections) are written as entries in the Raft log, replicated to all quorum members before being committed.
- This log lives in an internal topic called **`__cluster_metadata`**.
- Brokers subscribe to the Active Controller and receive metadata updates as a stream — no ZooKeeper polling.

### How Raft Leader Election Works

Each node is always in one of three states: **Leader**, **Follower**, or **Candidate**.

1. Followers expect periodic **heartbeats** from the Leader.
2. If no heartbeat arrives within a randomised **election timeout**, a Follower promotes itself to **Candidate**, increments its term, and sends `RequestVote` RPCs to peers.
3. A Candidate wins if it receives votes from a **strict majority**; it then becomes the new Leader and starts sending heartbeats.
4. Randomised timeouts prevent multiple candidates from splitting votes indefinitely.
5. Raft guarantees **at most one Leader per term**, preventing split-brain scenarios.

### Partition Leader Election (applies to both modes)

- The **Active Controller** (KRaft) or **Controller broker** (ZooKeeper) monitors broker liveness.
- When a broker goes down, the Controller selects a new partition leader from the **ISR list** for every partition that lost its leader.
- The first eligible replica in the ISR (ideally the **preferred replica**) is promoted.
- If the ISR is empty and `unclean.leader.election.enable=true`, an out-of-sync replica can be elected — this risks **data loss** but keeps the partition available.
- The new leader assignment is broadcast to all brokers via metadata propagation.

---

## Producer

### Replication Factor and In-Sync Replicas

**Replication factor** is the total number of copies Kafka keeps for each partition — one leader and the rest as followers. With a replication factor of 3, there is 1 leader + 2 followers.

**In-Sync Replicas (ISR)** is the subset of those replicas that are currently caught up with the leader. A replica stays in the ISR as long as it has fetched up to the leader's latest offset within the `replica.lag.time.max.ms` window (default 30 s). If a follower falls behind — due to network issues, GC pause, or broker slowness — it is removed from the ISR. When it catches up again, it is re-added.

**`min.insync.replicas` (min.ISR)** is the key knob that connects the two:

- It sets the minimum number of replicas that must be in the ISR for the partition to accept writes **when `acks=all`**.
- If the live ISR count drops below this value, the partition rejects writes with `NotEnoughReplicasException`.

**Typical production setup:**

| Config | Value | Meaning |
|--------|-------|---------|
| `replication.factor` | 3 | 3 copies total |
| `min.insync.replicas` | 2 | At least 2 must acknowledge |
| `acks` | `all` | Producer waits for all ISR replicas |

This tolerates **1 broker failure** while maintaining strong durability. You never want `min.insync.replicas == replication.factor` in production — if any single replica is unavailable the partition becomes unwritable.

**The relationship in plain terms:**

```
replication.factor = 3   →  [Leader | Follower-1 | Follower-2]
ISR (all healthy)        →  [Leader | Follower-1 | Follower-2]  ✓ 3 in ISR

Follower-2 crashes:
ISR shrinks              →  [Leader | Follower-1]               ✓ 2 in ISR ≥ min.ISR (2), writes still allowed

Follower-1 also crashes:
ISR shrinks              →  [Leader]                            ✗ 1 in ISR < min.ISR (2), writes blocked
```

### Acknowledgements (`acks`)

- `0` — fire and forget; no acknowledgement waited for (fastest, no durability guarantee).
- `1` — leader writes to its local log and acknowledges; followers may not have replicated yet (leader loss = data loss).
- `all` — leader waits until all ISR replicas have written the batch before acknowledging; combined with `min.insync.replicas`, this is the strongest durability guarantee.

### `linger.ms` and `batch.size`

- The producer accumulates records into batches before sending to improve throughput.
- A batch is sent when either `batch.size` bytes are filled (default 16 KB) **or** `linger.ms` has elapsed (default 0 ms).
- Setting `linger.ms=5` and a larger `batch.size` is a common throughput tuning: the producer waits up to 5 ms to fill a bigger batch before flushing.

### Idempotent Producer

#### The Problem It Solves

Without idempotence, a transient network failure during a produce request can cause a duplicate:

1. Producer sends a batch → broker writes it → broker acknowledgement is lost in transit.
2. Producer times out and retries → broker writes the same batch **again**.
3. Consumer sees the message twice.

#### How It Works Internally

Enable with `enable.idempotence=true`. Kafka automatically enforces: `acks=all`, `retries=Integer.MAX_VALUE`, `max.in.flight.requests.per.connection ≤ 5`.

**Producer ID (PID):** On first connect, the producer sends an `InitProducerId` request to the broker. The broker assigns a globally unique **Producer ID (PID)** and returns it. The PID identifies this producer session.

**Sequence Numbers:** The producer maintains a monotonically increasing **sequence number** per `(PID, TopicPartition)`, starting at 0. Every record gets stamped with the current sequence number before it is sent. The sequence number is part of the message metadata on the wire.

**Broker-side deduplication:** The broker tracks the **last committed sequence number** for every `(PID, partition)` pair.

- If a batch arrives with an **already-seen sequence number** → silent deduplicate, return success to producer.
- If a batch arrives **in order** (seq = last + 1) → commit, advance the tracked sequence.
- If a batch arrives with a **gap** (seq > last + 1) → reject with `OutOfOrderSequenceException`; this signals a bug, not a retry scenario.

This means retries are safe — the producer resends the same batch with the same sequence numbers and the broker simply ignores the duplicate.

#### Internal Batching Behaviour

The producer client has two internal components:

**`RecordAccumulator`** — an in-memory buffer organised as a map of `TopicPartition → Deque<RecordBatch>`. `send()` calls are non-blocking: they append the record to the appropriate batch in the accumulator and return a `Future`.

**`Sender` thread** — a single background thread that drains the accumulator and dispatches batches to brokers over the network.

With idempotent producer the Sender adds two guarantees:

1. **Sequence assignment happens in the Sender thread**, not the calling thread, so sequence numbers are assigned in the exact order batches are drained from the accumulator. This keeps ordering consistent even when multiple application threads call `send()` concurrently.
2. **In-flight tracking per partition**: the Sender keeps a queue of in-flight batches per `(broker, partition)`. On a retriable error, the batch is **re-queued at the front** of the deque with its original sequence numbers intact — the batch object is not recreated. This preserves the ordering the broker expects.

```
Application thread(s)         Sender thread           Broker
   send(record) ──►  RecordAccumulator  ──batch──►  seq check
                      [TP-A: [batch0]]              deduplicate
                      [TP-B: [batch1]]              commit & ack
                           │
                     on retry: requeue
                     batch at front with
                     same PID + seq
```

**Why `max.in.flight.requests.per.connection=5` is safe:** With plain retries (no idempotence) having multiple in-flight batches means a retry of batch-1 could arrive after batch-2 has already been committed — messages reorder. With idempotence the broker rejects any batch that arrives with a sequence number that creates a gap, so reordering is caught at the broker. Five in-flight batches is a deliberate balance: enough parallelism for throughput, small enough to bound the sequence tracking window on the broker.

---

## Consumer

### Consumer Group and Partition Assignment

A **consumer group** is a set of consumer instances sharing a single `group.id`. Kafka guarantees that each partition of a topic is assigned to **exactly one consumer** within the group at any given time — this is the fundamental unit of parallelism.

- **Scaling rule**: the maximum useful parallelism equals the number of partitions. Adding more consumers than partitions leaves the extras idle.
- **Group Coordinator**: one broker acts as the group coordinator, tracking membership and managing rebalances. The coordinator is determined by hashing the `group.id` to a partition of the internal `__consumer_offsets` topic.
- **Group Leader**: one consumer in the group (the first to join) is elected leader by the coordinator. The leader runs the **partition assignment algorithm** (RoundRobin, Range, Sticky, or a custom assignor) and sends the result back to the coordinator, which distributes assignments to all members.

**Rebalance** is triggered when a consumer joins, leaves, or crashes, or when topic partitions change. During a rebalance all consumption pauses. The **Sticky Assignor** minimises disruption by keeping existing assignments unchanged and only moving partitions that have to move.

**Message ordering** is guaranteed **within a partition** — consumers receive records in the exact order they were produced to that partition. There is no ordering guarantee across partitions. Since each partition is assigned to exactly one consumer in a group, that consumer processes its partition's records in offset order.

### Batch Consumer

Consumers do not pull one record at a time — `poll(duration)` returns a batch of up to `max.poll.records` records (default 500) in a single call.

```java
while (true) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<K, V> record : records) {
        process(record);
    }
    consumer.commitSync();
}
```

Key configs:

| Config | Default | Purpose |
|--------|---------|---------|
| `max.poll.records` | 500 | Max records returned per `poll()` call |
| `fetch.min.bytes` | 1 | Broker waits until this many bytes are available before responding |
| `fetch.max.wait.ms` | 500 ms | Max time broker will wait to satisfy `fetch.min.bytes` |
| `max.partition.fetch.bytes` | 1 MB | Max bytes fetched per partition per request |

Tuning `fetch.min.bytes` and `fetch.max.wait.ms` together controls the latency-throughput tradeoff on the broker side: higher `fetch.min.bytes` means larger batches but more latency before the first record arrives.

### Manual Acknowledgement

By default Kafka auto-commits offsets in the background every `auto.commit.interval.ms` (default 5 s). This can cause **at-most-once** semantics: if the consumer crashes after auto-commit but before finishing processing, records are skipped.

Set `enable.auto.commit=false` and commit explicitly for **at-least-once** guarantees.

- **`commitSync()`** — blocks until the broker confirms the commit. Safe but slows throughput.
- **`commitAsync()`** — non-blocking; fires the commit and provides a callback for success/failure. Higher throughput, but a failed async commit is not retried by default — retrying it naively can cause out-of-order commits (a later offset commits first, then a retry commits an earlier offset over it, losing progress).

**Common pattern**: use `commitAsync()` in the hot path and `commitSync()` only in the `finally` block on shutdown to ensure the last offsets are flushed.

```java
try {
    while (running) {
        var records = consumer.poll(Duration.ofMillis(100));
        process(records);
        consumer.commitAsync((offsets, ex) -> { if (ex != null) log.warn(...); });
    }
} finally {
    consumer.commitSync();   // best-effort flush on shutdown
    consumer.close();
}
```

### Nack / Retry (Single-Record Failure)

Kafka has no native negative-acknowledgement. When a single record fails processing:

1. **Do not commit** the offset of the failed record — it will be re-delivered on the next `poll()` after a restart or rebalance.
2. **Seek back** explicitly: call `consumer.seek(partition, failedOffset)` to re-position the consumer to the failed record without waiting for a crash/rebalance.
3. **Route to a retry topic**: instead of blocking, publish the failed record to a `<topic>.retry` topic and commit the original offset. A separate consumer processes the retry topic with backoff logic.

Option 2 (seek back) is simple but blocks the partition — all records after the failed one are stuck until the failure is resolved. Option 3 (retry topic) is preferred for production as it decouples failure handling from the main consumer's throughput.

### Batch Acknowledge

When processing records as a batch (e.g., bulk-inserting into a database), commit only after the **entire batch** succeeds:

```java
var records = consumer.poll(Duration.ofMillis(100));
bulkInsert(records);      // if this throws, do NOT commit
consumer.commitSync();    // commit only on success
```

This gives at-least-once semantics at batch granularity. If the bulk insert fails partway through, the whole batch is reprocessed on the next poll — so `bulkInsert` should be idempotent (e.g., use upserts or de-duplicate on the database side).

To commit at record granularity within the batch, use `commitSync(Map<TopicPartition, OffsetAndMetadata>)` with a precise offset map rather than committing all assigned partitions at once.

### Heartbeat Thread and Liveness Detection

The consumer runs a **dedicated background heartbeat thread** separate from the application thread that calls `poll()`. Liveness has two independent signals:

- **`session.timeout.ms`** (default 45 s) — if the broker does not receive a heartbeat within this window, it considers the consumer dead and triggers a rebalance. The heartbeat thread sends heartbeats every `heartbeat.interval.ms` (default 3 s; recommended: ≤ `session.timeout.ms / 3`).
- **`max.poll.interval.ms`** (default 5 min) — if the application thread does not call `poll()` again within this window, the consumer is also considered dead. This catches the case where the heartbeat thread is alive but the application is stuck processing a record and never returns to the poll loop.

```
Heartbeat thread    ──heartbeat──►  Group Coordinator  ← session.timeout.ms
Application thread  ──poll()──►     (same coordinator)  ← max.poll.interval.ms
```

Both timeouts must be satisfied to keep the consumer in the group. If `max.poll.interval.ms` is exceeded, the consumer voluntarily leaves the group and triggers a rebalance — even if the heartbeat thread is still healthy.

**Tuning guidance**: if record processing is genuinely slow (e.g., calling an external API per record), increase `max.poll.interval.ms` or reduce `max.poll.records` so each `poll()` returns fewer records to process within the window.

### Non-Blocking Retry Pattern (Retry Topics)

The retry-topic pattern decouples failure handling from the main consumer loop, keeping throughput high on the happy path.

```
main-topic  ──►  [Consumer]  ──failure──►  topic.retry-1
                     │                          │
                   success              [Retry Consumer, delay=30s]
                     │                          │──failure──►  topic.retry-2
                  commit                        │                    │
                                             success        [Retry Consumer, delay=5m]
                                              commit               │──failure──►  topic.dlt
```

1. Main consumer catches a processing exception → publishes the original record (with added headers: `retry-count`, `original-topic`, `error-message`) to `topic.retry-1` → commits the offset and continues.
2. A dedicated retry consumer subscribes to `topic.retry-1`, applies a delay (`Thread.sleep` or a scheduled poll pause), and retries the record.
3. On repeated failure the record is promoted to `topic.retry-2` (longer delay), and eventually to a **Dead Letter Topic (DLT)** for manual inspection.

Spring Kafka's `@RetryableTopic` implements this pattern automatically with configurable backoff and DLT routing.

### Circuit Breaker

When a downstream dependency (database, external API) is degraded, continuing to consume and fail records wastes resources and floods retry topics. A circuit breaker pauses consumption during outages.

**States**: Closed (normal) → Open (paused) → Half-Open (testing recovery).

**Kafka-specific implementation**:

```java
// On repeated failures, open the circuit:
consumer.pause(consumer.assignment());   // stop fetching, stay in group

// poll() must still be called to send heartbeats and avoid max.poll.interval.ms breach:
while (circuitOpen) {
    consumer.poll(Duration.ofMillis(100));  // returns empty — partitions are paused
    if (dependencyHealthy()) {
        consumer.resume(consumer.assignment());
        circuitOpen = false;
    }
}
```

`consumer.pause()` stops the broker from returning records for those partitions but keeps the consumer in the group and heartbeating. This is critical — if you simply stop calling `poll()` you will breach `max.poll.interval.ms` and trigger a rebalance.

The circuit opens after N consecutive failures (configurable threshold), waits a cooldown period, then enters half-open state where a single test record is processed to confirm recovery before fully resuming.

---

## Transactions

Kafka transactions give you **exactly-once semantics (EOS)** across produce and consume operations. Without understanding what the broker actually writes — and what the consumer actually reads — the guarantee is easy to break silently.

### What a Transaction Actually Writes to a Partition

When a producer sends messages inside a transaction, Kafka does **not** immediately make those messages visible to consumers. The broker writes them to the partition log but marks them as part of an open transaction. Two types of special records are involved:

**1. Regular data records** — written with the producer's `PID` (Producer ID) and a transaction flag set in the record batch header. They sit in the log at their assigned offsets, but are invisible to `read_committed` consumers until the transaction commits.

**2. Transaction markers (control records)** — written by the broker after the producer calls `commitTransaction()` or `abortTransaction()`. A marker is a special log entry at a new offset that signals `COMMIT` or `ABORT` for a given `PID`.

```
Partition log (offsets):

offset 0  │ data record  (PID=42, txn=open)   ← written during transaction
offset 1  │ data record  (PID=42, txn=open)   ← written during transaction
offset 2  │ COMMIT marker (PID=42)             ← written by broker on commitTransaction()
```

Without the `COMMIT` marker at offset 2, offsets 0 and 1 are not considered committed data — they are in limbo. An `ABORT` marker instead would tell consumers to discard offsets 0 and 1 entirely.

### The Transaction Coordinator and `__transaction_state`

Kafka has a dedicated internal component called the **Transaction Coordinator** — one per broker, selected by hashing the producer's `transactional.id`. It manages transaction lifecycle and persists state in the internal topic **`__transaction_state`**.

Transaction flow step by step:

```
Producer                        Transaction Coordinator           Partition Leaders
   │                                     │                              │
   ├─ initTransactions() ───────────────►│ assign PID + epoch           │
   │◄─ PID, epoch ───────────────────────┤                              │
   │                                     │                              │
   ├─ beginTransaction() [local only]    │                              │
   │                                     │                              │
   ├─ send(records) ─────────────────────┼──────────────────────────────► written, txn=open
   │                                     │                              │
   ├─ sendOffsetsToTransaction() ───────►│ record consumed offsets      │
   │                                     │  in __transaction_state      │
   │                                     │                              │
   ├─ commitTransaction() ──────────────►│ write COMMIT to              │
   │                                     │  __transaction_state         │
   │                                     ├─ write COMMIT marker ────────► offset 2: COMMIT(PID=42)
   │◄─ ack ──────────────────────────────┤                              │
```

The `epoch` prevents zombie producers: if a producer crashes and restarts with the same `transactional.id`, it gets a new higher epoch. The old epoch is fenced — any in-flight batches from the old instance are rejected by brokers.

### `isolation.level=read_committed` — What It Does

This is a **consumer-side config**. It controls which records from the partition log the consumer is allowed to see.

**`read_uncommitted` (default)** — the consumer reads every record at every offset as soon as it lands in the log, regardless of transaction state. It sees records from committed transactions, in-flight (open) transactions, and aborted transactions alike.

**`read_committed`** — the consumer reads only records that are part of a committed transaction (or records written outside of any transaction). It does this by tracking the **Last Stable Offset (LSO)**:

```
LSO = the offset up to which all transactions are resolved (committed or aborted)

Partition log:
offset 0  │ data (PID=42, txn=open)
offset 1  │ data (PID=42, txn=open)
offset 2  │ COMMIT marker (PID=42)   ← LSO advances past here once this is written
offset 3  │ data (PID=99, txn=open)  ← LSO stops here; txn still open
offset 4  │ data (PID=99, txn=open)
```

A `read_committed` consumer can consume up to and including offset 2 (the LSO). Offsets 3 and 4 are held back until PID=99's transaction resolves. Additionally, the broker sends an **abort index** alongside the fetch response — the consumer uses this to silently skip over records that belong to aborted transactions.

The consumer never sees the transaction markers themselves — those are internal control records filtered out by the client library.

### Why `read_uncommitted` Makes Transactions Useless

Suppose a producer reads from topic A, transforms the records, and writes to topic B — all inside a transaction. The goal is: either all writes to B succeed atomically, or none are visible.

**With `read_uncommitted` on the consumer of topic B:**

```
t=0  Producer begins transaction
t=1  Producer writes record-1 to topic B  (offset 0, txn=open)
t=2  Producer writes record-2 to topic B  (offset 1, txn=open)
t=3  Consumer of B reads offset 0 → sees record-1   ← PROBLEM
t=4  Producer crashes before commitTransaction()
t=5  Transaction Coordinator writes ABORT marker
t=6  Consumer of B never reads offset 1
```

The consumer already processed record-1 at t=3 — before the transaction was resolved. The producer aborted, so record-1 was never meant to be visible. But the `read_uncommitted` consumer has no way to know that — it already committed the offset and acted on the data. **The atomicity guarantee is completely broken.**

With `read_committed`, the consumer at t=3 checks the LSO — offsets 0 and 1 are behind an open transaction, so the LSO has not advanced. The consumer's fetch returns nothing. At t=5, when the ABORT marker lands, the broker sends the abort index and the consumer skips both records. The partition appears empty — exactly the correct behaviour.

### Exactly-Once Consume-Transform-Produce

The canonical EOS pattern in Kafka is:

```
[Source Topic] ──► Consumer (read_committed) ──► Transform ──► Producer (transactional) ──► [Sink Topic]
                                                                     │
                                                          sendOffsetsToTransaction()
                                                          (commits consumed offsets
                                                           atomically with the produce)
```

`sendOffsetsToTransaction()` registers the consumer group's offsets as part of the current transaction. When the transaction commits, **both** the output records in the sink topic **and** the input offsets in `__consumer_offsets` are committed atomically. If the transaction aborts, neither is visible. This prevents the double-processing that would happen if the produce succeeded but the offset commit failed (or vice versa).

| Component | Config | Effect |
|-----------|--------|--------|
| Producer | `transactional.id` | Enables transactional API; assigns stable PID + epoch |
| Producer | `enable.idempotence=true` | Automatically enabled with `transactional.id` |
| Consumer | `isolation.level=read_committed` | Only sees records from committed transactions |
| Consumer | `sendOffsetsToTransaction()` | Ties offset commit to the current transaction |

Without `isolation.level=read_committed` on every consumer downstream of the transactional producer, the transaction boundary means nothing — consumers will read and act on data from transactions that may still abort.

---

## Schema (Schema Registry)

**Compatibility modes:**

| Mode | Meaning |
|------|---------|
| **Backward** | New schema can read data written with the old schema. Upgrade **consumers first**. |
| **Forward** | Old schema can read data written with the new schema. Upgrade **producers first**. |
| **Full (Transitive)** | Both backward and forward compatible. |

**Field changes and compatibility:**

- Adding a field with a default → backward compatible.
- Removing a field that had a default → forward compatible.
- Adding a required field (no default) → breaking change.
- Removing a required field → breaking change.

---

## Avro — Binary Serialization and Payload Efficiency

Avro is a binary serialization format designed by Apache. In Kafka it is the most common choice for encoding messages because it significantly reduces wire size compared to JSON or XML.

### Why Avro Saves Payload Size

**1. Schema is stored separately — not in every message.**

With the Confluent Schema Registry, the schema is registered once and assigned an integer ID. Each Kafka message contains only a compact 5-byte header:

```
[0x00]  [schema-id: 4 bytes]  [binary payload...]
 magic     registry pointer
```

Field names, types, and structure are never repeated in the message itself.

**2. No key-value overhead.**

JSON must embed field names as strings in every record:

```json
{"userId": 12345, "name": "Alice", "active": true}
```

Avro writes only the values in the schema-defined order, binary-encoded — no keys, no quotes, no braces, no colons.

**3. Compact binary encoding for values.**

- **Integers**: variable-length zig-zag encoding. Small integers (0–63) fit in 1 byte. The number `12345` encodes to 3 bytes vs. 5 characters in JSON.
- **Strings**: length-prefixed (varint length + raw UTF-8 bytes) — no surrounding quotes.
- **Booleans**: 1 byte.
- **Null**: 0 bytes in a union — nothing on the wire.

**4. Concrete size comparison** — a simple record: `userId=1`, `name="Alice"`, `active=true`:

| Format | Encoded size (approx.) |
|--------|------------------------|
| JSON | ~40 bytes |
| Avro binary (5-byte header + payload) | ~13 bytes |

Roughly 60–70% smaller in practice for typical domain objects. Savings grow larger as the number of fields increases, because JSON repeats every field name while Avro never does.

### Avro Schema Example

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "userId", "type": "int"},
    {"name": "name",   "type": "string"},
    {"name": "active", "type": "boolean"}
  ]
}
```

### How the Schema Registry Fits In

1. **Producer** serializes a record using the Avro schema → checks/registers the schema in the Registry → receives a schema ID → prepends the 5-byte header → sends to Kafka.
2. **Consumer** reads a message → reads the schema ID from the header → fetches the schema from the Registry (cached after first fetch) → deserializes the binary payload.

The schema is fetched **once per unique schema ID** and cached locally — subsequent deserialization is a pure in-memory lookup.
