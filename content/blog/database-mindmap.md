+++
title = "Database Mind Map"
date = 2026-06-23
description = "A technical survey of database internals — storage engines, SQL vs NoSQL, scaling strategies, and transaction models from B-tree roots to LSM-tree writes."
[taxonomies]
tags = ["databases", "system-design"]
+++

## The Starting Point — SQL Databases

### How Data is Stored on Disk

A relational database stores rows in **pages** (typically 8 KB each). Pages are grouped into files on disk — one per table. When you insert a row it lands on whatever page has free space. There is no inherent sort order; rows for the same user might be scattered across dozens of pages.

```
Disk (heap file)
┌──────────┬──────────┬──────────┐
│  Page 1  │  Page 2  │  Page 3  │  ...
│ row 1    │ row 4    │ row 7    │
│ row 2    │ row 5    │ row 8    │
│ row 3    │ row 6    │ row 9    │
└──────────┴──────────┴──────────┘
```

Without an index, every query reads every page — a full table scan. Fine for small tables, slow for millions of rows.

### Indexes — Fast Paths to Your Data

An index is a separate data structure that maps column values to the page(s) where matching rows live. The database reads the index first, then jumps directly to only the relevant pages.

**B-Tree Index** — the default. Stores column values in sorted order in a tree. Good for equality (`=`) and range queries (`>`, `<`, `BETWEEN`). Most columns you index get a B-tree.

```sql
CREATE INDEX ON orders (user_id);
-- lookup user_id = 42: walk the tree → find page pointers → fetch only those pages
```

**Hash Index** — stores a hash of the column value. Faster than B-tree for exact equality lookups, but useless for ranges. Rarely used because B-tree is fast enough and more versatile.

**Composite Index** — a B-tree built on multiple columns together. Useful when queries filter on more than one column. Order matters: an index on `(user_id, created_at)` helps queries that filter by `user_id` or by `user_id + created_at`, but not queries that only filter by `created_at`.

```sql
CREATE INDEX ON orders (user_id, created_at);
```

**Clustered vs Non-Clustered** — in MySQL (InnoDB) the primary key index physically orders the rows on disk (clustered). All other indexes are non-clustered — they point back to the primary key which points to the row. PostgreSQL has no clustered index by default; all indexes are pointers to the heap.

### The Trade-off

Every index speeds up reads but slows down writes — on every insert or update the database must update both the table and all its indexes. Add indexes where query speed matters; avoid indexing columns that are rarely queried.

This setup works well on a single machine. Transactions are ACID. Joins span any table. The problem arrives when data grows beyond what one machine can hold.

---

## Storage Engine Internals — B-Tree vs LSM Tree

There are two dominant storage engine architectures in the database world. Everything else is a variation or hybrid.

### B-Tree Engines

A B-tree engine updates data **in place**. When you update a row, it finds the page on disk that holds it and writes the new value there. This is efficient for reads — the data is always at a known location — but writes are random I/O. Every update must seek to the right page before writing.

**Where you find B-tree engines:**

| Database | Engine |
|----------|--------|
| PostgreSQL | Heap access method + B-tree indexes |
| MySQL / MariaDB | InnoDB (B-tree, MVCC via undo logs) |
| MongoDB | WiredTiger (defaults to B-tree row format; also supports LSM) |
| SQLite | Custom B-tree page format |

PostgreSQL and InnoDB both implement **MVCC (Multi-Version Concurrency Control)** on top of B-tree storage. Instead of locking a row during an update, they write a new version and let concurrent readers see the old version until their transaction is done. PostgreSQL keeps old row versions in the heap itself (called "dead tuples"), while InnoDB keeps them in a separate undo log.

### LSM-Tree Engines

LSM stands for **Log-Structured Merge-Tree**. Instead of finding the right spot on disk and updating in place, an LSM engine always **appends**. Every write goes into an in-memory buffer first, then gets flushed to disk as an immutable file. This turns random I/O into sequential I/O, which is dramatically faster — especially on spinning disks, but also beneficial on SSDs because it reduces write amplification.

**The LSM write path:**

```
Write ──► Commit Log (WAL on disk, for durability)
       ──► Memtable (in-memory sorted buffer)
              │
              ▼ (when Memtable fills up, ~64–256 MB)
          SSTable (Sorted String Table, immutable file on disk)
              │
              ▼ (background process)
          Compaction (merge multiple SSTables, remove deleted/overwritten keys)
```

**The trade-off:** Writes are extremely fast because they are sequential and in-memory first. Reads are slower because the same key might exist across multiple SSTables at different levels, and the engine must check them all (or use bloom filters to skip most of them).

---

## MongoDB — Document Store on WiredTiger

MongoDB stores data as **BSON documents** (binary JSON) grouped into collections. There is no fixed schema — each document in a collection can have different fields.

### How Data is Stored

Under the hood MongoDB uses **WiredTiger** as its storage engine. WiredTiger stores documents in a B-tree by default, with each document identified by `_id`. Documents are stored as-is — nested objects, arrays, and all — in a single record on disk.

```
Collection: orders
──────────────────────────────────────────
{ _id: 1, user: "Alice", items: [...], address: { city: "NY" } }
{ _id: 2, user: "Bob",   items: [...] }                          ← no address field, that's fine
{ _id: 3, user: "Alice", items: [...], tags: ["urgent"] }        ← extra field, also fine
```

No joins needed to load a full order — everything is in one document. When you do need to query across collections, MongoDB supports `$lookup` in the aggregation pipeline, though it is heavier than a SQL join and best avoided on hot paths.

### Indexes

MongoDB supports the same index types as SQL databases, all built as B-trees on top of WiredTiger:

- **Single field** — `{ user_id: 1 }`, works for equality and range.
- **Compound** — `{ user_id: 1, created_at: -1 }`, same left-prefix rules as SQL composite indexes.
- **Multikey** — automatically created when indexing an array field; one index entry per array element.
- **Text** — inverted index for full-text search on string fields.
- **TTL** — special single-field index that auto-expires documents after a time threshold; used for session data, logs.

### Transactions

Single-document operations have always been atomic in MongoDB. Multi-document ACID transactions were added in MongoDB 4.0 — they work but carry a performance cost, so the recommended pattern is still to model data so most operations touch one document.

---

## ClickHouse — Columnar Storage and the MergeTree

ClickHouse is an OLAP (Online Analytical Processing) database built for analytical queries over billions of rows. It combines two key ideas: **columnar storage** and a **MergeTree** engine family that resembles LSM in structure.

### Columnar Storage

Traditional (row-based) storage writes each row contiguously on disk. Columnar storage writes each column contiguously instead.

```
Row store:     [id=1, name="Alice", age=30] [id=2, name="Bob", age=25] ...
Column store:  [id: 1, 2, 3, ...]  [name: "Alice", "Bob", ...]  [age: 30, 25, ...]
```

When you run `SELECT avg(age) FROM users`, a column store reads only the `age` column — a tiny fraction of the total data. A row store reads every row, including `id` and `name` that are not needed.

Columnar layout also compresses far better because values in the same column share a data type and often share value ranges. ClickHouse applies **Run-Length Encoding (RLE)** and other codecs per column — a column that repeats "US" for 10 million rows compresses to almost nothing.

### MergeTree

ClickHouse's **MergeTree** engine is the write path that makes high-throughput ingestion practical:

1. **Inserts land as data parts** — each INSERT batch creates a new immutable directory of column files on disk.
2. **Background merges** — ClickHouse continuously merges small parts into larger parts in the background (analogous to SSTable compaction in Cassandra).
3. **Sorted by primary key** — each part is sorted by the table's `ORDER BY` key, enabling fast primary-key range scans.

```
INSERT batch 1 ──► part_1 (sorted column files)
INSERT batch 2 ──► part_2 (sorted column files)
INSERT batch 3 ──► part_3 (sorted column files)
                              │
                   Background merge
                              │
                              ▼
                   merged_part (larger sorted column files)
```

This is why ClickHouse requires **bulk inserts** (thousands of rows at a time) rather than single-row inserts — each insert creates a part, and too many tiny parts slow down merges and queries until background merges catch up. The recommended pattern is to buffer inserts in the application and flush in batches.

**ReplicatedMergeTree** adds multi-node replication using ZooKeeper (or ClickHouse Keeper) to coordinate which merges are happening and replicate parts across nodes.

---

## Traditional SQL at Scale — The Limits

The typical growth path for a SQL application looks like this:

```
Single PostgreSQL node
    │
    ▼ (read traffic grows)
Primary + Read Replicas (streaming replication)
    │
    ▼ (write traffic grows, single primary bottleneck)
Table Partitioning
    │
    ▼ (outgrows single machine)
Sharding
```

### Read Replicas

PostgreSQL replication streams WAL records from the primary to standbys. Reads can be directed to standbys, but **all writes must go to the primary**. This solves read scaling but does nothing for write scaling — the primary is still a single bottleneck.

### Table Partitioning

PostgreSQL supports declarative partitioning — a logical table split across multiple physical child tables, by range, list, or hash. Queries that filter on the partition key only scan relevant partitions ("partition pruning"). Partitioning lives within a single database instance; it does not distribute load across machines.

### Sharding

Sharding splits the data across completely separate database instances — each **shard** holds a subset of rows. A routing layer directs queries to the correct shard based on a shard key (typically hashed user ID, tenant ID, etc.).

The benefits are real: write throughput scales linearly with the number of shards, and each shard can live on its own machine in any data center. The costs are severe:

- **No cross-shard joins** — queries that touch rows on multiple shards require application-level data assembly.
- **No cross-shard transactions** — distributed transactions (2PC) are possible but expensive and rarely implemented well.
- **Re-sharding is painful** — moving data when you need to add shards requires careful migration and downtime planning.
- **Schema changes are multiplied** — you apply migrations to every shard.

Tools like Citus (PostgreSQL extension), Vitess (MySQL), and PlanetScale (MySQL) exist precisely to make sharding less painful, but the fundamental constraints remain.

---

## NoSQL — Scaling as a First Principle

NoSQL databases were built with horizontal scaling as a design constraint, not an afterthought. The trade-offs they accepted to achieve this:

- **No joins** — data must be modeled for the queries you will run, not normalized into tables.
- **Flexible schema (document stores)** — each document can have different fields; schema enforcement is the application's responsibility.
- **Eventual consistency as the default** — most NoSQL databases favor availability over strong consistency during a partition.

### Document Stores (MongoDB)

Documents (JSON-like objects) are stored together, so a read that previously required joining `users`, `orders`, and `addresses` now reads a single document. No join needed — but you can no longer query across embedded data the way SQL can.

MongoDB uses WiredTiger under the hood and supports single-document ACID transactions. Multi-document transactions exist since MongoDB 4.0 but carry a performance cost.

### Key-Value Stores (Redis, DynamoDB)

The simplest model — a distributed hash map. Every access is by a primary key. Redis keeps all data in memory with optional persistence (RDB snapshots and AOF log). Sub-millisecond latency is the selling point.

**DynamoDB** is AWS's managed key-value / document store. It partitions data automatically using consistent hashing, and each partition holds up to 10 GB and handles up to 3,000 read capacity units or 1,000 write capacity units. When a partition exceeds these limits, DynamoDB splits it automatically — the operational burden of repartitioning falls on AWS.

### Wide-Column Stores (Cassandra, HBase)

Wide-column databases store data as rows keyed by a partition key, with each row holding a variable number of columns. They have a schema (unlike document stores) but no foreign keys and no joins. Cassandra distributes rows across nodes using consistent hashing on the partition key — given a partition key, Cassandra knows which node(s) own it with no routing table lookup.

---

## Achieving High Availability — Replication in NoSQL

### Cassandra's Replication Model

Every row in Cassandra is replicated across **N** nodes (configured per keyspace as the **replication factor**). With `replication_factor=3`, three nodes hold a copy of each row. Which nodes? The token ring assigns each node a range of the hash space; a row's partition key is hashed and placed at the appropriate position on the ring, then copied to the next N-1 nodes clockwise.

**Consistency is tunable per query:**

| Consistency Level | Writes | Reads |
|-------------------|--------|-------|
| `ONE` | 1 replica acknowledgement | Read from 1 replica |
| `QUORUM` | Majority (N/2 + 1) must acknowledge | Read from majority |
| `ALL` | All replicas must acknowledge | Read from all replicas |
| `LOCAL_QUORUM` | Quorum within local data center only | — |

`QUORUM` reads + `QUORUM` writes gives you **strong consistency** even with eventual replication, because the quorum sets overlap and always include at least one node with the latest data. `ONE` on both sides gives maximum availability but may return stale reads.

### DynamoDB's High Availability

DynamoDB replicates each partition across three Availability Zones within a region. Writes are committed synchronously to a majority (2 of 3) before acknowledging. Global Tables extend this across multiple AWS regions, with multi-active writes — every region accepts writes and replicates asynchronously to others (last-writer-wins conflict resolution).

---

## Transactions and Isolation — ACID vs BASE

### ACID (Relational Databases)

**Atomicity** — a transaction either commits all its changes or rolls all of them back. There is no partial success.

**Consistency** — the database transitions from one valid state to another. Constraints (foreign keys, unique indexes, CHECK constraints) are enforced.

**Isolation** — concurrent transactions appear to execute serially. The degree of isolation is configurable:

| Isolation Level | Dirty Reads | Non-Repeatable Reads | Phantom Reads |
|-----------------|-------------|----------------------|---------------|
| Read Uncommitted | Possible | Possible | Possible |
| Read Committed | Prevented | Possible | Possible |
| Repeatable Read | Prevented | Prevented | Possible |
| Serializable | Prevented | Prevented | Prevented |

PostgreSQL's default is **Read Committed** — you never see uncommitted data from other transactions, but a second read within your transaction may see rows that another transaction committed between your two reads. PostgreSQL's **Serializable** level uses Serializable Snapshot Isolation (SSI), which detects conflicting transaction patterns and aborts one of them rather than using table-level locks — making it practical for production use.

**Durability** — a committed transaction survives crashes. Both PostgreSQL and InnoDB achieve this via **Write-Ahead Logging (WAL)**: changes are written to the WAL on disk before they are applied to data pages. On crash recovery, the WAL is replayed.

### BASE (NoSQL Databases)

**Basically Available** — the system always returns a response, even if it is stale or an error during a partition, rather than waiting until the network heals.

**Soft State** — the system's state may change over time even without new input, as replication catches up.

**Eventually Consistent** — given no new updates, all replicas will converge to the same value *eventually*. The window can be milliseconds to seconds under normal conditions.

Cassandra is the canonical BASE database. By default, a write is acknowledged after one replica receives it. Other replicas catch up asynchronously. A subsequent read from a different replica may return the old value until replication completes. **Hinted handoff** and **read repair** are the two mechanisms Cassandra uses to drive convergence:

- **Hinted handoff** — if a replica is temporarily down when a write occurs, the coordinator stores a "hint" and replays the write when the replica comes back.
- **Read repair** — when a read detects divergence across replicas (by comparing digests), it writes the latest value back to stale replicas in the background.

### CAP Theorem

A distributed system can only guarantee two of three properties simultaneously:

- **Consistency** — every read returns the most recent write.
- **Availability** — every request receives a response (not an error).
- **Partition Tolerance** — the system continues operating when network partitions occur.

Since network partitions are a physical reality that cannot be engineered away, the real choice is between **CP** and **AP**:

| System | Choice | Behavior During Partition |
|--------|--------|--------------------------|
| PostgreSQL (single node) | CA | Partition impossible by definition |
| CockroachDB / Spanner | CP | Some requests may block or fail to preserve consistency |
| Cassandra (ONE consistency) | AP | Always responds, may return stale data |
| Cassandra (QUORUM) | CP | May reject writes if quorum unavailable |
| DynamoDB (eventual) | AP | Always responds |
| DynamoDB (strong) | CP | May fail reads if majority unavailable |

The key insight is that most NoSQL databases let you **dial the CAP trade-off per query** via consistency levels, rather than making it a fixed architectural decision.

---

## Summary — Choosing a Database

```
What is your primary concern?

Structured data + complex queries + ACID?
    └─► PostgreSQL (single node or read replicas)
         └─► Citus / Vitess / PlanetScale if you need sharding

High write throughput + time-series / wide rows + geographic distribution?
    └─► Cassandra

Analytical queries over billions of rows?
    └─► ClickHouse (OLAP) or BigQuery / Redshift (managed)

Sub-millisecond latency + caching?
    └─► Redis

Managed, serverless, infinite scale on AWS?
    └─► DynamoDB

Document model + flexible schema?
    └─► MongoDB (with the trade-off that joins become application logic)
```

No database is universally best. The decision is always about which trade-offs — write amplification vs read latency, strong consistency vs availability, schema rigidity vs flexibility — align with your workload's actual access patterns.

---

## SQL Can Scale Too — So Why Does NoSQL Exist?

A common misconception is that NoSQL exists purely because SQL cannot scale. That was the rallying cry in 2009, but it was never the full story — and today it is largely outdated.

**SQL scales just fine** with the right tooling:

- **Read replicas** — distribute read traffic across standbys.
- **Table partitioning** — split large tables across physical partitions, pruned automatically at query time.
- **Sharding** — Citus (PostgreSQL), Vitess (MySQL), PlanetScale distribute writes across machines.
- **NewSQL** — CockroachDB and Google Spanner give you full SQL with global horizontal scaling built in.

So if SQL can scale, why choose MongoDB or any other NoSQL database? The real reasons have always been about **developer experience and data model**, not scaling.

### What NoSQL Actually Wins On

**No schema** — you can store a document without defining its shape upfront. Add a new field to one document and nothing breaks. No `ALTER TABLE`, no migration script, no downtime.

**JSON-like documents** — a user with nested addresses, preferences, and order history maps directly to one document. No joins needed to reassemble it. What you store is what you get back.

**Developer flexibility** — spin up a collection and start inserting. No schema file, no ORM, no migration workflow. For fast-moving teams or evolving data shapes this removes real friction.

### The Honest Trade-off

| | SQL | NoSQL (Document) |
|---|---|---|
| Schema | Strict, enforced | Flexible, per document |
| Joins | Native, expressive | Application-side |
| Scaling | Possible, requires tooling | Built in from day one |
| Transactions | Full ACID | Improving (MongoDB 4.0+) |
| Data model | Normalized tables | Denormalized documents |

The lines have also blurred: PostgreSQL supports `jsonb` for flexible document storage with full SQL on top. MongoDB added ACID transactions and schema validation. Neither camp is a pure archetype anymore.

**Choose SQL** when your data is relational, your queries are complex, and you need strong consistency. **Choose a document store** when your data is naturally document-shaped, your schema evolves rapidly, and developer speed matters more than join flexibility.
