# Retry-Safe Batch Processor

A Java 17 command-line tool that reads a product CSV, processes rows concurrently, retries transient failures, skips rows completed in earlier runs, and writes permanent failures to a dead-letter file (`failed.csv`).

## Design decisions

**Idempotency via a completed-key log.** Batch jobs get rerun after partial failures, and reprocessing every row wastes work and can create duplicates. Each row is keyed by `sellerId|sku`. On success, the key is appended to `done.log`. On startup, those keys load into memory, and matching rows are skipped. Rerunning a finished file does no processing (100k rows in 0.6s).

**Buffered log writes.** The first version opened and appended to `done.log` for every row inside a `synchronized` method. That serializes the 8 threads on file writes and limits throughput. A single `BufferedWriter` opened at startup removes the per-row open/close cost. The slowest early run was caused by simulated retry sleeps, not the log; lowering the simulated failure rate fixed it.

**Retries only for transient errors.** Retrying a bad price never succeeds, so permanent errors (`price <= 0`) fail immediately. Transient errors (timeouts) retry with exponential backoff (100ms, 200ms, 400ms) up to 3 times. Retrying everything would hide bad data and waste time.

**Dead-letter output.** Rows that still fail are written to `failed.csv` with the reason, so they can be fixed and resubmitted without rerunning the whole file.

**Fixed thread pool of 8.** Bounded concurrency keeps load on downstream systems predictable. The number is a starting point, not a tuned value.

## Run

```bash
mvn -q package -DskipTests
java -cp target/classes BatchProcessor data/sample.csv
```

Output: `accepted=N skipped=N failed=N retried=N`.

To reset state: `rm -f done.log failed.csv`.

## Test

```bash
mvn -q test
```

Tests cover retry on transient errors, giving up after the max retries, no retry on permanent errors, and price validation.

## Benchmark

Codespace, 8 worker threads, 1M synthetic rows (50k sellers) generated with `awk`.

| Run | Rows | Time | Result |
|---|---|---|---|
| Fresh run | 1,000,000 | 15.4s | accepted=1000000, retried=1101 |
| Rerun (idempotency) | 100,000 | 0.6s | skipped=100000 |

About 65k rows/sec, including JVM startup.

## Limitations

- **`process()` is a stub.** It has a simulated 0.1% transient failure rate and a price check. It does not write to a real downstream system, so these numbers measure the pipeline, not production throughput.
- **At-least-once on crash.** A row is marked done only after success, but buffered `done.log` entries may be lost on a crash or `Ctrl+C`. Those rows get reprocessed on the next run. This is safe only if the real downstream write is idempotent.
- **`failed.csv` is written at the end.** An interrupted run loses the dead-letter list. Rerunning recreates it for rows not yet done.
- **Local state.** Progress is a file, not a database, so this doesn't run across multiple machines.
