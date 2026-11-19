# memctl-reference

**A runnable, sanitized explanation of agent memory semantics.** This is a
reference model for the architecture behind [memctl](https://mandrock.me/cv/cases/memctl.html),
not a mirror of its production implementation.

The production system uses a Go CLI, HTTPS synchronization, a Python service,
PostgreSQL as its canonical memory store, and server-side SQLite sessions.
The operational backend is intentionally private: it includes deployment
details and privileged interfaces. This repository contains **no personal
memory data, credentials, endpoints, binaries or infrastructure configuration**.

## Why it exists

An agent can finish a session with a confident but stale fact; a second
agent can overwrite a correction; network retries can duplicate a write.
Treating a conversation transcript as a database cannot resolve these
failure modes. Useful memory needs explicit identity, revisions and a
defined conflict policy.

This repository isolates three mechanisms into ~one file:

1. **Optimistic revision checks:** each update names the revision it read.
   A stale writer fails instead of silently clobbering a newer fact.
2. **Idempotent mutation receipts:** a replay of the same request ID and
   payload returns the original result. Reusing the ID with different
   input is rejected.
3. **Supersession with audit history:** a correction creates a new active
   version; the earlier version remains inspectable but is not recalled.

## Architecture boundary

```text
Production (private)                      Public demonstration (this repo)
┌──────────────────────┐                  ┌───────────────────────────────┐
│ Cross-platform CLI   │                  │ MemoryStore (SQLite)          │
│ Windows / mac / Linux│                  │ topic + key + revision        │
└──────────┬───────────┘                  │ CAS + idempotency receipts    │
           │ HTTPS                        │ supersede + history           │
┌──────────▼───────────┐                  └───────────────────────────────┘
│ Sync API / sessions  │
└──────────┬───────────┘
           │ canonical DB writes
┌──────────▼───────────┐
│ PostgreSQL mem.*     │
└──────────────────────┘
```

This small model is **not wire-compatible** with the production API and does
not reproduce its authentication, retrieval ranking, schema, or authorization.
It illustrates the underlying failure modes and a testable mitigation.

## Run without dependencies

Requires Python 3.11+.

```bash
python3 -m unittest discover -s tests -v
```

```python
from reference.memory_store import MemoryStore, StaleRevision

store = MemoryStore()
first = store.write(topic="project", key="status", value="draft",
                    expected_revision=0, request_id="request-1")
latest = store.supersede(fact_id=first.id, value="verified",
                         expected_revision=1, request_id="request-2")
print(latest.revision)      # 2
print(store.history("project", "status"))  # both versions
store.close()
```

## Failure-mode matrix

| Failure | Control | Covered by |
| --- | --- | --- |
| Two agents overwrite the same fact | CAS on expected revision | concurrent-writer test |
| HTTP client retries a completed write | request ID receipt | replay test |
| Retry ID accidentally reused for another payload | fingerprint comparison | mismatch test |
| Correction erases audit history | supersession, not in-place mutation | history test |
| Search crosses topics | explicit topic in every query | topic-scope test |
| SQL metacharacters accidentally become queries | bound parameters + escaped LIKE | literal-search test |

## Trade-offs, not magic

- This is a single-node SQLite model, not a distributed consensus system.
- A request ID protects against duplicate *application mutations*, but the
  caller must persist or reconstruct the same ID for a retry to be safe.
- Conflict checks protect cooperative clients, **not** a compromised database
  owner or a caller authorized to write malicious facts.
- For production, account-level authorization, token revocation, retention,
  observability, migrations, and backup/restore all need separate controls.
- Revision 0 is the expected state for a nonexistent key; first write creates
  revision 1. IDs identify stored versions, not logical facts.
- The original application has additional operations and semantics. Do not
  infer 1:1 feature equivalence from this example.

## Related work

- [memctl case study](https://mandrock.me/cv/cases/memctl.html)
- [Multi-tenant RAG case study](https://mandrock.me/cv/cases/rag-tenant.html)

MIT licensed. This reference intentionally carries no production source files.
