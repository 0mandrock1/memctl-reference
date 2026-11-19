"""Minimal reference state machine for revisioned agent memory.

This is a self-contained teaching implementation, NOT the production memctl
server or a wire-compatible replica. It contains no real memory records.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass


class StaleRevision(Exception):
    """Another writer has changed this fact since the caller read it."""


class IdempotencyConflict(Exception):
    """A request ID was reused with different inputs."""


@dataclass(frozen=True)
class Fact:
    id: int
    topic: str
    key: str
    value: str
    revision: int
    active: bool


class MemoryStore:
    """SQLite demonstration of optimistic revisions, replay and supersession."""

    def __init__(self, database: str = ":memory:") -> None:
        self._db = sqlite3.connect(database, isolation_level=None, timeout=15,
                                   check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._db.executescript("""
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                revision INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                previous_id INTEGER REFERENCES facts(id)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS one_active_fact
                ON facts(topic, key) WHERE active = 1;
            CREATE TABLE IF NOT EXISTS receipts (
                request_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                fact_id INTEGER NOT NULL REFERENCES facts(id)
            );
        """)

    def close(self) -> None:
        self._db.close()

    @staticmethod
    def _fact(row: sqlite3.Row) -> Fact:
        return Fact(row["id"], row["topic"], row["key"], row["value"],
                    row["revision"], bool(row["active"]))

    def _one(self, query: str, args: tuple) -> Fact | None:
        row = self._db.execute(query, args).fetchone()
        return self._fact(row) if row else None

    def get(self, topic: str, key: str) -> Fact | None:
        with self._lock:
            return self._one(
                "SELECT * FROM facts WHERE topic=? AND key=? AND active=1",
                (topic, key),
            )

    def history(self, topic: str, key: str) -> list[Fact]:
        with self._lock:
            return [self._fact(r) for r in self._db.execute(
                "SELECT * FROM facts WHERE topic=? AND key=? ORDER BY revision",
                (topic, key),
            )]

    def recall(self, topic: str, term: str) -> list[Fact]:
        """Literal substring match; '%' and '_' are never SQL wildcards."""
        escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._lock:
            return [self._fact(r) for r in self._db.execute(
                "SELECT * FROM facts WHERE topic=? AND active=1 "
                "AND (key LIKE ? ESCAPE '\\' OR value LIKE ? ESCAPE '\\') "
                "ORDER BY key",
                (topic, "%" + escaped + "%", "%" + escaped + "%"),
            )]

    def write(self, *, topic: str, key: str, value: str,
              expected_revision: int, request_id: str) -> Fact:
        """Create at revision 0, or CAS-replace one active fact.

        BEGIN IMMEDIATE serializes writers. Retried request_id returns its
        original result, even if a later revision now exists.
        """
        if not all(isinstance(v, str) and v.strip()
                   for v in (topic, key, value, request_id)):
            raise ValueError("topic, key, value and request_id must be non-empty")
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected_revision must be a nonnegative integer")
        fingerprint = hashlib.sha256(json.dumps(
            [topic, key, value, expected_revision],
            ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")).hexdigest()

        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                prior = self._db.execute(
                    "SELECT fingerprint, fact_id FROM receipts WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                if prior:
                    if prior["fingerprint"] != fingerprint:
                        raise IdempotencyConflict("request_id reused with new payload")
                    result = self._one("SELECT * FROM facts WHERE id=?",
                                       (prior["fact_id"],))
                    assert result is not None
                else:
                    current = self._one(
                        "SELECT * FROM facts WHERE topic=? AND key=? AND active=1",
                        (topic, key),
                    )
                    actual = current.revision if current else 0
                    # Zero represents absence only. Created facts start at rev 1.
                    if actual != expected_revision:
                        raise StaleRevision(
                            f"expected revision {expected_revision}, actual {actual}"
                        )
                    if current:
                        self._db.execute(
                            "UPDATE facts SET active=0 WHERE id=?", (current.id,)
                        )
                    revision = actual + 1
                    cur = self._db.execute(
                        "INSERT INTO facts(topic,key,value,revision,previous_id) "
                        "VALUES (?,?,?,?,?)",
                        (topic, key, value, revision, current.id if current else None),
                    )
                    self._db.execute(
                        "INSERT INTO receipts(request_id,fingerprint,fact_id) "
                        "VALUES (?,?,?)", (request_id, fingerprint, cur.lastrowid)
                    )
                    result = self._one("SELECT * FROM facts WHERE id=?",
                                       (cur.lastrowid,))
                    assert result is not None
                self._db.execute("COMMIT")
                return result
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def supersede(self, *, fact_id: int, value: str,
                  expected_revision: int, request_id: str) -> Fact:
        """Replace the named version only if it is still current."""
        with self._lock:
            old = self._one("SELECT * FROM facts WHERE id=?", (fact_id,))
            if old is None or not old.active or old.revision != expected_revision:
                raise StaleRevision("fact_id is no longer the active revision")
            return self.write(topic=old.topic, key=old.key, value=value,
                              expected_revision=expected_revision,
                              request_id=request_id)
