import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from reference.memory_store import (
    IdempotencyConflict, MemoryStore, StaleRevision,
)


class MemoryStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()

    def tearDown(self):
        self.store.close()

    def test_create_and_read(self):
        fact = self.store.write(topic="work", key="stack", value="PHP/Postgres",
                                expected_revision=0, request_id="r1")
        self.assertEqual(fact.revision, 1)
        self.assertEqual(self.store.get("work", "stack"), fact)

    def test_stale_revision_rejected_without_side_effect(self):
        self.store.write(topic="work", key="stack", value="old",
                         expected_revision=0, request_id="r1")
        with self.assertRaises(StaleRevision):
            self.store.write(topic="work", key="stack", value="unwanted",
                             expected_revision=0, request_id="r2")
        self.assertEqual(self.store.get("work", "stack").value, "old")

    def test_idempotent_replay_returns_original_revision(self):
        first = self.store.write(topic="work", key="stack", value="old",
                                 expected_revision=0, request_id="r1")
        self.store.write(topic="work", key="stack", value="new",
                         expected_revision=1, request_id="r2")
        repeated = self.store.write(topic="work", key="stack", value="old",
                                    expected_revision=0, request_id="r1")
        self.assertEqual(repeated.id, first.id)
        self.assertFalse(repeated.active)
        self.assertEqual(len(self.store.history("work", "stack")), 2)

    def test_request_id_payload_mismatch_rejected(self):
        self.store.write(topic="a", key="x", value="v",
                         expected_revision=0, request_id="repeat")
        with self.assertRaises(IdempotencyConflict):
            self.store.write(topic="a", key="x", value="different",
                             expected_revision=0, request_id="repeat")

    def test_supersession_preserves_history(self):
        old = self.store.write(topic="a", key="x", value="draft",
                               expected_revision=0, request_id="r1")
        new = self.store.supersede(fact_id=old.id, value="verified",
                                   expected_revision=1, request_id="r2")
        self.assertEqual([f.revision for f in self.store.history("a", "x")], [1, 2])
        self.assertEqual(new.revision, 2)
        with self.assertRaises(StaleRevision):
            self.store.supersede(fact_id=old.id, value="stale",
                                 expected_revision=1, request_id="r3")

    def test_supersede_retry_is_idempotent(self):
        first = self.store.write(topic="a", key="x", value="v1",
                                 expected_revision=0, request_id="r1")
        second = self.store.supersede(fact_id=first.id, value="v2",
                                      expected_revision=1, request_id="r2")
        retry = self.store.supersede(fact_id=first.id, value="v2",
                                     expected_revision=1, request_id="r2")
        self.assertEqual(retry.id, second.id)
        self.assertEqual(len(self.store.history("a", "x")), 2)
        with self.assertRaises(IdempotencyConflict):
            self.store.supersede(fact_id=first.id, value="different",
                                 expected_revision=1, request_id="r2")

    def test_topic_scope_and_literal_search(self):
        self.store.write(topic="a", key="user%key", value="100% coverage",
                         expected_revision=0, request_id="r1")
        self.store.write(topic="b", key="user%key", value="secret",
                         expected_revision=0, request_id="r2")
        self.assertEqual(len(self.store.recall("a", "%")), 1)
        self.assertEqual(len(self.store.recall("a", "_")), 0)
        self.assertEqual(len(self.store.recall("b", "coverage")), 0)

    def test_sql_input_is_data_not_sql(self):
        key = "x'); DROP TABLE facts; --"
        self.store.write(topic="a", key=key, value="safe",
                         expected_revision=0, request_id="r1")
        self.assertEqual(self.store.get("a", key).value, "safe")

    def test_concurrent_cas_only_one_writer_wins(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "memory.sqlite3")
            a, b = MemoryStore(path), MemoryStore(path)
            try:
                a.write(topic="a", key="x", value="old",
                        expected_revision=0, request_id="init")
                barrier = Barrier(2)

                def update(store, value):
                    barrier.wait(timeout=5)
                    try:
                        return store.write(topic="a", key="x", value=value,
                                           expected_revision=1,
                                           request_id="write-" + value)
                    except StaleRevision:
                        return "stale"

                with ThreadPoolExecutor(max_workers=2) as pool:
                    f1, f2 = pool.submit(update, a, "one"), pool.submit(update, b, "two")
                    results = [f1.result(), f2.result()]
                self.assertEqual(sum(r == "stale" for r in results), 1)
                self.assertEqual(a.get("a", "x").revision, 2)
            finally:
                a.close()
                b.close()


if __name__ == "__main__":
    unittest.main()
