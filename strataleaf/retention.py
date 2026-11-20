import math
from .codec import name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision
from .transaction import WriteBatch

DEFAULT_CHUNK = 100
MAX_CHUNK = 100000

# A version below the chunk boundary is superseded when the same document key has a
# newer version that is itself at or below that boundary.  The newest version at or
# below the boundary is therefore always kept: it is the baseline needed to build
# the read view for that boundary (and for every later revision).
_SWEEP_SUPERSEDED = """DELETE FROM versions WHERE rev<? AND EXISTS
  (SELECT 1 FROM versions newer WHERE newer.collection=versions.collection
   AND newer.key=versions.key AND newer.rev>versions.rev AND newer.rev<=?)"""


def _chunk(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_CHUNK:
        raise InvalidDocument("compaction chunk must be between 1 and 100000 revisions")
    return value


def _meta_int(store, key):
    value = store.get(key)
    return None if value == "" else int(value)


class Retention:
    def __init__(self, store):
        self.store = store

    def pin(self, pin_name, revision=None):
        name(pin_name)
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            if connection.execute("SELECT 1 FROM pins WHERE name=?", (pin_name,)).fetchone():
                raise Conflict("read pin already exists")
            connection.execute("INSERT INTO pins VALUES (?,?)", (pin_name, revision))
        return revision

    def unpin(self, pin_name):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM pins WHERE name=?", (pin_name,)).rowcount:
                raise NotFound("unknown read pin")

    # ------------------------------------------------------------------
    # Reclamation plan
    #
    # A plan records an intended target revision.  It never moves the floor by
    # itself.  The floor only advances in the same short IMMEDIATE transaction
    # that deletes the corresponding prefix, so the published floor can never
    # lead the work that has actually been committed.  Pins and consumer
    # acknowledgements are read inside that transaction and cap the next chunk.
    # ------------------------------------------------------------------

    @staticmethod
    def _blockers(connection):
        return [row[0] for row in connection.execute("SELECT rev FROM pins UNION ALL SELECT ack FROM consumers")]

    def _boundary(self, connection, target):
        return min([target] + self._blockers(connection))

    def _read_status(self, connection):
        target = _meta_int(self.store, "compaction_target")
        state = self.store.get("compaction_state")
        floor = int(self.store.get("floor"))
        boundary = None if target is None else self._boundary(connection, target)
        return {
            "target": target,
            "state": state,
            "floor": floor,
            "boundary": boundary,
            "remaining": 0 if boundary is None else max(0, boundary - floor),
            "blocked": target is not None and state != "completed" and boundary < target,
            "base_floor": int(self.store.get("compaction_base_floor")),
            "chunk": int(self.store.get("compaction_chunk")),
            "removed_versions": int(self.store.get("compaction_removed_versions")),
            "removed_commits": int(self.store.get("compaction_removed_commits")),
        }

    def status(self):
        with self.store.read() as connection:
            return self._read_status(connection)

    def plan(self, target, chunk=DEFAULT_CHUNK):
        """Persist a resumable reclamation plan without deleting anything.

        The returned ``boundary`` is only a *preview* of the safe limit: the
        published ``floor`` stays where it is until :meth:`step` actually
        commits the matching deletions.
        """
        chunk = _chunk(chunk)
        with self.store.atomic(writable=True) as connection:
            target = checked_revision(self.store, target)
            floor = int(self.store.get("floor"))
            if self._boundary(connection, target) < floor:
                raise Conflict("retention metadata violates current floor")
            self.store.set("compaction_target", target)
            self.store.set("compaction_base_floor", floor)
            self.store.set("compaction_chunk", chunk)
            self.store.set("compaction_removed_versions", 0)
            self.store.set("compaction_removed_commits", 0)
            self.store.set("compaction_state", "completed" if floor >= target else "active")
            return self._read_status(connection)

    def step(self, chunk=None):
        """Delete one bounded chunk and publish the floor it fully processed."""
        with self.store.atomic(writable=True) as connection:
            target = _meta_int(self.store, "compaction_target")
            if target is None:
                raise Conflict("no compaction plan")
            state = self.store.get("compaction_state")
            if state == "paused":
                raise Conflict("compaction plan is paused")
            floor = int(self.store.get("floor"))
            boundary = self._boundary(connection, target)
            if boundary < floor:
                raise Conflict("retention metadata violates current floor")
            size = int(self.store.get("compaction_chunk")) if chunk is None else _chunk(chunk)
            if floor >= target or state == "completed":
                self.store.set("compaction_state", "completed")
                return {"target": target, "state": "completed", "floor": floor, "boundary": boundary,
                        "from": floor, "through": floor, "advanced": 0, "removed_versions": 0,
                        "removed_commits": 0, "cumulative_versions": int(self.store.get("compaction_removed_versions")),
                        "cumulative_commits": int(self.store.get("compaction_removed_commits")),
                        "complete": True, "blocked": False}
            if boundary <= floor:
                return {"target": target, "state": "active", "floor": floor, "boundary": boundary,
                        "from": floor, "through": floor, "advanced": 0, "removed_versions": 0,
                        "removed_commits": 0, "cumulative_versions": int(self.store.get("compaction_removed_versions")),
                        "cumulative_commits": int(self.store.get("compaction_removed_commits")),
                        "complete": False, "blocked": True}
            through = min(boundary, floor + size)
            removed_versions = connection.execute(_SWEEP_SUPERSEDED, (through, through)).rowcount
            removed_commits = connection.execute("DELETE FROM commits WHERE rev<=?", (through,)).rowcount
            cumulative_versions = int(self.store.get("compaction_removed_versions")) + removed_versions
            cumulative_commits = int(self.store.get("compaction_removed_commits")) + removed_commits
            self.store.set("floor", through)
            self.store.set("compaction_removed_versions", cumulative_versions)
            self.store.set("compaction_removed_commits", cumulative_commits)
            complete = through >= target
            self.store.set("compaction_state", "completed" if complete else "active")
            return {"target": target, "state": "completed" if complete else "active", "floor": through,
                    "boundary": boundary, "from": floor, "through": through, "advanced": through - floor,
                    "removed_versions": removed_versions, "removed_commits": removed_commits,
                    "cumulative_versions": cumulative_versions, "cumulative_commits": cumulative_commits,
                    "complete": complete, "blocked": not complete and through >= boundary}

    def run(self, chunk=None, max_steps=None):
        """Advance an existing plan until it completes, blocks or hits ``max_steps``."""
        if max_steps is not None and (isinstance(max_steps, bool) or not isinstance(max_steps, int) or max_steps < 1):
            raise InvalidDocument("max_steps must be a positive integer")
        steps = 0
        while True:
            progress = self.step(chunk)
            steps += 1
            if progress["complete"] or progress["blocked"] or progress["advanced"] == 0:
                break
            if max_steps is not None and steps >= max_steps:
                break
        return {**progress, "steps": steps}

    def pause(self):
        with self.store.atomic(writable=True) as connection:
            if self.store.get("compaction_state") != "active":
                raise Conflict("no active compaction plan")
            self.store.set("compaction_state", "paused")
            return self._read_status(connection)

    def resume(self):
        with self.store.atomic(writable=True) as connection:
            if self.store.get("compaction_state") != "paused":
                raise Conflict("compaction plan is not paused")
            self.store.set("compaction_state", "active")
            return self._read_status(connection)

    def cancel(self):
        with self.store.atomic(writable=True) as connection:
            if _meta_int(self.store, "compaction_target") is None:
                raise Conflict("no compaction plan")
            self.store.set("compaction_target", "")
            self.store.set("compaction_state", "idle")
            self.store.set("compaction_base_floor", 0)
            self.store.set("compaction_removed_versions", 0)
            self.store.set("compaction_removed_commits", 0)
            return self._read_status(connection)

    def compact(self, target, chunk=DEFAULT_CHUNK):
        """Reclaim up to the safe boundary, one bounded chunk per write transaction.

        Kept backwards compatible with the original one-shot call: the return
        keys and their meaning are unchanged, but the writer lock is released
        between chunks.  A caller that stops early can reopen the database and
        continue through the persisted plan.
        """
        chunk = _chunk(chunk)
        self.plan(target, chunk)
        result = self.run(chunk)
        return {"requested": target, "floor": result["floor"],
                "removed_versions": result["cumulative_versions"],
                "removed_commits": result["cumulative_commits"],
                "blocked": result["floor"] < target}

    def sweep(self, now, limit=1000):
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise InvalidDocument("expiry sweep requires a finite timestamp")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise InvalidDocument("sweep limit must be between 1 and 10000")
        with self.store.read() as connection:
            rows = connection.execute("""SELECT v.* FROM versions v WHERE v.body IS NOT NULL AND v.expires<=?
              AND NOT EXISTS (SELECT 1 FROM versions n WHERE n.collection=v.collection AND n.key=v.key AND n.rev>v.rev)
              ORDER BY v.expires,v.collection,v.key LIMIT ?""", (now, limit)).fetchall()
        if not rows:
            return {"expired": 0, "revision": None}
        batch = WriteBatch(self.store)
        for row in rows:
            batch.delete(row["collection"], row["key"], expected=row["rev"])
        result = batch.commit()
        return {"expired": result.count, "revision": result.revision}
