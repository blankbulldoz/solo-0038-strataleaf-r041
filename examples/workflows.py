import json
import sys
import tempfile
from pathlib import Path
from strataleaf import Database, Archives, Conflict, InvalidDocument, InvalidCursor, ReadOnly, SnapshotTooOld, ReplicationError

SCHEMA = {"fields": {"region": "string", "sequence": "integer", "code": "string"}, "required": ["region", "sequence"]}


def seed(db):
    db.catalog.create("segments", SCHEMA)
    batch = db.batch("initial_segments")
    for index in range(6):
        batch.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
    return batch.commit().revision


def rejected(call, error):
    try:
        call()
    except error as exc:
        return {"error": type(exc).__name__, "message": str(exc)}
    raise RuntimeError("operation unexpectedly succeeded")


def read_view(db, revision):
    return [(row.key, row.revision, row.document)
            for row in db.query("segments", limit=1000, revision=revision).records]


def audit_reclamation(db, model, floor):
    """Prove every externally readable revision is still complete.

    ``model`` is the reference read view captured for each committed revision
    before that revision could be reclaimed.  The audit checks, for every
    revision still promised by ``floor``, that the full page and each point
    read match that reference, that ``floor - 1`` is refused, and that no
    retention blocker sits below the published floor.
    """
    head = db.revision
    status = db.retention.status()
    if status["floor"] != floor:
        raise RuntimeError("published floor differs from the processed position")
    if status["boundary"] is not None and status["boundary"] < status["floor"]:
        raise RuntimeError("floor advanced past a live retention blocker")
    for revision in range(floor, head + 1):
        expected = model[revision]
        if read_view(db, revision) != expected:
            raise RuntimeError(f"readable revision {revision} changed during reclamation")
        for key, version, document in expected:
            point = db.get("segments", key, revision)
            if point is None or (point.revision, point.document) != (version, document):
                raise RuntimeError(f"point read differs at revision {revision}")
    if floor > 0:
        try:
            read_view(db, floor - 1)
        except SnapshotTooOld:
            pass
        else:
            raise RuntimeError("reclaimed revision remained readable")
    if not db.diagnostics.inspect()["ok"]:
        raise RuntimeError("inspection failed during reclamation")
    return {"floor": floor, "head": head, "readable_revisions": [floor, head]}


def compact_segments(db, folder):
    """Drive a reclamation plan in chunks across writes, pause, reopen and consumers."""
    path = folder / "source.db"
    first = db.revision
    model = {0: [], first: read_view(db, first)}
    db.retention.pin("reader", first)
    db.feed.register("slow_copy", first)
    for sequence in (8, 9, 10):
        written = db.batch().put("segments", "seg_00", {"region": "north", "sequence": sequence}).commit()
        model[written.revision] = read_view(db, written.revision)
    target = db.revision

    # A plan only records intent: it publishes no floor and deletes nothing.
    planned = db.retention.plan(target, chunk=1)
    if planned["floor"] != 0 or planned["removed_versions"] or planned["removed_commits"]:
        raise RuntimeError("planning moved the floor or deleted data")
    if planned["target"] != target or planned["boundary"] != first:
        raise RuntimeError("plan did not preview the pin-bounded boundary")

    # The first chunk deletes and publishes its floor inside one short write
    # transaction, so no reader can observe a floor without its baseline.
    first_step = db.retention.step()
    if first_step["floor"] != first_step["through"] or first_step["advanced"] <= 0:
        raise RuntimeError("step published a floor it had not processed")
    first_audit = audit_reclamation(db, model, first_step["floor"])

    # Pause, close the handle, reopen the file: the plan survives.
    paused = db.retention.pause()
    db.close()
    db = Database(path)
    reopened = db.retention.status()
    if reopened["state"] != "paused" or reopened["target"] != target or reopened["floor"] != first_step["floor"]:
        raise RuntimeError("compaction plan did not survive reopen")
    db.retention.resume()

    # The pin at the original revision still caps the next chunk.
    blocked = db.retention.step()
    if blocked["advanced"] != 0 or not blocked["blocked"] or blocked["floor"] != first_step["floor"]:
        raise RuntimeError("retention blocker did not stop the floor")
    blocked_audit = audit_reclamation(db, model, first_step["floor"])

    # Writes keep committing while reclamation is blocked; each new revision is
    # immediately readable and remains outside the planned target.
    for sequence in (11, 12):
        written = db.batch().put("segments", "seg_01", {"region": "south", "sequence": sequence}).commit()
        model[written.revision] = read_view(db, written.revision)
    writes_audit = audit_reclamation(db, model, first_step["floor"])

    # A new pin or consumer cannot reach behind the published floor to protect
    # data that has already been deleted.
    late_pin = rejected(lambda: db.retention.pin("late_reader", 0), SnapshotTooOld)
    late_consumer = rejected(lambda: db.feed.register("late_copy", 0), SnapshotTooOld)

    # Consumer progress releases the boundary; each step publishes exactly the
    # prefix it deleted in that transaction.
    db.retention.unpin("reader")
    db.feed.acknowledge("slow_copy", 3)
    consumer_step = db.retention.step(chunk=2)
    consumer_audit = audit_reclamation(db, model, consumer_step["floor"])
    db.feed.acknowledge("slow_copy", target)
    final_step = db.retention.step(chunk=2)
    if final_step["state"] != "completed" or final_step["floor"] != target:
        raise RuntimeError("plan did not complete at its target")
    completed_audit = audit_reclamation(db, model, final_step["floor"])
    if completed_audit["head"] <= target:
        raise RuntimeError("continuous writes did not survive reclamation")

    # The legacy one-shot call keeps its exact result shape.  It starts from the
    # current consumer position and still refuses to move the floor past it.
    head = db.revision
    legacy_blocked = db.retention.compact(head)
    if set(legacy_blocked) != {"requested", "floor", "removed_versions", "removed_commits", "blocked"}:
        raise RuntimeError("one-shot compact result shape changed")
    if not legacy_blocked["blocked"] or legacy_blocked["floor"] != target:
        raise RuntimeError("one-shot compact ignored the consumer position")
    db.feed.acknowledge("slow_copy", head)
    legacy = db.retention.compact(head)
    if legacy["blocked"] or legacy["floor"] != head:
        raise RuntimeError("one-shot compact did not advance to head")
    final_audit = audit_reclamation(db, model, head)
    status = db.retention.status()
    db.close()
    return {"plan": planned, "first_step": first_step, "first_audit": first_audit,
            "paused": paused["state"], "reopened": reopened, "blocked_step": blocked, "blocked_audit": blocked_audit,
            "writes_while_blocked": writes_audit, "late_pin": late_pin, "late_consumer": late_consumer,
            "consumer_step": consumer_step, "consumer_audit": consumer_audit, "final_step": final_step,
            "completed_audit": completed_audit, "status": status, "legacy_blocked": legacy_blocked,
            "legacy": legacy, "final_audit": final_audit}



def run(flow):
    with tempfile.TemporaryDirectory(prefix="strataleaf-example-") as directory:
        folder = Path(directory)
        with Database(folder / "source.db") as db:
            first = seed(db)
            if flow == "catalog":
                output = {"collections": db.catalog.list(), "duplicate": rejected(lambda: db.catalog.create("segments"), Conflict)}
            elif flow == "schema":
                updated = {**SCHEMA, "fields": {**SCHEMA["fields"], "note": "string"}}
                generation = db.catalog.evolve("segments", updated, 1)
                failure = rejected(lambda: db.catalog.evolve("segments", {**updated, "required": ["note"]}, generation), InvalidDocument)
                output = {"generation": generation, "after_rejection": db.catalog.describe("segments"), "incompatible": failure}
            elif flow == "commit":
                db.catalog.create("marks")
                result = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                replay = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                conflict = rejected(lambda: db.batch().put("marks", "discard", {}).put("segments", "seg_01", {"region": "x", "sequence": 1}, expected=0).commit(), Conflict)
                if db.get("marks", "discard") is not None or not replay.replayed:
                    raise RuntimeError("atomicity or replay observation failed")
                output = {"commit": result.__dict__, "replay": replay.__dict__, "conflict": conflict}
            elif flow == "snapshot":
                db.batch().delete("segments", "seg_00", expected=first).commit()
                output = {"past": db.get("segments", "seg_00", first).__dict__, "present": db.get("segments", "seg_00"),
                          "future": rejected(lambda: db.get("segments", "seg_00", db.revision + 1), InvalidDocument)}
            elif flow == "query":
                query = {"field": "sequence", "op": "ge", "value": 0}
                page = db.query("segments", query, limit=2)
                db.batch().delete("segments", "seg_02").put("segments", "seg_07", {"region": "west", "sequence": 7}).commit()
                keys = [row.key for row in page.records]
                cursor = page.cursor
                while cursor:
                    next_page = db.query("segments", query, limit=2, cursor=cursor)
                    keys.extend(row.key for row in next_page.records)
                    cursor = next_page.cursor
                if keys != [f"seg_{i:02}" for i in range(6)]:
                    raise RuntimeError("read revision changed during pagination")
                output = {"revision": page.revision, "keys": keys, "foreign_query": rejected(lambda: db.query("segments", cursor=page.cursor), InvalidCursor)}
            elif flow == "index":
                db.indexes.create("segments", "by_region", "region")
                db.indexes.create("segments", "unique_code", "code", unique=True)
                predicate = {"field": "region", "op": "eq", "value": "east"}
                indexed = db.query("segments", predicate)
                scanned = db.query("segments", predicate, force_scan=True)
                if indexed.records != scanned.records:
                    raise RuntimeError("index and scan differ")
                collision = rejected(lambda: db.batch().put("segments", "new", {"region": "north", "sequence": 8, "code": "c0"}).commit(), Conflict)
                output = {"plan": indexed.plan, "keys": [row.key for row in indexed.records], "collision": collision, "integrity": db.diagnostics.inspect(True)}
            elif flow == "feed":
                db.feed.register("mirror", 0)
                db.batch().delete("segments", "seg_00").commit()
                commits = db.feed.read(0, limit=1)
                db.feed.acknowledge("mirror", commits[-1]["revision"])
                output = {"complete_commit": commits, "ack": db.feed.position("mirror"),
                          "backward": rejected(lambda: db.feed.acknowledge("mirror", 0), Conflict)}
            elif flow == "backup":
                backup = db.archives.backup(folder / "backup.db")
                db.batch().delete("segments", "seg_00").commit()
                bad = rejected(lambda: Archives.restore(folder / "backup.db", folder / "invalid.db", "wrong"), Exception)
                Archives.restore(folder / "backup.db", folder / "restored.db", backup["sha256"])
                with Database(folder / "restored.db") as restored:
                    output = {"backup": backup, "restored": restored.diagnostics.fingerprint(), "bad_checksum": bad}
                    if restored.revision != first:
                        raise RuntimeError("backup read boundary changed")
            elif flow == "replica":
                db.archives.backup(folder / "base.db")
                Archives.restore(folder / "base.db", folder / "replica.db", follower=True)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 19}).commit()
                db.batch().delete("segments", "seg_01").commit()
                db.indexes.create("segments", "by_region", "region")
                with Database(folder / "replica.db") as replica:
                    part = db.replication.export(replica.revision, limit=1)
                    applied = replica.replication.apply(part)
                    tail = db.replication.export(replica.revision)
                    replica.replication.apply(tail)
                    repeat = replica.replication.apply(tail)
                    readonly = rejected(lambda: replica.batch().delete("segments", "seg_00").commit(), ReadOnly)
                    if replica.diagnostics.fingerprint() != db.diagnostics.fingerprint() or not replica.diagnostics.inspect(True)["ok"]:
                        raise RuntimeError("replica differs from source")
                    output = {"first_packet": applied, "repeat": repeat, "readonly": readonly, "fingerprint": replica.diagnostics.fingerprint()}
            elif flow == "compact":
                output = compact_segments(db, folder)
            elif flow == "expire":
                written = db.batch().put("segments", "timed", {"region": "north", "sequence": 10}, expires_at=100).commit()
                before = db.get("segments", "timed")
                swept = db.retention.sweep(100)
                if db.get("segments", "timed") is not None or db.get("segments", "timed", written.revision) is None:
                    raise RuntimeError("expiry did not preserve historical visibility")
                output = {"before": before.__dict__, "sweep": swept, "feed": db.feed.read(written.revision), "idle": db.retention.sweep(100)}
            elif flow == "aggregate":
                output = db.aggregates.group("segments", "region", "sequence", revision=first)
                if sum(item["count"] for item in output["groups"]) != 6:
                    raise RuntimeError("aggregation lost visible documents")
                output["invalid"] = rejected(lambda: db.aggregates.group("unknown", "region"), Exception)
            elif flow == "patch":
                result = db.patch("segments", "seg_00", {"code": None, "detail": {"block": 4}}, expected=first)
                output = {"patch": result.__dict__, "document": db.get("segments", "seg_00").document,
                          "stale": rejected(lambda: db.patch("segments", "seg_00", {"sequence": 8}, expected=first), Conflict)}
            elif flow == "inspect":
                db.indexes.create("segments", "by_code", "code")
                output = {"inspection": db.diagnostics.inspect(deep=True), "fingerprint": db.diagnostics.fingerprint(), "checkpoint": db.checkpoint()}
                if not output["inspection"]["ok"]:
                    raise RuntimeError("integrity inspection failed")
            elif flow == "transfer":
                exported = db.transfer.export_lines("segments")
                db.catalog.create("copy_segments", SCHEMA)
                imported = db.transfer.import_lines("copy_segments", exported["lines"])
                output = {"revision": exported["revision"], "imported": imported.__dict__,
                          "duplicate": rejected(lambda: db.transfer.import_lines("copy_segments", exported["lines"]), Conflict)}
                if len(db.query("copy_segments").records) != 6:
                    raise RuntimeError("logical import differs")
            else:
                raise ValueError("unknown example workflow: " + flow)
            print(json.dumps({"workflow": flow, "result": output}, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "commit")
