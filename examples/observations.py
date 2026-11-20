"""Reproduce operational boundaries through real temporary database operations."""
import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from strataleaf import Database, SnapshotTooOld
from .workflows import rejected, seed


def contention(folder):
    with Database(folder / 'observe.db', timeout=0.03) as db:
        seed(db)
        holder = sqlite3.connect(folder / 'observe.db', isolation_level=None)
        try:
            holder.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            try:
                db.batch().delete('segments', 'seg_00').commit()
            except sqlite3.OperationalError as exc:
                blocked = {'exception': type(exc).__name__, 'message': str(exc),
                           'elapsed_ms': round((time.monotonic() - started) * 1000, 2)}
            else:
                raise RuntimeError('expected SQLite writer contention was not observed')
        finally:
            holder.rollback()
            holder.close()
        result = db.batch().delete('segments', 'seg_00').commit()
        return {'blocked_write': blocked, 'after_release': result.__dict__, 'inspection': db.diagnostics.inspect(True)}


def retention_pressure(folder):
    with Database(folder / 'observe.db') as db:
        first = seed(db)
        db.retention.pin('long_reader', first)
        db.feed.register('offline_replica', first)
        for value in range(3):
            db.batch().put('segments', 'seg_00', {'region': 'east', 'sequence': value + 10}).commit()
        db.retention.plan(db.revision, chunk=1)
        planned = db.retention.status()
        both = db.retention.run()
        pin_floor = [row.key for row in db.query('segments', limit=100, revision=both['floor']).records]
        db.retention.unpin('long_reader')
        one = db.retention.run()
        consumer_floor = [row.key for row in db.query('segments', limit=100, revision=one['floor']).records]
        db.feed.acknowledge('offline_replica', db.revision)
        none = db.retention.run()
        final_floor = [row.key for row in db.query('segments', limit=100, revision=none['floor']).records]
        # Each published floor was read complete while it was current; once the
        # floor advances, anything below it is refused.
        stale = rejected(lambda: db.query('segments', revision=both['floor'] - 1), SnapshotTooOld) \
            if both['floor'] > 0 else None
        return {'planned': planned, 'both_holds': both, 'one_hold': one, 'released': none,
                'readable_at_floor': {'pin': pin_floor, 'consumer': consumer_floor, 'final': final_floor},
                'stale_read': stale, 'inspection': db.diagnostics.inspect(True)}


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='strataleaf-observation-') as directory:
        operation = {'contention': contention, 'retention_pressure': retention_pressure}[sys.argv[1]]
        print(json.dumps(operation(Path(directory)), ensure_ascii=False, sort_keys=True))
