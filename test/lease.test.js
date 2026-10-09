'use strict';

/**
 * Pins the properties the whole guarantee rests on: the claim is a single
 * conditional upsert against the database's own clock, a losing replica skips
 * silently, an unreadable result is never mistaken for a win, and every failure
 * path leaves the lease held rather than freeing it for an immediate re-run.
 *
 * These drive a stand-in pool. The claim's actual concurrency semantics are
 * proven against a real Postgres in lease.integration.test.js — a regex over a
 * SQL string cannot tell you whether the SQL works.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  withCronLease, LEASE, CRON_LEASES_TABLE_SQL, createTableSql, quoteTable,
} = require('..');

/**
 * A stand-in `pg` pool that records the SQL it was handed. Each winning claim
 * returns a fresh `claim_token`, as the database's `claimed_at` would.
 */
function fakeDb({ rowCount = 1, throws = null, result } = {}) {
  const calls = [];
  let claims = 0;
  return {
    calls,
    async query(sql, params) {
      calls.push({ sql, params });
      if (throws) throw throws;
      if (result !== undefined) return result;
      claims += 1;
      return { rowCount, rows: rowCount ? [{ job_name: params[0], claim_token: `17600000000000${claims}` }] : [] };
    },
  };
}

function counter() {
  const fn = async (ctx) => { fn.count += 1; fn.ctx = ctx; return 'done'; };
  fn.count = 0;
  return fn;
}


describe('withCronLease — winning the lease', () => {
  it('runs the tick and reports the result', async () => {
    const tick = counter();
    const out = await withCronLease(fakeDb({ rowCount: 1 }), 'billing:daily', LEASE.DAILY, tick);

    assert.deepEqual(out, { ran: true, result: 'done' });
    assert.equal(tick.count, 1);
  });

  it('claims with a conditional upsert, not a blind write', async () => {
    const db = fakeDb({ rowCount: 1 });
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, counter());
    const { sql, params } = db.calls[0];

    // The WHERE on the DO UPDATE is the whole mutual exclusion: without it
    // every replica would overwrite the lease and all of them would run.
    assert.match(sql, /ON CONFLICT \(job_name\) DO UPDATE/);
    assert.match(sql, /WHERE "cron_leases"\.lease_until <= clock_timestamp\(\)/);
    assert.match(sql, /RETURNING job_name/);
    assert.equal(params[0], 'billing:daily');
    assert.equal(params[1], LEASE.HOURLY);
  });

  it('reads the clock from clock_timestamp(), never NOW()', async () => {
    // NOW() is transaction_timestamp(), frozen at the start of the surrounding
    // transaction. With a transaction handle it makes an expired lease read as
    // held and a fresh claim expire early.
    const db = fakeDb({ rowCount: 1 });
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, counter());

    assert.doesNotMatch(db.calls[0].sql, /\bNOW\(\)/);
    assert.equal((db.calls[0].sql.match(/clock_timestamp\(\)/g) || []).length, 4);
  });

  it('stamps the holder name so you can see who claimed the job', async () => {
    const db = fakeDb({ rowCount: 1 });
    const tick = counter();
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, tick, { holder: 'worker-7' });
    assert.equal(db.calls[0].params[2], 'worker-7');
    assert.equal(tick.ctx.holder, 'worker-7', 'ctx.holder is what was written');
  });

  it('returns the claim\'s timestamp as its fencing token', async () => {
    const db = fakeDb({ rowCount: 1 });
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, counter());
    assert.match(db.calls[0].sql, /RETURNING job_name, \(extract\(epoch FROM claimed_at\) \* 1000000\)::bigint::text AS claim_token/);
  });

  it('accepts a driver that returns rows but no rowCount', async () => {
    const db = fakeDb({ result: { rows: [{ job_name: 'billing:daily', claim_token: '1' }] } });
    const out = await withCronLease(db, 'billing:daily', LEASE.HOURLY, counter());
    assert.equal(out.ran, true);
  });
});

describe('withCronLease — losing the lease', () => {
  it('skips the tick when another replica holds it', async () => {
    const tick = counter();
    const out = await withCronLease(fakeDb({ rowCount: 0 }), 'billing:daily', LEASE.HOURLY, tick);

    assert.deepEqual(out, { ran: false, result: null }, 'same key set on both paths');
    assert.equal(tick.count, 0, 'a losing replica must not run the tick');
  });

  it('treats an empty RETURNING as a loss even without a rowCount', async () => {
    const db = fakeDb({ result: { rows: [] } });
    const tick = counter();

    assert.equal((await withCronLease(db, 'billing:daily', LEASE.HOURLY, tick)).ran, false);
    assert.equal(tick.count, 0);
  });

  it('logs the skip at debug, not as an error', async () => {
    const lines = [];
    await withCronLease(fakeDb({ rowCount: 0 }), 'billing:daily', LEASE.HOURLY, counter(), {
      logger: { debug: (m) => lines.push(m) },
    });
    assert.equal(lines.length, 1);
    assert.match(lines[0], /lease held by another replica/);
  });

  it('tolerates a logger without debug()', async () => {
    await assert.doesNotReject(
      withCronLease(fakeDb({ rowCount: 0 }), 'billing:daily', LEASE.HOURLY, counter(), { logger: {} }),
    );
  });
});

describe('withCronLease — an unreadable result is never a win', () => {
  it('throws rather than running the tick in every replica', async () => {
    // Reading rowCount alone and treating anything non-zero as a win means an
    // unrecognised driver result runs the job everywhere — the exact failure
    // this package exists to prevent.
    for (const result of [{}, { rows: 'nope' }, null, 'ok']) {
      const tick = counter();
      await assert.rejects(
        withCronLease(fakeDb({ result }), 'billing:daily', LEASE.HOURLY, tick),
        /cannot tell whether the lease/,
      );
      assert.equal(tick.count, 0, `must not run for result ${JSON.stringify(result)}`);
    }
  });
});

describe('withCronLease — failures leave the lease held', () => {
  it('propagates a claim failure and does not run the tick', async () => {
    const db = fakeDb({ throws: new Error('db down') });
    const tick = counter();

    await assert.rejects(withCronLease(db, 'billing:daily', LEASE.HOURLY, tick), /db down/);
    assert.equal(tick.count, 0);
  });

  it('propagates a tick failure without releasing early', async () => {
    const db = fakeDb({ rowCount: 1 });

    await assert.rejects(
      withCronLease(db, 'billing:daily', LEASE.HOURLY, async () => {
        throw new Error('tick exploded');
      }),
      /tick exploded/,
    );

    // Exactly one statement: the claim. No DELETE, no UPDATE releasing the
    // lease — that is what stops the other replica re-running a half-done job.
    assert.equal(db.calls.length, 1);
  });
});

describe('withCronLease — argument validation', () => {
  it('rejects a missing or non-string jobName', async () => {
    const db = fakeDb();
    await assert.rejects(withCronLease(db, '', LEASE.HOURLY, counter()), /jobName/);
    await assert.rejects(withCronLease(db, null, LEASE.HOURLY, counter()), /jobName/);
  });

  it('rejects a non-positive or non-finite leaseMs', async () => {
    const db = fakeDb();
    await assert.rejects(withCronLease(db, 'a:b', 0, counter()), /leaseMs/);
    await assert.rejects(withCronLease(db, 'a:b', -1, counter()), /leaseMs/);
    await assert.rejects(withCronLease(db, 'a:b', NaN, counter()), /leaseMs/);
  });

  it('rejects a leaseMs that rounds to zero milliseconds', async () => {
    // 0.4 is "a positive number", but as a lease it is born expired — and a
    // born-expired lease is claimed by every replica in turn.
    await assert.rejects(withCronLease(fakeDb(), 'a:b', 0.4, counter()), /leaseMs/);
  });

  it('rejects a db that cannot query', async () => {
    await assert.rejects(withCronLease(null, 'a:b', LEASE.HOURLY, counter()), /db must be/);
    await assert.rejects(withCronLease({}, 'a:b', LEASE.HOURLY, counter()), /db must be/);
  });

  it('validates every argument BEFORE taking the lease', async () => {
    // A claim taken and then abandoned to a TypeError holds the lease for its
    // full duration: a wiring typo would silently disable the job fleet-wide.
    for (const bad of [undefined, null, 'notAFunction', 42]) {
      const db = fakeDb({ rowCount: 1 });
      await assert.rejects(withCronLease(db, 'a:b', LEASE.HOURLY, bad), /runTick/);
      assert.equal(db.calls.length, 0, 'no lease may be taken on a bad argument');
    }
  });
});

describe('withCronLease — renew', () => {
  it('hands the tick a way to extend its own lease', async () => {
    const db = fakeDb({ rowCount: 1 });
    let renewed;
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, async ({ renew, jobName, holder }) => {
      assert.equal(jobName, 'billing:daily');
      assert.equal(typeof holder, 'string');
      renewed = await renew();
    });

    assert.equal(renewed, true);
    const { sql, params } = db.calls[1];
    // Guarded by the claim AND expiry, so a lease that was already re-claimed
    // cannot be extended out from under its new owner.
    assert.match(sql, /^\s*UPDATE "cron_leases"/);
    assert.match(sql, /AND \(extract\(epoch FROM claimed_at\) \* 1000000\)::bigint::text = \$3/);
    assert.match(sql, /AND lease_until > clock_timestamp\(\)/);
    assert.equal(params[1], LEASE.HOURLY);
    assert.equal(params[2], '176000000000001', 'renew fences on its own claim\'s token');
  });

  it('fences each tick on its own claim, never on the holder name', async () => {
    // Two claims by one process share a holder name; the older tick's renew()
    // must not be able to extend the newer claim's lease.
    const db = fakeDb({ rowCount: 1 });
    const tick = async ({ renew }) => { await renew(); };
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, tick, { holder: 'worker-7' });
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, tick, { holder: 'worker-7' });
    const fences = db.calls.filter((c) => /^\s*UPDATE/.test(c.sql)).map((c) => c.params[2]);
    assert.equal(fences.length, 2);
    assert.notEqual(fences[0], fences[1]);
  });

  it('refuses to renew when the driver returned no rows to fence on', async () => {
    // A rowCount alone proves the claim won, so the tick runs — but without
    // the token, renew could extend a claim that is not this tick's.
    const db = fakeDb({ result: { rowCount: 1 } });
    let ran = false;
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, async ({ renew }) => {
      ran = true;
      await assert.rejects(renew(), /cannot tell which claim/);
    });
    assert.equal(ran, true);
    assert.equal(db.calls.length, 1, 'no unfenced UPDATE may be issued');
  });

  it('reports false when the lease has been taken by someone else', async () => {
    const db = {
      n: 0,
      async query() {
        this.n += 1;
        return this.n === 1 ? { rowCount: 1, rows: [{ claim_token: '1' }] } : { rowCount: 0, rows: [] };
      },
    };
    let renewed = true;
    await withCronLease(db, 'billing:daily', LEASE.HOURLY, async ({ renew }) => {
      renewed = await renew();
    });
    assert.equal(renewed, false, 'a tick that overran its lease must be able to find out');
  });

  it('rejects a non-positive extension', async () => {
    await withCronLease(fakeDb({ rowCount: 1 }), 'a:b', LEASE.HOURLY, async ({ renew }) => {
      await assert.rejects(renew(0), /extendMs/);
      await assert.rejects(renew(NaN), /extendMs/);
    });
  });
});

describe('LEASE presets', () => {
  it('are all shorter than their interval, or every other tick would be skipped', () => {
    const intervals = {
      EVERY_MINUTE: 60 * 1000,
      EVERY_5_MIN: 5 * 60 * 1000,
      EVERY_10_MIN: 10 * 60 * 1000,
      HOURLY: 60 * 60 * 1000,
      DAILY: 24 * 60 * 60 * 1000,
    };
    for (const [name, interval] of Object.entries(intervals)) {
      assert.ok(LEASE[name] < interval, `${name} must be shorter than its interval`);
      assert.ok(LEASE[name] > interval * 0.5, `${name} must not expire too eagerly`);
    }
  });

  it('is frozen', () => {
    assert.throws(() => { LEASE.HOURLY = 1; }, TypeError);
  });
});

describe('table naming', () => {
  it('creates the table idempotently, keyed by job name', () => {
    assert.match(CRON_LEASES_TABLE_SQL, /CREATE TABLE IF NOT EXISTS "cron_leases"/);
    assert.match(CRON_LEASES_TABLE_SQL, /job_name\s+TEXT PRIMARY KEY/);
  });

  it('supports a custom or schema-qualified table', async () => {
    assert.match(createTableSql('jobs.leases'), /CREATE TABLE IF NOT EXISTS "jobs"\."leases"/);

    const db = fakeDb({ rowCount: 1 });
    await withCronLease(db, 'a:b', LEASE.HOURLY, counter(), { table: 'jobs.leases' });
    assert.match(db.calls[0].sql, /INSERT INTO "jobs"\."leases"/);
  });

  it('refuses anything that is not an identifier', async () => {
    // A table name cannot be a bind parameter, so it is the one value that
    // reaches the statement as text.
    for (const bad of ['cron_leases; DROP TABLE users', 'a.b.c', '', 'has space', '1abc', null]) {
      assert.throws(() => quoteTable(bad), TypeError, `should reject ${JSON.stringify(bad)}`);
    }
    await assert.rejects(
      withCronLease(fakeDb(), 'a:b', LEASE.HOURLY, counter(), { table: 'x"; DROP TABLE y --' }),
      TypeError,
    );
  });
});
