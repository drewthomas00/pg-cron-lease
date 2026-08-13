'use strict';

/**
 * The claim's actual concurrency semantics, against a real Postgres.
 *
 * The unit suite drives a stand-in pool, which can only prove the SQL string
 * was typed correctly — for a library that IS one SQL statement, that is close
 * to proving nothing. This suite is the one that matters: two replicas firing
 * the same occurrence must resolve to exactly one runner, an expired lease must
 * be re-claimable, and a lease must not be extendable out from under whoever
 * took it next.
 *
 * Self-skips when no database is reachable, so `npm test` stays green on a
 * laptop without one. Set DATABASE_URL to run it:
 *
 *   docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:18-alpine
 *   DATABASE_URL=postgres://postgres:pg@localhost:5432/postgres npm test
 */

const { describe, it, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const { withCronLease, LEASE, createTableSql } = require('..');

const TABLE = 'pg_cron_lease_int';
const JOB = 'int-test:lease';

let pool = null;
let reason = 'DATABASE_URL is not set';

before(async () => {
  if (!process.env.DATABASE_URL) return;
  let Pool;
  try {
    ({ Pool } = require('pg'));
  } catch {
    reason = "the 'pg' package is not installed";
    return;
  }
  const candidate = new Pool({ connectionString: process.env.DATABASE_URL, max: 4 });
  try {
    await candidate.query(createTableSql(TABLE));
    pool = candidate;
  } catch (err) {
    reason = `database unreachable: ${err.message}`;
    await candidate.end().catch(() => {});
  }
});

after(async () => {
  if (!pool) return;
  await pool.query(`DROP TABLE IF EXISTS "${TABLE}"`);
  await pool.end();
});

beforeEach(async () => {
  if (pool) await pool.query(`DELETE FROM "${TABLE}" WHERE job_name LIKE $1`, [`${JOB}%`]);
});

/**
 * Runs the body only when a database is available; otherwise reports a skip.
 *
 * The skip is decided inside the test, not in the `it` options: option objects
 * are evaluated when the test is registered, which happens before `before()`
 * has had a chance to connect.
 */
const itDb = (name, fn) => it(name, async (t) => {
  if (!pool) {
    t.skip(reason);
    return;
  }
  await fn();
});

const opts = (holder) => ({ table: TABLE, holder });

describe('cross-replica claim (real Postgres)', () => {
  itDb('two replicas firing the same occurrence → exactly one runs', async () => {
    const ticks = [];
    const outcomes = await Promise.all([
      withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => ticks.push('a'), opts('replica-a')),
      withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => ticks.push('b'), opts('replica-b')),
    ]);

    assert.equal(outcomes.filter((o) => o.ran).length, 1);
    assert.equal(ticks.length, 1);
  });

  itDb('ten replicas racing → still exactly one runs', async () => {
    const outcomes = await Promise.all(Array.from({ length: 10 }, (_, i) =>
      withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => i, opts(`replica-${i}`))));

    assert.equal(outcomes.filter((o) => o.ran).length, 1);
  });

  itDb('a skewed second fire inside the lease window is skipped', async () => {
    // node-cron drifts by seconds between machines; this is the case the lease
    // exists for.
    const first = await withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => 'ran', opts('replica-a'));
    assert.equal(first.ran, true);

    const second = await withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => 'ran', opts('replica-b'));
    assert.deepEqual(second, { ran: false, result: null });
  });

  itDb('an expired lease is re-claimable by anyone', async () => {
    await withCronLease(pool, JOB, 60, async () => 'first', opts('replica-a'));
    await new Promise((r) => setTimeout(r, 150));

    const second = await withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => 'second', opts('replica-b'));
    assert.equal(second.ran, true);

    const { rows } = await pool.query(`SELECT holder FROM "${TABLE}" WHERE job_name = $1`, [JOB]);
    assert.equal(rows[0].holder, 'replica-b');
  });

  itDb('a failed tick leaves the lease held, so nobody re-runs it immediately', async () => {
    await assert.rejects(
      withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => { throw new Error('boom'); }, opts('replica-a')),
      /boom/,
    );
    const next = await withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => 'ran', opts('replica-b'));
    assert.equal(next.ran, false, 'a missed occurrence beats a half-completed duplicate');
  });
});

describe('the clock is the database, not the replica', () => {
  itDb('claims correctly from inside a long-open transaction', async () => {
    // NOW() is transaction_timestamp(). With it, a lease that has genuinely
    // expired reads as still held inside a transaction that has been open a
    // few seconds, and the occurrence is silently skipped forever.
    await withCronLease(pool, JOB, 60, async () => 'first', opts('replica-a'));

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_sleep(0.5)');   // the transaction clock is now stale

      const out = await withCronLease(client, JOB, LEASE.EVERY_5_MIN, async () => 'second', opts('replica-b'));
      assert.equal(out.ran, true, 'the lease had expired on the wall clock');
      await client.query('COMMIT');
    } finally {
      client.release();
    }
  });
});

describe('renew', () => {
  itDb('extends a lease the holder still owns', async () => {
    let before;
    let after;
    await withCronLease(pool, JOB, 2000, async ({ renew }) => {
      ({ rows: [{ lease_until: before }] } =
        await pool.query(`SELECT lease_until FROM "${TABLE}" WHERE job_name = $1`, [JOB]));
      assert.equal(await renew(60000), true);
      ({ rows: [{ lease_until: after }] } =
        await pool.query(`SELECT lease_until FROM "${TABLE}" WHERE job_name = $1`, [JOB]));
    }, opts('replica-a'));

    assert.ok(after > before, 'renew must push lease_until forward');
  });

  itDb('refuses to extend a lease that has already been re-claimed', async () => {
    // The fencing case: a tick that overran its lease must be able to find out
    // that someone else is now running the job.
    let renewed = true;
    await withCronLease(pool, JOB, 60, async ({ renew }) => {
      await new Promise((r) => setTimeout(r, 150));
      await withCronLease(pool, JOB, LEASE.EVERY_5_MIN, async () => 'stolen', opts('replica-b'));
      renewed = await renew();
    }, opts('replica-a'));

    assert.equal(renewed, false);
  });
});
