'use strict';

/**
 * pg-cron-lease — make an in-process cron job a singleton across replicas,
 * using the Postgres you already have.
 *
 * Node schedulers (node-cron, setInterval) start their jobs in every replica.
 * In-process guards like a `this.running` flag only prevent overlap *within*
 * one process — so the moment you run two containers, both fire the same tick.
 * During a rolling deploy old and new tasks overlap for minutes; after any
 * scale-out it is permanent. Duplicate sends and double charges follow.
 *
 * This is a lease, not a lock. Each tick atomically claims a row keyed by job
 * name, with an expiry set to roughly 80% of the cron interval. Exactly one
 * claimant wins — the ON CONFLICT UPDATE only applies when the stored lease has
 * already expired — and everyone else skips that occurrence.
 *
 * Every timestamp is read from the database, never from a replica, so the
 * mechanism is immune to clock skew between machines. That is the whole reason
 * it works, and it is why the SQL uses `clock_timestamp()` rather than `NOW()`:
 * `NOW()` is `transaction_timestamp()`, frozen at the start of the surrounding
 * transaction. Pass a transaction handle that has been open for a few seconds
 * and every comparison is stale — an expired lease reads as held (the
 * occurrence is silently skipped) and a fresh claim expires early (a second
 * replica can claim inside the window you meant to reserve).
 *
 * The lease is deliberately NEVER released early. Holding it until expiry is
 * what protects against scheduler drift: node-cron fires drift by seconds, so a
 * fast tick that released immediately would let a lagging replica claim the
 * same occurrence and run it again.
 *
 * Failure semantics, both chosen so a missed occurrence beats a duplicate:
 *
 *   - The claim query throws (database unreachable) → the error propagates to
 *     your tick's own error handling. Fail loud: the work inside the tick
 *     almost certainly needs that same database anyway.
 *
 *   - Your tick function throws → propagates, and the lease stays held until it
 *     expires. The other replica does not immediately re-run a partially
 *     completed job; the next scheduled fire retries it cleanly.
 */

const os = require('node:os');

/** Unqualified or schema-qualified SQL identifier. */
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$]*$/;

/** The table `CRON_LEASES_TABLE_SQL` and `withCronLease` use by default. */
const DEFAULT_TABLE = 'cron_leases';

/**
 * Quote a caller-supplied table name so it can be interpolated into SQL.
 *
 * A table name cannot be a bind parameter, so it is the one value in this
 * library that reaches the statement as text. Nothing but an identifier — or a
 * `schema.table` pair — gets through.
 *
 * @param {string} table
 * @returns {string} e.g. `"public"."cron_leases"`
 * @throws {TypeError} on anything that is not a plain identifier
 */
function quoteTable(table) {
  if (typeof table !== 'string' || !table) {
    throw new TypeError(`pg-cron-lease: table must be a non-empty string (got ${table})`);
  }
  const parts = table.split('.');
  if (parts.length > 2 || !parts.every((part) => IDENTIFIER.test(part))) {
    throw new TypeError(
      `pg-cron-lease: table must be an identifier or schema.identifier (got '${table}')`,
    );
  }
  return parts.map((part) => `"${part}"`).join('.');
}

/**
 * DDL for the lease table. Run once per database, via your migration tool.
 *
 * @param {string} [table=DEFAULT_TABLE]
 * @returns {string}
 */
function createTableSql(table = DEFAULT_TABLE) {
  return `
CREATE TABLE IF NOT EXISTS ${quoteTable(table)} (
  job_name    TEXT PRIMARY KEY,
  lease_until TIMESTAMPTZ NOT NULL,
  holder      TEXT,
  claimed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
`;
}

/** DDL for the default table, as a constant. */
const CRON_LEASES_TABLE_SQL = createTableSql();

/**
 * Lease durations for common cadences — about 80% of the interval.
 *
 * The ratio matters in both directions. A lease equal to the full interval
 * would still be held when the next fire arrives, skipping every other tick. A
 * very short lease would expire before a slow replica's clock catches up,
 * letting it re-run the occurrence you just handled.
 */
const LEASE = Object.freeze({
  EVERY_MINUTE: 50 * 1000,
  EVERY_5_MIN: 4 * 60 * 1000,
  EVERY_10_MIN: 8 * 60 * 1000,
  HOURLY: 50 * 60 * 1000,
  DAILY: 20 * 60 * 60 * 1000,
});

const DEFAULT_HOLDER = `${os.hostname()}:${process.pid}`;

/**
 * Did the claim win?
 *
 * `pg` reports `rowCount`, but this library accepts "anything with a
 * `.query()`", and a thin wrapper may forward only `rows`. Reading `rowCount`
 * alone and treating anything that is not the number zero as a win means an
 * unrecognised result runs the tick in EVERY replica — the precise failure this
 * package exists to prevent. Fall back to the `RETURNING` rows, and refuse to
 * guess when neither is present.
 *
 * @param {object} res
 * @returns {boolean|null} null when the result carries no usable signal
 */
function claimWon(res) {
  if (!res || typeof res !== 'object') return null;
  if (typeof res.rowCount === 'number') return res.rowCount > 0;
  if (Array.isArray(res.rows)) return res.rows.length > 0;
  return null;
}

/** Call `logger[level]` if there is one. A partial logger must not break a tick. */
function log(logger, level, message) {
  if (logger && typeof logger[level] === 'function') logger[level](message);
}

/**
 * Run `runTick` only if this process wins the lease for `jobName`.
 *
 * The claim is a single atomic upsert: it wins if and only if no row exists or
 * the stored lease has expired. No transaction, no advisory lock, no retry.
 *
 * `runTick` is called with a context object, so a long-running tick can hold
 * its own lease open:
 *
 *   await withCronLease(pool, 'billing:hourly', LEASE.HOURLY, async ({ renew }) => {
 *     for (const batch of batches) {
 *       if (!await renew()) throw new Error('lost the lease — another replica has it');
 *       await process(batch);
 *     }
 *   });
 *
 * @param {{query: Function}} db - a `pg` Pool or Client
 * @param {string} jobName - unique key for this job, e.g. 'billing:daily-invoice'
 * @param {number} leaseMs - how long the claim excludes other replicas; use a
 *   LEASE preset, or roughly 80% of your interval
 * @param {(ctx: {renew: (ms?: number) => Promise<boolean>, jobName: string,
 *   holder: string, leaseMs: number}) => Promise<any>} runTick
 * @param {{logger?: {debug?: Function}, holder?: string, table?: string}} [opts]
 * @returns {Promise<{ran: boolean, result: any}>} `ran: false` means another
 *   replica holds the lease for this occurrence and the tick was skipped.
 * @throws {TypeError} on a malformed argument, always before the lease is taken
 */
async function withCronLease(db, jobName, leaseMs, runTick, opts = {}) {
  const { logger, holder, table = DEFAULT_TABLE } = opts || {};

  // Validate everything BEFORE claiming. A claim taken and then abandoned to a
  // TypeError holds the lease for its full duration, so a wiring typo would
  // silently disable the job fleet-wide until it expires — and again on every
  // fire after that.
  if (!db || typeof db.query !== 'function') {
    throw new TypeError('withCronLease: db must be a pg Pool/Client, or anything with .query(sql, params)');
  }
  if (!jobName || typeof jobName !== 'string') {
    throw new TypeError('withCronLease: jobName (string) is required');
  }
  // Validated after rounding: a leaseMs of 0.4 is "positive" but becomes a
  // zero-millisecond lease — one that is born expired, which every replica
  // then claims in turn. That is the exact failure this package prevents.
  if (!Number.isFinite(leaseMs) || Math.round(leaseMs) <= 0) {
    throw new TypeError(`withCronLease: leaseMs must be at least 1 millisecond (got ${leaseMs})`);
  }
  if (typeof runTick !== 'function') {
    throw new TypeError('withCronLease: runTick (function) is required');
  }

  const target = quoteTable(table);
  const claimant = holder || DEFAULT_HOLDER;
  const ms = Math.round(leaseMs);

  // clock_timestamp(), not NOW(): see the module header. NOW() is frozen at the
  // start of the surrounding transaction, so a caller passing a transaction
  // handle would compare against a stale clock in both directions.
  const res = await db.query(
    `INSERT INTO ${target} (job_name, lease_until, holder, claimed_at)
     VALUES ($1, clock_timestamp() + ($2::bigint * interval '1 millisecond'), $3, clock_timestamp())
     ON CONFLICT (job_name) DO UPDATE
       SET lease_until = EXCLUDED.lease_until,
           holder = EXCLUDED.holder,
           claimed_at = clock_timestamp()
       WHERE ${target}.lease_until <= clock_timestamp()
     RETURNING job_name`,
    [jobName, ms, claimant],
  );

  const won = claimWon(res);
  if (won === null) {
    // The lease is held at this point, so the occurrence is lost either way.
    // Throw rather than skip: silently returning `ran: false` forever is a job
    // that never runs and never complains.
    throw new TypeError(
      'withCronLease: db.query must resolve to { rowCount } or { rows } — '
      + `cannot tell whether the lease for '${jobName}' was claimed`,
    );
  }

  if (!won) {
    log(logger, 'debug', `${jobName}: cron lease held by another replica — skipping this fire`);
    return { ran: false, result: null };
  }

  /**
   * Extend this holder's lease. Returns false if we no longer hold it, which
   * means another replica has already claimed the job and may be running it.
   *
   * The `holder` and `lease_until` predicates are what make this safe: a lease
   * that expired and was re-claimed cannot be extended out from under its new
   * owner.
   */
  const renew = async (extendMs = ms) => {
    const bump = Math.round(extendMs);
    if (!Number.isFinite(bump) || bump <= 0) {
      throw new TypeError(`renew: extendMs must be a positive number (got ${extendMs})`);
    }
    const out = await db.query(
      `UPDATE ${target}
          SET lease_until = clock_timestamp() + ($2::bigint * interval '1 millisecond')
        WHERE job_name = $1
          AND holder = $3
          AND lease_until > clock_timestamp()
      RETURNING job_name`,
      [jobName, bump, claimant],
    );
    return claimWon(out) === true;
  };

  const result = await runTick({ renew, jobName, holder: claimant, leaseMs: ms });
  return { ran: true, result };
}

module.exports = {
  withCronLease,
  LEASE,
  CRON_LEASES_TABLE_SQL,
  createTableSql,
  quoteTable,
  DEFAULT_TABLE,
};
