# pg-cron-lease

[![npm](https://img.shields.io/npm/v/pg-cron-lease)](https://www.npmjs.com/package/pg-cron-lease)
[![CI](https://github.com/drewthomas00/pg-cron-lease/actions/workflows/ci.yml/badge.svg)](https://github.com/drewthomas00/pg-cron-lease/actions/workflows/ci.yml)
[![dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)](https://www.npmjs.com/package/pg-cron-lease?activeTab=dependencies)
[![node](https://img.shields.io/node/v/pg-cron-lease)](https://www.npmjs.com/package/pg-cron-lease)
[![license](https://img.shields.io/npm/l/pg-cron-lease)](LICENSE)

Make an in-process cron job a singleton across replicas, using the Postgres you already have.

```sh
npm install pg-cron-lease
```

## The problem

Node schedulers — `node-cron`, `setInterval`, whatever you reach for — start their jobs in **every replica**. In-process guards like a `this.running` flag only prevent overlap *within* one process.

So the moment you run two containers, both fire the same tick. During a rolling deploy the old and new tasks overlap for minutes; after any scale-out it's permanent. If that job sends email, charges cards, or posts to an API, you now do it twice.

The usual answers are a Redis lock or a coordination service. If you already have Postgres, you don't need either.

## Usage

Once per database, via your migration tool:

```js
const { CRON_LEASES_TABLE_SQL } = require('pg-cron-lease');
await db.query(CRON_LEASES_TABLE_SQL);
```

Then wrap the tick:

```js
const cron = require('node-cron');
const { withCronLease, LEASE } = require('pg-cron-lease');

cron.schedule('0 * * * *', async () => {
  const { ran } = await withCronLease(
    pool,                 // a pg Pool or Client
    'billing:hourly-sync',
    LEASE.HOURLY,
    async () => sendPendingInvoices(),
  );
  if (!ran) return;       // another replica has this occurrence
});
```

## How it works

Each tick performs one atomic upsert:

```sql
INSERT INTO cron_leases (job_name, lease_until, holder, claimed_at)
VALUES ($1, clock_timestamp() + …, $3, clock_timestamp())
ON CONFLICT (job_name) DO UPDATE
  SET lease_until = EXCLUDED.lease_until, …
  WHERE cron_leases.lease_until <= clock_timestamp()
RETURNING job_name, …claimed_at AS claim_token
```

**The `WHERE` on the `DO UPDATE` is the entire mechanism.** Without it every replica would overwrite the row and all of them would run. With it, the update applies only when the stored lease has already expired — so exactly one claimant gets a row back, and everyone else gets nothing and skips.

No transaction, no advisory lock, no retry loop. One statement.

### Every timestamp comes from the database

That is why this works at all: replicas never compare their own clocks to anything, so skew between machines cannot produce two winners.

It is also why the SQL says `clock_timestamp()` and not `NOW()`. **`NOW()` is `transaction_timestamp()`** — frozen at the start of the surrounding transaction. Pass a transaction handle that has been open for a few seconds and every comparison is stale in both directions: an expired lease reads as still held (that occurrence is silently skipped), and a fresh claim expires early (a second replica can claim inside the window you meant to reserve). It's an easy thing to get wrong and a hard thing to notice.

### It's a lease, not a lock

**The lease is never released early**, and that's deliberate. Schedulers drift by seconds between machines. A fast tick that released its lease immediately would let a lagging replica claim the *same occurrence* and run it again — reintroducing the exact bug you installed this to fix.

So the lease is held until it expires, which is why the duration matters:

| Preset | Duration | For an interval of |
|---|---|---|
| `LEASE.EVERY_MINUTE` | 50s | 1 min |
| `LEASE.EVERY_5_MIN` | 4 min | 5 min |
| `LEASE.EVERY_10_MIN` | 8 min | 10 min |
| `LEASE.HOURLY` | 50 min | 1 hour |
| `LEASE.DAILY` | 20 hours | 1 day |

Roughly 80% of the interval, and it has to be in that band. A lease equal to the full interval is still held when the next fire arrives, so you'd skip every other tick. A very short lease expires before a slow replica catches up, so it re-runs what you just did.

### A tick that outlives its lease

This prevents duplicate **claims** per occurrence. It does not, by itself, prevent two copies of a *slow* tick running at once: if your hourly job takes 55 minutes and the lease is 50, the lease expires while you're still working and the next replica claims it legitimately.

Keep the tick well under the lease, or hold your own lease open. `runTick` is called with a context that lets you:

```js
const BATCH_BUDGET_MS = 5 * 60 * 1000; // generously more than one batch takes

await withCronLease(pool, 'billing:hourly', LEASE.HOURLY, async ({ renew }) => {
  for (const batch of batches) {
    if (!await renew(BATCH_BUDGET_MS)) throw new Error('lost the lease — another replica has it');
    await processBatch(batch);
  }
});
```

`renew(ms)` sets the lease to expire `ms` from now, only while you still hold it, and resolves **false** once someone else has claimed the job — which is your signal to stop rather than to keep writing.

Renew by what the next piece of work needs, not by the whole lease. The lease is never released early, so whatever you renew past the end of your tick is still held when the next fire arrives — and that occurrence is skipped. `renew()` with no argument extends by the full `leaseMs`; on an hourly job that is 50 minutes past the moment you call it.

"You" means this claim, not this process. `renew()` fences on the `claimed_at` its claim returned (the database clock, to the microsecond), so an overrunning tick cannot extend a lease that its own process's next fire has since re-claimed — nor one claimed by a replica that shares its holder name (every container is pid 1, so two with the same fixed hostname do). That fence needs the claim's `RETURNING` row: a driver that reports only `rowCount` can claim, but `renew()` will throw rather than extend a lease it cannot prove is its own.

## Failure semantics

Every failure path is chosen on the same principle: **a missed occurrence beats a duplicate.**

**The claim query throws** (database unreachable) → the error propagates to your handler. Fail loud. The work inside your tick almost certainly needs that same database anyway, so pretending you got the lease helps nobody.

**Your tick throws** → propagates, and the lease *stays held until it expires*. The other replica does not immediately re-run a job that half-completed. The next scheduled fire retries it from a clean state.

**The driver returns something unrecognisable** → throws. `withCronLease` reads `rowCount`, falls back to the `RETURNING` rows, and refuses to guess when neither is present. Treating "I can't tell" as a win would run the tick in every replica, which is the one outcome this package exists to prevent.

This is the right trade for sends, charges, and anything else with external side effects. If your job is idempotent and you'd rather retry immediately, this library is the wrong shape — you want a queue.

## Notes

- Arguments are validated **before** the lease is claimed. A claim taken and then abandoned to a `TypeError` would hold the lease for its full duration, so a wiring typo would silently disable the job fleet-wide.
- `jobName` is a free-form string and the primary key. Namespace it (`'billing:hourly-sync'`) so two services can't collide.
- `holder` defaults to `hostname:pid`, so `SELECT * FROM cron_leases` tells you who last *claimed* each job, and when. It is informational — nothing fences on it. The row does not tell you whether the claimant finished: after a crash it looks the same as a healthy one until it expires.
- `table` (and `createTableSql(table)`) accept a custom or schema-qualified name. A table name can't be a bind parameter, so it's validated as an identifier and quoted.
- The table holds only job names, holder names and timestamps — no application data.
- `pg` is an optional peer dependency — anything with a `.query(sql, params)` resolving to `{rowCount}` or `{rows}` works (`renew()` needs the `rows`).
- **Pass a pool, not an open transaction.** It works — `clock_timestamp()` keeps the comparison honest — but the claim then lives inside your transaction: other replicas' claims *block* on the row lock until you commit instead of skipping straight away, and a rollback erases the claim, so a tick that throws no longer keeps its lease.

## Testing

```sh
npm test
```

Node's built-in test runner, no dependencies. The unit suite drives a stand-in pool; the integration suite runs the real thing and **self-skips** when no database is reachable, so this stays green on a laptop without one:

```sh
docker run --rm -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:18-alpine
DATABASE_URL=postgres://postgres:pg@localhost:5432/postgres npm test
```

The integration suite is the one that matters — it races ten claimants at a single occurrence and asserts exactly one runs, re-claims an expired lease, proves a failed tick keeps its lease, and pins the transaction-clock behaviour above. CI runs it against Postgres 14 and 18.

## Related packages

Small, dependency-light pieces pulled out of production systems I've built:

- **[tcpa-quiet-hours](https://github.com/drewthomas00/tcpa-quiet-hours)** — is it legal to send this marketing message right now?
- **[twilio-signature-verify](https://github.com/drewthomas00/twilio-signature-verify)** — verify `X-Twilio-Signature`, including behind a reverse proxy
- **[us-zip-centroids](https://github.com/drewthomas00/us-zip-centroids)** — offline US ZIP → lat/lng, no geocoder

## License

MIT © [Drew Thomas](https://drewthomasbuilds.com)
