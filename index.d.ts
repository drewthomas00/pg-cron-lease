/**
 * pg-cron-lease — make an in-process cron job a singleton across replicas,
 * using the Postgres you already have.
 */

/** Anything with a `pg`-shaped query method: a Pool, a Client, a transaction. */
export interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rowCount?: number | null; rows?: unknown[] }>;
}

export interface LeaseContext {
  /**
   * Set this claim's lease to expire `extendMs` from now. Resolves false when
   * we no longer hold it — meaning another replica has claimed the job and may
   * be running it. Renew by what the next piece of work needs: the lease is
   * never released early, so anything renewed past the end of the tick skips
   * the next fire.
   *
   * @param extendMs how far past now to push the expiry; defaults to `leaseMs`
   * @throws {TypeError} if the driver returned no rows to fence the claim on
   */
  renew(extendMs?: number): Promise<boolean>;
  jobName: string;
  /** The holder name this claim wrote to the `holder` column. */
  holder: string;
  leaseMs: number;
}

export interface LeaseOptions {
  logger?: { debug?: (message: string) => void };
  /**
   * Identifies this claimant in the table's `holder` column. Defaults to
   * `hostname:pid`. Informational only: `renew` fences on the claim itself,
   * so replicas sharing a name are still told apart.
   */
  holder?: string;
  /** Lease table, optionally schema-qualified. Defaults to `cron_leases`. */
  table?: string;
}

/**
 * `ran: true` carries the tick's return value; `ran: false` means another
 * replica holds the lease for this occurrence and the tick was skipped.
 */
export type LeaseOutcome<T> = { ran: true; result: T } | { ran: false; result: null };

/**
 * Run `runTick` only if this process wins the lease for `jobName`.
 *
 * @throws {TypeError} on a malformed argument — always before the lease is
 *   taken — or when the driver's result carries no usable claim signal.
 */
export declare function withCronLease<T>(
  db: Queryable,
  jobName: string,
  leaseMs: number,
  runTick: (ctx: LeaseContext) => Promise<T> | T,
  opts?: LeaseOptions,
): Promise<LeaseOutcome<T>>;

/** Lease durations for common cadences — about 80% of the interval. */
export declare const LEASE: Readonly<{
  EVERY_MINUTE: number;
  EVERY_5_MIN: number;
  EVERY_10_MIN: number;
  HOURLY: number;
  DAILY: number;
}>;

/** DDL for the default `cron_leases` table. */
export declare const CRON_LEASES_TABLE_SQL: string;

/** DDL for a custom or schema-qualified table. */
export declare function createTableSql(table?: string): string;

/** Quote an identifier for interpolation. Throws on anything else. */
export declare function quoteTable(table: string): string;

export declare const DEFAULT_TABLE: string;
