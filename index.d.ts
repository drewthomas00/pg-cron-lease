/**
 * pg-cron-lease — make an in-process cron job a singleton across replicas,
 * using the Postgres you already have.
 */

/** Anything with a `pg`-shaped query method: a Pool, a Client, a transaction. */
export interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rowCount?: number; rows?: unknown[] }>;
}

export interface LeaseContext {
  /**
   * Extend this holder's lease. Resolves false when we no longer hold it —
   * meaning another replica has claimed the job and may be running it.
   *
   * @param extendMs how far past now to push the expiry; defaults to `leaseMs`
   */
  renew(extendMs?: number): Promise<boolean>;
  jobName: string;
  holder: string;
  leaseMs: number;
}

export interface LeaseOptions {
  logger?: { debug?: (message: string) => void };
  /** Identifies this claimant in the table. Defaults to `hostname:pid`. */
  holder?: string;
  /** Lease table, optionally schema-qualified. Defaults to `cron_leases`. */
  table?: string;
}

export interface LeaseOutcome<T> {
  /** false means another replica holds the lease for this occurrence. */
  ran: boolean;
  /** The tick's return value, or null when it was skipped. */
  result: T | null;
}

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
