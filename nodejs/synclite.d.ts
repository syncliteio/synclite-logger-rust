/* SyncLite Node.js package type declarations (ergonomic facade). */

export interface DestinationOptions {
  dst_type: string;
  dst_connection_string: string;
  dst_database?: string;
  dst_schema?: string;
  dst_sync_mode?: string;
}

export interface InitializeOptions {
  device_type: string;
  device_name: string;
  db_path: string;
  destination?: DestinationOptions;
  config_path?: string;
}

/**
 * Register a device + destination ahead of any `open(...)` call. Accepts an
 * options object (preferred) or a pre-serialized JSON string.
 */
export declare function initialize(options: InitializeOptions | string): void;

/** Block until the embedded shipper and consolidator apply pending commits. */
export declare function awaitSync(dbPath: string, timeoutSeconds: number): void;

/** A single row of query results. */
export type Row = Array<number | string | null | number[]>;

/** Prepared statement bound to a parent connection. */
export declare class Statement {
  execute(params?: Array<unknown>): number;
  query(params?: Array<unknown>): Row[];
  addBatch(params?: Array<unknown>): this;
  clearBatch(): this;
  executeBatch(): number[];
}

/** SyncLite-managed SQLite-family connection. */
export declare class SqliteConnection {
  static open(dbPath: string): SqliteConnection;
  static initialize(dbPath: string): SqliteConnection;
  static openWithConfig(configPath: string): SqliteConnection;
  static initializeWithConfig(configPath: string): SqliteConnection;
  execute(sql: string, params?: Array<unknown>): number;
  query(sql: string, params?: Array<unknown>): Row[];
  prepare(sql: string): Statement;
  setAutoCommit(autoCommit: boolean): void;
  getAutoCommit(): boolean;
  commit(): void;
  rollback(): void;
  flush(): void;
  close(): void;
}

/** SyncLite-managed DuckDB-family connection. */
export declare class DuckDbConnection {
  static open(dbPath: string): DuckDbConnection;
  static initialize(dbPath: string): DuckDbConnection;
  static openWithConfig(configPath: string): DuckDbConnection;
  static initializeWithConfig(configPath: string): DuckDbConnection;
  execute(sql: string, params?: Array<unknown>): number;
  query(sql: string, params?: Array<unknown>): Row[];
  prepare(sql: string): Statement;
  setAutoCommit(autoCommit: boolean): void;
  getAutoCommit(): boolean;
  commit(): void;
  rollback(): void;
  flush(): void;
  close(): void;
}
