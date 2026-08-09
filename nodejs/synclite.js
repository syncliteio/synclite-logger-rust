// SyncLite Node.js package entry point.
//
// This is a thin ergonomic facade over the auto-generated N-API binding in
// `index.js`. It mirrors the SyncLite Python package experience: pass an
// options object to `initialize(...)`, bind statement parameters with plain
// JavaScript arrays, get query rows back as arrays, and prepare reusable
// statements that support `addBatch` / `executeBatch`.

'use strict';

const native = require('./index.js');

function toParamsJson(params) {
  if (params === undefined || params === null) {
    return undefined;
  }
  if (!Array.isArray(params)) {
    throw new TypeError('statement parameters must be an array');
  }
  return JSON.stringify(params);
}

/**
 * Register a device + destination ahead of any `open(...)` call.
 *
 * Mirrors the Python `initialize(...)` keyword arguments. Accepts an options
 * object (preferred) or a pre-serialized JSON string.
 */
function initialize(options) {
  const configJson = typeof options === 'string' ? options : JSON.stringify(options);
  return native.initialize(configJson);
}

/** Block until the embedded shipper and consolidator apply pending commits. */
function awaitSync(dbPath, timeoutSeconds) {
  return native.awaitSync(dbPath, timeoutSeconds);
}

/** Prepared statement bound to a parent connection. */
class Statement {
  constructor(connection, sql) {
    this._connection = connection;
    this._sql = sql;
    this._batches = [];
  }

  /** Execute the prepared statement once; returns the affected-row count. */
  execute(params) {
    return this._connection.execute(this._sql, params);
  }

  /** Run the prepared statement as a query; returns an array of rows. */
  query(params) {
    return this._connection.query(this._sql, params);
  }

  /** Queue one batch row. */
  addBatch(params) {
    this._batches.push(params === undefined || params === null ? [] : params);
    return this;
  }

  /** Clear queued batch rows. */
  clearBatch() {
    this._batches = [];
    return this;
  }

  /** Execute every queued batch row; returns an array of affected-row counts. */
  executeBatch() {
    const batches = this._batches;
    this._batches = [];
    const results = [];
    for (const row of batches) {
      results.push(this._connection.execute(this._sql, row));
    }
    return results;
  }
}

function wrapConnection(NativeConnection) {
  return class Connection {
    constructor(inner) {
      this._inner = inner;
    }

    static open(dbPath) {
      return new this(NativeConnection.open(dbPath));
    }

    static initialize(dbPath) {
      return new this(NativeConnection.initialize(dbPath));
    }

    static openWithConfig(configPath) {
      return new this(NativeConnection.openWithConfig(configPath));
    }

    static initializeWithConfig(configPath) {
      return new this(NativeConnection.initializeWithConfig(configPath));
    }

    /** Execute a mutating statement; returns the affected-row count. */
    execute(sql, params) {
      return this._inner.execute(sql, toParamsJson(params));
    }

    /** Execute a query; returns an array of rows (each row an array). */
    query(sql, params) {
      return JSON.parse(this._inner.queryJson(sql, toParamsJson(params)));
    }

    /** Prepare a reusable statement. */
    prepare(sql) {
      return new Statement(this, sql);
    }

    setAutoCommit(autoCommit) {
      return this._inner.setAutoCommit(autoCommit);
    }

    getAutoCommit() {
      return this._inner.getAutoCommit();
    }

    commit() {
      return this._inner.commit();
    }

    rollback() {
      return this._inner.rollback();
    }

    flush() {
      return this._inner.flush();
    }

    close() {
      return this._inner.close();
    }
  };
}

module.exports = {
  initialize,
  awaitSync,
  Statement,
  SqliteConnection: wrapConnection(native.SqliteConnection),
  DuckDbConnection: wrapConnection(native.DuckDbConnection),
};
