/*
 * synclite.h — C ABI for the SyncLite runtime.
 *
 * Mirrors the Rust user-facing API (`synclite::initialize`,
 * `synclite::rusqlite::Connection`, prepared statements, `await_sync`).
 * Hand-written to match `crates/logger/bindings-c/src/lib.rs`.
 *
 * Link against `synclite_c` (cdylib or staticlib).
 *
 * All strings are NUL-terminated UTF-8. Functions returning `int` use
 * 0 for success and -1 for error; call `synclite_last_error()` on the
 * same thread to retrieve the message.
 */
#ifndef SYNCLITE_H
#define SYNCLITE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ----- Value variant -------------------------------------------------- */

typedef enum SyncLiteValueTag {
    SYNCLITE_VAL_NULL = 0,
    SYNCLITE_VAL_INT  = 1,
    SYNCLITE_VAL_REAL = 2,
    SYNCLITE_VAL_TEXT = 3,
    SYNCLITE_VAL_BLOB = 4,
} SyncLiteValueTag;

typedef struct SyncLiteValue {
    SyncLiteValueTag tag;
    int64_t          int_val;
    double           real_val;
    const char*      text_val;   /* NUL-terminated UTF-8 */
    const uint8_t*   blob_ptr;
    size_t           blob_len;
} SyncLiteValue;

/* ----- Destination spec ---------------------------------------------- */

typedef struct SyncLiteDestination {
    const char* dst_type;              /* "SQLITE" | "DUCKDB" | "POSTGRES" */
    const char* dst_connection_string;
    const char* dst_database;          /* may be NULL */
    const char* dst_schema;            /* may be NULL */
    const char* dst_sync_mode;         /* "CONSOLIDATION" | "REPLICATION" */
} SyncLiteDestination;

/* ----- Opaque handles ------------------------------------------------- */

typedef struct SyncLiteConnection     SyncLiteConnection;
typedef struct SyncLiteStatement      SyncLiteStatement;
typedef struct SyncLiteDuckConnection SyncLiteDuckConnection;
typedef struct SyncLiteDuckStatement  SyncLiteDuckStatement;
typedef struct SyncLiteRows           SyncLiteRows;

/* ----- Module functions ---------------------------------------------- */

int  synclite_initialize(const char* device_type,
                         const char* device_name,
                         const char* db_path,
                         const SyncLiteDestination* destination /* may be NULL */,
                         const char* config_path                /* may be NULL */);

int  synclite_await_sync(const char* db_path, double timeout_seconds);

const char* synclite_last_error(void);

/* ----- SQLite-family connection -------------------------------------- */

SyncLiteConnection* synclite_connection_open(const char* db_path);
SyncLiteConnection* synclite_connection_open_with_config(const char* conf_path);
SyncLiteConnection* synclite_connection_initialize(const char* db_path);
SyncLiteConnection* synclite_connection_initialize_with_config(const char* conf_path);

int  synclite_connection_execute(SyncLiteConnection* conn,
                                 const char* sql,
                                 const SyncLiteValue* params, size_t n,
                                 uint64_t* out_rows /* may be NULL */);
int  synclite_connection_query  (SyncLiteConnection* conn,
                                 const char* sql,
                                 const SyncLiteValue* params, size_t n,
                                 SyncLiteRows** out);
SyncLiteStatement* synclite_connection_prepare(SyncLiteConnection* conn,
                                               const char* sql);
int  synclite_connection_set_auto_commit(SyncLiteConnection* conn, int auto_commit);
int  synclite_connection_get_auto_commit(SyncLiteConnection* conn);
int  synclite_connection_commit  (SyncLiteConnection* conn);
int  synclite_connection_rollback(SyncLiteConnection* conn);
int  synclite_connection_flush   (SyncLiteConnection* conn);
int  synclite_connection_close   (SyncLiteConnection* conn);

int  synclite_stmt_execute(SyncLiteStatement* stmt,
                           const SyncLiteValue* params, size_t n,
                           uint64_t* out_rows);
int  synclite_stmt_query  (SyncLiteStatement* stmt,
                           const SyncLiteValue* params, size_t n,
                           SyncLiteRows** out);
int  synclite_stmt_add_batch  (SyncLiteStatement* stmt,
                               const SyncLiteValue* params, size_t n);
int  synclite_stmt_clear_batch(SyncLiteStatement* stmt);
int  synclite_stmt_execute_batch(SyncLiteStatement* stmt,
                                 uint64_t** out_rows /* caller frees */,
                                 size_t*   out_len);
void synclite_stmt_free(SyncLiteStatement* stmt);

/* ----- DuckDB-family connection -------------------------------------- */

SyncLiteDuckConnection* synclite_duckdb_connection_open(const char* db_path);
SyncLiteDuckConnection* synclite_duckdb_connection_open_with_config(const char* conf_path);
SyncLiteDuckConnection* synclite_duckdb_connection_initialize(const char* db_path);
SyncLiteDuckConnection* synclite_duckdb_connection_initialize_with_config(const char* conf_path);

int  synclite_duckdb_connection_execute(SyncLiteDuckConnection* conn,
                                        const char* sql,
                                        const SyncLiteValue* params, size_t n,
                                        uint64_t* out_rows);
int  synclite_duckdb_connection_query  (SyncLiteDuckConnection* conn,
                                        const char* sql,
                                        const SyncLiteValue* params, size_t n,
                                        SyncLiteRows** out);
SyncLiteDuckStatement* synclite_duckdb_connection_prepare(SyncLiteDuckConnection* conn,
                                                          const char* sql);
int  synclite_duckdb_connection_set_auto_commit(SyncLiteDuckConnection* conn, int auto_commit);
int  synclite_duckdb_connection_get_auto_commit(SyncLiteDuckConnection* conn);
int  synclite_duckdb_connection_commit  (SyncLiteDuckConnection* conn);
int  synclite_duckdb_connection_rollback(SyncLiteDuckConnection* conn);
int  synclite_duckdb_connection_flush   (SyncLiteDuckConnection* conn);
int  synclite_duckdb_connection_close   (SyncLiteDuckConnection* conn);

int  synclite_duckdb_stmt_execute(SyncLiteDuckStatement* stmt,
                                  const SyncLiteValue* params, size_t n,
                                  uint64_t* out_rows);
int  synclite_duckdb_stmt_query  (SyncLiteDuckStatement* stmt,
                                  const SyncLiteValue* params, size_t n,
                                  SyncLiteRows** out);
int  synclite_duckdb_stmt_add_batch  (SyncLiteDuckStatement* stmt,
                                      const SyncLiteValue* params, size_t n);
int  synclite_duckdb_stmt_clear_batch(SyncLiteDuckStatement* stmt);
int  synclite_duckdb_stmt_execute_batch(SyncLiteDuckStatement* stmt,
                                        uint64_t** out_rows, size_t* out_len);
void synclite_duckdb_stmt_free(SyncLiteDuckStatement* stmt);

/* ----- Rows ----------------------------------------------------------- */

size_t                  synclite_rows_count(const SyncLiteRows* rows);
size_t                  synclite_rows_cols (const SyncLiteRows* rows);
const SyncLiteValue*    synclite_rows_cell (const SyncLiteRows* rows, size_t row, size_t col);
void                    synclite_rows_free (SyncLiteRows* rows);

/* ----- Free helpers --------------------------------------------------- */

void synclite_free_u64_array(uint64_t* ptr, size_t len);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SYNCLITE_H */
