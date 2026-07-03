// synclite.hpp — header-only C++17 RAII wrapper over synclite.h.
//
// Mirrors the Rust `synclite::rusqlite::Connection` / `synclite::duckdb::Connection`
// API so C++ samples read like the Rust ones:
//
//     synclite::initialize("SQLITE", "device", "x.db");
//     synclite::Connection conn = synclite::Connection::open("x.db");
//     conn.execute("CREATE TABLE t(id INTEGER, name TEXT)");
//     {
//         auto stmt = conn.prepare("INSERT INTO t VALUES(?, ?)");
//         stmt.execute({1, "Alice"});
//         stmt.execute({2, "Bob"});
//     }
//     for (auto& row : conn.query("SELECT id, name FROM t ORDER BY id")) {
//         std::cout << row[0].as_int() << " " << row[1].as_text() << "\n";
//     }
//     conn.flush();
//     synclite::await_sync("x.db", 30.0);
//     conn.close();
//
// All methods throw `synclite::Error` (derived from `std::runtime_error`)
// on failure, pulling the message from `synclite_last_error()`.

#ifndef SYNCLITE_HPP
#define SYNCLITE_HPP

#include "synclite.h"

#include <cstdint>
#include <cstring>
#include <deque>
#include <initializer_list>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace synclite {

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {

class ParamBuf;

[[noreturn]] inline void throw_last(const char* fallback) {
    const char* msg = ::synclite_last_error();
    throw Error(msg ? msg : fallback);
}

inline void check(int rc, const char* fallback) {
    if (rc != 0) {
        throw_last(fallback);
    }
}

} // namespace detail

// ---------- Value ----------------------------------------------------------

using Blob = std::vector<std::uint8_t>;

class Value {
public:
    using Storage = std::variant<std::monostate, std::int64_t, double, std::string, Blob>;

    Value() = default;                                   // NULL
    Value(std::nullptr_t) {}                             // NULL
    Value(int v)            : storage_(static_cast<std::int64_t>(v)) {}
    Value(long v)           : storage_(static_cast<std::int64_t>(v)) {}
    Value(long long v)      : storage_(static_cast<std::int64_t>(v)) {}
    Value(unsigned int v)   : storage_(static_cast<std::int64_t>(v)) {}
    Value(unsigned long v)  : storage_(static_cast<std::int64_t>(v)) {}
    Value(double v)         : storage_(v) {}
    Value(float v)          : storage_(static_cast<double>(v)) {}
    Value(const char* v)    : storage_(std::string(v)) {}
    Value(std::string v)    : storage_(std::move(v)) {}
    Value(std::string_view v): storage_(std::string(v)) {}
    Value(Blob v)           : storage_(std::move(v)) {}

    bool is_null() const noexcept { return std::holds_alternative<std::monostate>(storage_); }
    bool is_int()  const noexcept { return std::holds_alternative<std::int64_t>(storage_); }
    bool is_real() const noexcept { return std::holds_alternative<double>(storage_); }
    bool is_text() const noexcept { return std::holds_alternative<std::string>(storage_); }
    bool is_blob() const noexcept { return std::holds_alternative<Blob>(storage_); }

    std::int64_t       as_int()  const { return std::get<std::int64_t>(storage_); }
    double             as_real() const { return std::get<double>(storage_); }
    const std::string& as_text() const { return std::get<std::string>(storage_); }
    const Blob&        as_blob() const { return std::get<Blob>(storage_); }

    static Value from_c(const SyncLiteValue& v) {
        switch (v.tag) {
            case SYNCLITE_VAL_NULL: return Value{};
            case SYNCLITE_VAL_INT:  return Value{v.int_val};
            case SYNCLITE_VAL_REAL: return Value{v.real_val};
            case SYNCLITE_VAL_TEXT: return Value{std::string(v.text_val ? v.text_val : "")};
            case SYNCLITE_VAL_BLOB: {
                Blob b;
                if (v.blob_ptr && v.blob_len) {
                    b.assign(v.blob_ptr, v.blob_ptr + v.blob_len);
                }
                return Value{std::move(b)};
            }
        }
        return Value{};
    }

private:
    Storage storage_{};

    friend class detail::ParamBuf;
};

namespace detail {

// Holds a list of parameters as both Rust-friendly `Value` and an owning
// stash of strings so the C-side `SyncLiteValue` pointers stay valid for the
// duration of the call.
class ParamBuf {
public:
    template <typename Range>
    explicit ParamBuf(const Range& params) {
        const std::size_t n = static_cast<std::size_t>(std::size(params));
        c_values_.reserve(n);
        for (const Value& v : params) {
            push(v);
        }
    }

    ParamBuf(std::initializer_list<Value> il) {
        c_values_.reserve(il.size());
        for (const Value& v : il) {
            push(v);
        }
    }

    const SyncLiteValue* data() const noexcept {
        return c_values_.empty() ? nullptr : c_values_.data();
    }
    std::size_t size() const noexcept { return c_values_.size(); }

private:
    void push(const Value& v) {
        SyncLiteValue out{};
        std::memset(&out, 0, sizeof(out));
        if (v.is_null()) {
            out.tag = SYNCLITE_VAL_NULL;
        } else if (v.is_int()) {
            out.tag = SYNCLITE_VAL_INT;
            out.int_val = v.as_int();
        } else if (v.is_real()) {
            out.tag = SYNCLITE_VAL_REAL;
            out.real_val = v.as_real();
        } else if (v.is_text()) {
            text_stash_.push_back(v.as_text());
            out.tag = SYNCLITE_VAL_TEXT;
            out.text_val = text_stash_.back().c_str();
        } else if (v.is_blob()) {
            blob_stash_.push_back(v.as_blob());
            out.tag = SYNCLITE_VAL_BLOB;
            out.blob_ptr = blob_stash_.back().data();
            out.blob_len = blob_stash_.back().size();
        }
        c_values_.push_back(out);
    }

    std::vector<SyncLiteValue> c_values_;
    // deque so element addresses stay stable as we push more params.
    std::deque<std::string>    text_stash_;
    std::deque<Blob>           blob_stash_;
};

} // namespace detail

using Row  = std::vector<Value>;
using Rows = std::vector<Row>;

// ---------- DestinationOptions --------------------------------------------

struct DestinationOptions {
    std::string                dst_type;
    std::string                dst_connection_string;
    std::optional<std::string> dst_database;
    std::optional<std::string> dst_schema;
    std::string                dst_sync_mode{"CONSOLIDATION"};
};

// ---------- module fns -----------------------------------------------------

inline void initialize(std::string_view device_type,
                       std::string_view device_name,
                       std::string_view db_path,
                       const std::optional<DestinationOptions>& destination = std::nullopt,
                       const std::optional<std::string>& config_path = std::nullopt) {
    const std::string dt(device_type);
    const std::string dn(device_name);
    const std::string dp(db_path);
    SyncLiteDestination c_dest{};
    const char* db_buf = nullptr;
    const char* sc_buf = nullptr;
    if (destination) {
        c_dest.dst_type              = destination->dst_type.c_str();
        c_dest.dst_connection_string = destination->dst_connection_string.c_str();
        if (destination->dst_database) { db_buf = destination->dst_database->c_str(); }
        if (destination->dst_schema)   { sc_buf = destination->dst_schema->c_str(); }
        c_dest.dst_database  = db_buf;
        c_dest.dst_schema    = sc_buf;
        c_dest.dst_sync_mode = destination->dst_sync_mode.c_str();
    }
    const char* cp = config_path ? config_path->c_str() : nullptr;
    detail::check(::synclite_initialize(dt.c_str(), dn.c_str(), dp.c_str(),
                                        destination ? &c_dest : nullptr, cp),
                  "synclite_initialize failed");
}

inline void await_sync(std::string_view db_path, double timeout_seconds) {
    const std::string s(db_path);
    detail::check(::synclite_await_sync(s.c_str(), timeout_seconds),
                  "synclite_await_sync failed");
}

// ---------- helpers --------------------------------------------------------

namespace detail {

inline Rows take_rows(SyncLiteRows* raw) {
    if (!raw) return {};
    const std::size_t r = ::synclite_rows_count(raw);
    const std::size_t c = ::synclite_rows_cols(raw);
    Rows out;
    out.reserve(r);
    for (std::size_t i = 0; i < r; ++i) {
        Row row;
        row.reserve(c);
        for (std::size_t j = 0; j < c; ++j) {
            const SyncLiteValue* v = ::synclite_rows_cell(raw, i, j);
            row.push_back(v ? Value::from_c(*v) : Value{});
        }
        out.push_back(std::move(row));
    }
    ::synclite_rows_free(raw);
    return out;
}

} // namespace detail

// ---------- Statement / Connection templates ------------------------------
//
// We share one template for sqlite-family and duckdb-family by making the
// C handle type and entry points a trait. Keeps the public C++ API single-
// shape: `synclite::Connection` (sqlite-style) and `synclite::DuckConnection`
// (duckdb-style) — mirroring the Python split.

namespace detail {

struct sqlite_traits {
    using conn_t = ::SyncLiteConnection;
    using stmt_t = ::SyncLiteStatement;
    static conn_t* open(const char* p)            { return ::synclite_connection_open(p); }
    static conn_t* open_with_config(const char* p){ return ::synclite_connection_open_with_config(p); }
    static conn_t* initialize_(const char* p)     { return ::synclite_connection_initialize(p); }
    static conn_t* initialize_with_config(const char* p) { return ::synclite_connection_initialize_with_config(p); }
    static int  execute(conn_t* c, const char* s, const SyncLiteValue* p, size_t n, uint64_t* r)
        { return ::synclite_connection_execute(c, s, p, n, r); }
    static int  query  (conn_t* c, const char* s, const SyncLiteValue* p, size_t n, SyncLiteRows** r)
        { return ::synclite_connection_query(c, s, p, n, r); }
    static stmt_t* prepare(conn_t* c, const char* s){ return ::synclite_connection_prepare(c, s); }
    static int  set_ac (conn_t* c, int v) { return ::synclite_connection_set_auto_commit(c, v); }
    static int  get_ac (conn_t* c)        { return ::synclite_connection_get_auto_commit(c); }
    static int  commit (conn_t* c)        { return ::synclite_connection_commit(c); }
    static int  rollback(conn_t* c)       { return ::synclite_connection_rollback(c); }
    static int  flush  (conn_t* c)        { return ::synclite_connection_flush(c); }
    static int  close  (conn_t* c)        { return ::synclite_connection_close(c); }
    static int  stmt_exec (stmt_t* s, const SyncLiteValue* p, size_t n, uint64_t* r)
        { return ::synclite_stmt_execute(s, p, n, r); }
    static int  stmt_query(stmt_t* s, const SyncLiteValue* p, size_t n, SyncLiteRows** r)
        { return ::synclite_stmt_query(s, p, n, r); }
    static int  stmt_add  (stmt_t* s, const SyncLiteValue* p, size_t n)
        { return ::synclite_stmt_add_batch(s, p, n); }
    static int  stmt_clear(stmt_t* s) { return ::synclite_stmt_clear_batch(s); }
    static int  stmt_batch(stmt_t* s, uint64_t** r, size_t* l)
        { return ::synclite_stmt_execute_batch(s, r, l); }
    static void stmt_free (stmt_t* s) { ::synclite_stmt_free(s); }
};

struct duckdb_traits {
    using conn_t = ::SyncLiteDuckConnection;
    using stmt_t = ::SyncLiteDuckStatement;
    static conn_t* open(const char* p)            { return ::synclite_duckdb_connection_open(p); }
    static conn_t* open_with_config(const char* p){ return ::synclite_duckdb_connection_open_with_config(p); }
    static conn_t* initialize_(const char* p)     { return ::synclite_duckdb_connection_initialize(p); }
    static conn_t* initialize_with_config(const char* p) { return ::synclite_duckdb_connection_initialize_with_config(p); }
    static int  execute(conn_t* c, const char* s, const SyncLiteValue* p, size_t n, uint64_t* r)
        { return ::synclite_duckdb_connection_execute(c, s, p, n, r); }
    static int  query  (conn_t* c, const char* s, const SyncLiteValue* p, size_t n, SyncLiteRows** r)
        { return ::synclite_duckdb_connection_query(c, s, p, n, r); }
    static stmt_t* prepare(conn_t* c, const char* s){ return ::synclite_duckdb_connection_prepare(c, s); }
    static int  set_ac (conn_t* c, int v) { return ::synclite_duckdb_connection_set_auto_commit(c, v); }
    static int  get_ac (conn_t* c)        { return ::synclite_duckdb_connection_get_auto_commit(c); }
    static int  commit (conn_t* c)        { return ::synclite_duckdb_connection_commit(c); }
    static int  rollback(conn_t* c)       { return ::synclite_duckdb_connection_rollback(c); }
    static int  flush  (conn_t* c)        { return ::synclite_duckdb_connection_flush(c); }
    static int  close  (conn_t* c)        { return ::synclite_duckdb_connection_close(c); }
    static int  stmt_exec (stmt_t* s, const SyncLiteValue* p, size_t n, uint64_t* r)
        { return ::synclite_duckdb_stmt_execute(s, p, n, r); }
    static int  stmt_query(stmt_t* s, const SyncLiteValue* p, size_t n, SyncLiteRows** r)
        { return ::synclite_duckdb_stmt_query(s, p, n, r); }
    static int  stmt_add  (stmt_t* s, const SyncLiteValue* p, size_t n)
        { return ::synclite_duckdb_stmt_add_batch(s, p, n); }
    static int  stmt_clear(stmt_t* s) { return ::synclite_duckdb_stmt_clear_batch(s); }
    static int  stmt_batch(stmt_t* s, uint64_t** r, size_t* l)
        { return ::synclite_duckdb_stmt_execute_batch(s, r, l); }
    static void stmt_free (stmt_t* s) { ::synclite_duckdb_stmt_free(s); }
};

} // namespace detail

template <typename Traits>
class BasicStatement {
public:
    BasicStatement(const BasicStatement&)            = delete;
    BasicStatement& operator=(const BasicStatement&) = delete;

    BasicStatement(BasicStatement&& o) noexcept : stmt_(o.stmt_) { o.stmt_ = nullptr; }
    BasicStatement& operator=(BasicStatement&& o) noexcept {
        if (this != &o) {
            destroy();
            stmt_ = o.stmt_;
            o.stmt_ = nullptr;
        }
        return *this;
    }
    ~BasicStatement() { destroy(); }

    std::uint64_t execute(std::initializer_list<Value> params = {}) {
        detail::ParamBuf buf(params);
        std::uint64_t rows = 0;
        detail::check(Traits::stmt_exec(stmt_, buf.data(), buf.size(), &rows),
                      "statement execute failed");
        return rows;
    }

    Rows query(std::initializer_list<Value> params = {}) {
        detail::ParamBuf buf(params);
        SyncLiteRows* out = nullptr;
        detail::check(Traits::stmt_query(stmt_, buf.data(), buf.size(), &out),
                      "statement query failed");
        return detail::take_rows(out);
    }

    void add_batch(std::initializer_list<Value> params) {
        detail::ParamBuf buf(params);
        detail::check(Traits::stmt_add(stmt_, buf.data(), buf.size()),
                      "statement add_batch failed");
    }

    void clear_batch() {
        detail::check(Traits::stmt_clear(stmt_), "statement clear_batch failed");
    }

    std::vector<std::uint64_t> execute_batch() {
        std::uint64_t* arr = nullptr;
        std::size_t    len = 0;
        detail::check(Traits::stmt_batch(stmt_, &arr, &len),
                      "statement execute_batch failed");
        std::vector<std::uint64_t> out;
        if (arr) {
            out.assign(arr, arr + len);
            ::synclite_free_u64_array(arr, len);
        }
        return out;
    }

private:
    template <typename> friend class BasicConnection;

    explicit BasicStatement(typename Traits::stmt_t* s) : stmt_(s) {}
    void destroy() {
        if (stmt_) {
            Traits::stmt_free(stmt_);
            stmt_ = nullptr;
        }
    }

    typename Traits::stmt_t* stmt_ = nullptr;
};

template <typename Traits>
class BasicConnection {
public:
    BasicConnection(const BasicConnection&)            = delete;
    BasicConnection& operator=(const BasicConnection&) = delete;

    BasicConnection(BasicConnection&& o) noexcept : conn_(o.conn_) { o.conn_ = nullptr; }
    BasicConnection& operator=(BasicConnection&& o) noexcept {
        if (this != &o) {
            close_silent();
            conn_ = o.conn_;
            o.conn_ = nullptr;
        }
        return *this;
    }
    ~BasicConnection() { close_silent(); }

    static BasicConnection open(const std::string& db_path) {
        auto* h = Traits::open(db_path.c_str());
        if (!h) detail::throw_last("connection open failed");
        return BasicConnection(h);
    }
    static BasicConnection open_with_config(const std::string& conf_path) {
        auto* h = Traits::open_with_config(conf_path.c_str());
        if (!h) detail::throw_last("connection open_with_config failed");
        return BasicConnection(h);
    }
    static BasicConnection initialize(const std::string& db_path) {
        auto* h = Traits::initialize_(db_path.c_str());
        if (!h) detail::throw_last("connection initialize failed");
        return BasicConnection(h);
    }
    static BasicConnection initialize_with_config(const std::string& conf_path) {
        auto* h = Traits::initialize_with_config(conf_path.c_str());
        if (!h) detail::throw_last("connection initialize_with_config failed");
        return BasicConnection(h);
    }

    std::uint64_t execute(const std::string& sql,
                          std::initializer_list<Value> params = {}) {
        detail::ParamBuf buf(params);
        std::uint64_t rows = 0;
        detail::check(Traits::execute(conn_, sql.c_str(), buf.data(), buf.size(), &rows),
                      "connection execute failed");
        return rows;
    }

    Rows query(const std::string& sql,
               std::initializer_list<Value> params = {}) {
        detail::ParamBuf buf(params);
        SyncLiteRows* out = nullptr;
        detail::check(Traits::query(conn_, sql.c_str(), buf.data(), buf.size(), &out),
                      "connection query failed");
        return detail::take_rows(out);
    }

    BasicStatement<Traits> prepare(const std::string& sql) {
        auto* s = Traits::prepare(conn_, sql.c_str());
        if (!s) detail::throw_last("connection prepare failed");
        return BasicStatement<Traits>(s);
    }

    void set_auto_commit(bool v) {
        detail::check(Traits::set_ac(conn_, v ? 1 : 0), "set_auto_commit failed");
    }
    bool get_auto_commit() {
        int rc = Traits::get_ac(conn_);
        if (rc < 0) detail::throw_last("get_auto_commit failed");
        return rc != 0;
    }
    void commit()   { detail::check(Traits::commit(conn_),   "commit failed"); }
    void rollback() { detail::check(Traits::rollback(conn_), "rollback failed"); }
    void flush()    { detail::check(Traits::flush(conn_),    "flush failed"); }

    void close() {
        if (conn_) {
            int rc = Traits::close(conn_);
            conn_ = nullptr;
            if (rc != 0) detail::throw_last("connection close failed");
        }
    }

private:
    explicit BasicConnection(typename Traits::conn_t* c) : conn_(c) {}
    void close_silent() {
        if (conn_) {
            Traits::close(conn_);
            conn_ = nullptr;
        }
    }

    typename Traits::conn_t* conn_ = nullptr;
};

using Connection     = BasicConnection<detail::sqlite_traits>;
using Statement      = BasicStatement <detail::sqlite_traits>;
using DuckConnection = BasicConnection<detail::duckdb_traits>;
using DuckStatement  = BasicStatement <detail::duckdb_traits>;

} // namespace synclite

#endif // SYNCLITE_HPP
