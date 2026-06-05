// C++ mirror of `synclite_duckdb.rs` / `synclite_duckdb.py`.
//
// Uses the DuckDB-backed connection. Same shape as the SQLite sample,
// just `synclite::DuckConnection` instead of `synclite::Connection`.

#include "synclite.hpp"

#include <cstdio>
#include <iostream>

namespace sl = synclite;

static const char* DB_PATH     = "sample_duckdb.duckdb";
static const char* DEVICE_NAME = "sampledevice";

static void print_row(const sl::Row& row) {
    std::cout << "(";
    for (std::size_t i = 0; i < row.size(); ++i) {
        const sl::Value& v = row[i];
        if (i) std::cout << ", ";
        if      (v.is_null()) std::cout << "NULL";
        else if (v.is_int())  std::cout << v.as_int();
        else if (v.is_real()) std::cout << v.as_real();
        else if (v.is_text()) std::cout << "'" << v.as_text() << "'";
        else                  std::cout << "<blob:" << v.as_blob().size() << ">";
    }
    std::cout << ")\n";
}

int main() {
    try {
        sl::DestinationOptions dst;
        dst.dst_type              = "POSTGRES";
        dst.dst_connection_string = "postgresql://postgres:postgres@localhost:5432/syncdb";
        dst.dst_database          = "syncdb";
        dst.dst_schema            = "syncschema";
        dst.dst_sync_mode         = "CONSOLIDATION";

        sl::initialize("DUCKDB", DEVICE_NAME, DB_PATH, dst);

        auto conn = sl::DuckConnection::open(DB_PATH);

        conn.execute("DROP TABLE IF EXISTS users");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users("
            " id INTEGER PRIMARY KEY, name TEXT, score INTEGER)");

        {
            auto stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)");
            stmt.execute({1, "Alice", 100});
            stmt.execute({2, "Bob",   200});
        }

        conn.execute("UPDATE users SET score = ? WHERE name = ?", {250, "Bob"});
        conn.commit();

        {
            auto stmt = conn.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)");
            stmt.add_batch({3, "Carol", 300});
            stmt.add_batch({4, "Dave",  400});
            stmt.execute_batch();
        }
        conn.commit();

        for (auto& row : conn.query("SELECT id, name, score FROM users ORDER BY id")) {
            print_row(row);
        }

        conn.flush();
        sl::await_sync(DB_PATH, 30.0);

        conn.close();
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ERROR: %s\n", e.what());
        return 1;
    }
}
