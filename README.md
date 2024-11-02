Experiments with DuckDB in Elixir.

For an DuckDB Ecto adapter, please see [DuxDB.Ecto.](https://github.com/ruslandoga/duxdb_ecto)

Here's an example of how DuxDB can be used on its own:

```sh
brew install duckdb llvm

duckdb --version
# v1.1.2 f680b7d08f

clang --version
# Homebrew clang version 19.1.2

export CC=$(which clang)
export DUXDB_CFLAGS=-std=c23
export DUXDB_LDFLAGS=-L/opt/homebrew/opt/duckdb/lib
```

```elixir
Mix.install([
  {:duxdb, github: "ruslandoga/duxdb"}
])

db = DuxDB.open(":memory:", %{"max_memory" => "1GB"})
conn = DuxDB.connect(db)

stmt = DuxDB.prepare(conn, """
SELECT * FROM (VALUES
  (101, 'Hello, ' || $database || '!', today(), -1.0),
  (102, 'Insert a lot of rows per batch', today() - 1, 1.41421),
  (102, 'Sort your data based on your commonly-used queries', today() + 1, 2.718),
  (101, 'Granules are the smallest chunks of data read', $day, $pi)
) AS my_first_table(user_id, message, timestamp, metric)
""")

DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "database"), "ClickHouse")
DuxDB.bind_double(stmt, DuxDB.bind_parameter_index(stmt, "pi"), 3.14159)
DuxDB.bind_date(stmt, DuxDB.bind_parameter_index(stmt, "day"), ~D[2025-03-14])

%DuxDB.Result{
  columns: ["user_id", "message", "timestamp", "metric"],
  rows: [
    [101, "Hello, ClickHouse!", ~D[2024-10-29], -1.0],
    [102, "Insert a lot of rows per batch", ~D[2024-10-28], 1.41421],
    [102, "Sort your data based on your commonly-used queries", ~D[2024-10-30], 2.718],
    [101, "Granules are the smallest chunks of data read", ~D[2025-03-14], 3.14159]
  ],
  command: :select,
  num_rows: 4
} = DuxDB.execute_prepared_dirty_cpu(stmt)
```

For more examples, please see doctests and tests.
