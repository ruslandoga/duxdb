Experiments with DuckDB in Elixir.

The current API tries to map 1-to-1 to DuckDB C API. This is very verbose and low-level but it enables more idiomatic libraries built on top without any performance or feature sacrifice. For an example of such library, please see [DuxDB.Ecto.](https://github.com/ruslandoga/duxdb_ecto)

Here's an example of how it could (would, once it's done) be used.

```elixir
Mix.install([
  {:duxdb, github: "ruslandoga/duxdb"},
  {:s3, github: "ruslandoga/s3"}
])

config = DuxDB.create_config()
:ok = DuxDB.set_config(config, "max_memory", "1GB")

db = DuxDB.open_ext(":memory:", config)
conn = DuxDB.connect(db)

DuxDB.query(conn, """
COPY (
  SELECT 1
) TO 'heartbeats-1.parquet.lz4' (
  FORMAT PARQUET, COMPRESSION LZ4_RAW
)
""")

stmt = DuxDB.prepare(conn, "SELECT time, next, project FROM read_parquet(?)")
DuxDB.bind_varchar(stmt, 1, "heartbeats-1.parquet.lz4")

result = DuxDB.execute_prepared(stmt)
DuxDB.fetch_chunk(result)
```

For more examples, please see doctests and tests.
