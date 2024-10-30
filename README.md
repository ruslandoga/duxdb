Experiments with DuckDB in Elixir.

The current API tries to map 1-to-1 to DuckDB C API. This is very verbose and low-level but it enables more idiomatic libraries to be built on top of this one without any performance or feature sacrifice. For an example of a higher level library, please see [DuxDB.Ecto.](https://github.com/ruslandoga/duxdb_ecto)

Here's an example of how it can be used on its own:

```elixir
Mix.install([
  {:duxdb, github: "ruslandoga/duxdb"}
])

config = DuxDB.create_config()
DuxDB.set_config(config, "max_memory", "1GB")

db = DuxDB.open_ext(":memory:", config)
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

result = DuxDB.execute_prepared(stmt)

chunks =
  Stream.repeatedly(fn -> DuxDB.fetch_chunk(result) end)
  |> Stream.take_while(&is_reference/1)
  |> Enum.map(fn chunk ->
    Map.new(0..DuxDB.column_count(result) - 1, fn i ->
      {DuxDB.column_name(result, i), DuxDB.data_chunk_get_vector(chunk, i)}
    end)
  end)

[
  %{
    "message" => ["Hello, ClickHouse!", "Insert a lot of rows per batch",
     "Sort your data based on your commonly-used queries",
     "Granules are the smallest chunks of data read"],
    "metric" => [-1.0, 1.41421, 2.718, 3.14159],
    "timestamp" => [~D[2024-10-29], ~D[2024-10-28], ~D[2024-10-30],
     ~D[2025-03-14]],
    "user_id" => [101, 102, 102, 101]
  }
] = chunks
```

For more examples, please see doctests and tests.
