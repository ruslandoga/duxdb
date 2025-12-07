Experiments with DuckDB in Elixir.

> [!NOTE]
>
> The current API tries to map 1-to-1 to DuckDB C API.
> This is very verbose and low-level but it enables more idiomatic libraries to be built on top of this one without any performance or feature sacrifice.
> 
> For an example of a higher level library, please see [DuxDB.Ecto.](https://github.com/ruslandoga/duxdb_ecto)

Here's how it can be used on its own:

```sh
brew install duckdb

duckdb --version
# v1.4.2 (Andium) 68d7555f68

brew --prefix duckdb
# /opt/homebrew/opt/duckdb
```

```elixir
Mix.install([{:duxdb, github: "ruslandoga/duxdb"}],
  force: true,
  system_env: [
    DUXDB_CFLAGS: "-I/opt/homebrew/opt/duckdb/include",
    DUXDB_LDFLAGS: "-L/opt/homebrew/opt/duckdb/lib"
  ]
)

db = DuxDB.open(":memory:", %{"max_memory" => "1GB"})
conn = DuxDB.connect(db)

stmt =
  DuxDB.prepare(conn, """
  SELECT * FROM (VALUES
    (112, 'Hello, ' || $database || '!', today(), -1.0),
    (105, 'Insert a lot of rows per batch', today() - 1, 1.41421),
    (101, 'Sort your data based on your commonly-used queries', today() + 1, 2.718),
    (115, 'Granules are the smallest chunks of data read', $day, $pi)
  ) AS my_first_table(user_id, message, timestamp, metric)
  """)

DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "database"), "ClickHouse")
DuxDB.bind_double(stmt, DuxDB.bind_parameter_index(stmt, "pi"), 3.14159)
DuxDB.bind_date(stmt, DuxDB.bind_parameter_index(stmt, "day"), ~D[2025-03-14])

result = DuxDB.execute_prepared(stmt)

read_chunks =
  fn result ->
    Stream.repeatedly(fn -> DuxDB.fetch_chunk(result) end)
    |> Stream.take_while(&is_reference/1)
    |> Enum.map(fn chunk ->
      Map.new(0..(DuxDB.column_count(result) - 1), fn i ->
        {DuxDB.column_name(result, i), DuxDB.data_chunk_get_vector(chunk, i)}
      end)
    end)
  end

read_chunks.(result)
# [
#   %{
#     "message" => [
#       "Hello, ClickHouse!",
#       "Insert a lot of rows per batch",
#       "Sort your data based on your commonly-used queries",
#       "Granules are the smallest chunks of data read"
#     ],
#     "metric" => [-1.0, 1.41421, 2.718, 3.14159],
#     "timestamp" => [~D[2025-12-07], ~D[2025-12-06], ~D[2025-12-08], ~D[2025-03-14]],
#     "user_id" => ~c"pies"
#   }
# ]
```

For more examples, please see doctests and tests.
