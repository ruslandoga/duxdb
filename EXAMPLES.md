# Enhanced DuxDB API Examples

This file demonstrates the new C API functions added to DuxDB.

## Basic Utility Functions

```elixir
# Get the internal vector size used by DuckDB
vector_size = DuxDB.vector_size()
IO.puts("DuckDB vector size: #{vector_size}")

# Query some data
conn = DuxDB.connect(DuxDB.open())
result = DuxDB.query(conn, "SELECT 42::INTEGER, 'hello'::VARCHAR, 3.14::DOUBLE")

# Get column types
for i <- 0..2 do
  type = DuxDB.column_type(result, i)
  IO.puts("Column #{i} type: #{type}")
end

# Get total row count
total_rows = DuxDB.row_count(result)
IO.puts("Total rows: #{total_rows}")

# Get chunk information
chunk = DuxDB.fetch_chunk(result)
chunk_size = DuxDB.data_chunk_get_size(chunk)
IO.puts("Chunk size: #{chunk_size}")
```

## Bulk Insert with Appender API

```elixir
# Create a table for bulk inserts
conn = DuxDB.connect(DuxDB.open())
DuxDB.query(conn, """
  CREATE TABLE employees (
    id INTEGER,
    name VARCHAR,
    salary DOUBLE,
    department VARCHAR
  )
""")

# Create an appender for fast bulk inserts
appender = DuxDB.appender_create(conn, nil, "employees")

# Insert 1000 rows efficiently
for i <- 1..1000 do
  DuxDB.appender_begin_row(appender)
  DuxDB.append_int32(appender, i)
  DuxDB.append_varchar(appender, "Employee #{i}")
  DuxDB.append_double(appender, 50000.0 + :rand.uniform(50000))
  DuxDB.append_varchar(appender, ["Engineering", "Sales", "Marketing"] |> Enum.random())
  DuxDB.appender_end_row(appender)
end

# Finalize the appender
DuxDB.appender_close(appender)
DuxDB.appender_destroy(appender)

# Verify the data
result = DuxDB.query(conn, "SELECT COUNT(*) FROM employees")
count = DuxDB.fetch_chunk(result) |> DuxDB.data_chunk_get_vector(0) |> hd()
IO.puts("Inserted #{count} employees")
```

## Advanced Type Information

```elixir
# Query with various data types
result = DuxDB.query(conn, """
  SELECT 
    42::BIGINT as big_int,
    'text'::VARCHAR as text_col,
    3.14159::DOUBLE as pi,
    true::BOOLEAN as flag,
    '2024-01-01'::DATE as today
""")

# Get detailed type information
for i <- 0..4 do
  # Simple type ID
  type_id = DuxDB.column_type(result, i)
  
  # Detailed logical type information
  logical_type = DuxDB.column_logical_type(result, i)
  logical_type_id = DuxDB.get_type_id(logical_type)
  
  column_name = DuxDB.column_name(result, i)
  
  IO.puts("Column '#{column_name}': type=#{type_id}, logical_type=#{logical_type_id}")
  
  # Clean up logical type
  DuxDB.destroy_logical_type(logical_type)
end
```

## Performance Comparison

The appender API is significantly faster for bulk inserts compared to individual INSERT statements:

```elixir
# Time regular inserts
{time_regular, _} = :timer.tc(fn ->
  for i <- 1..1000 do
    DuxDB.query(conn, "INSERT INTO test_table VALUES (#{i}, 'name#{i}')")
  end
end)

# Time appender inserts  
{time_appender, _} = :timer.tc(fn ->
  appender = DuxDB.appender_create(conn, nil, "test_table")
  for i <- 1..1000 do
    DuxDB.appender_begin_row(appender)
    DuxDB.append_int32(appender, i)
    DuxDB.append_varchar(appender, "name#{i}")
    DuxDB.appender_end_row(appender)
  end
  DuxDB.appender_close(appender)
  DuxDB.appender_destroy(appender)
end)

IO.puts("Regular inserts: #{time_regular/1000}ms")
IO.puts("Appender inserts: #{time_appender/1000}ms")
IO.puts("Speedup: #{Float.round(time_regular/time_appender, 2)}x")
```

The appender API is typically 10-100x faster for bulk data loading operations.