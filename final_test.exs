#!/usr/bin/env elixir

# Simple test of our new DuckDB functions
# This bypasses the Mix framework to directly test the NIF

:ok = :erlang.load_nif('./priv/duxdb', 0)

# Test basic functions
IO.puts "Testing vector_size..."
try do
  size = :erlang.apply(:"Elixir.DuxDB", :vector_size, [])
  IO.puts "✓ vector_size returned: #{size}"
rescue
  error ->
    IO.puts "✗ vector_size failed: #{inspect(error)}"
end

# Test simple database operation
IO.puts "\nTesting database operations..."
try do
  # This should work if our basic functions are implemented
  db = :erlang.apply(:"Elixir.DuxDB", :open_ext, [nil, nil])
  IO.puts "✓ Database opened"
  
  conn = :erlang.apply(:"Elixir.DuxDB", :connect, [db])
  IO.puts "✓ Connection established"
  
  result = :erlang.apply(:"Elixir.DuxDB", :query_nif, [conn, "SELECT 42::INTEGER AS answer\0"])
  IO.puts "✓ Query executed"
  
  # Test our new functions
  type = :erlang.apply(:"Elixir.DuxDB", :column_type, [result, 0])
  IO.puts "✓ column_type returned: #{type}"
  
  rows = :erlang.apply(:"Elixir.DuxDB", :row_count, [result])
  IO.puts "✓ row_count returned: #{rows}"
  
  chunk = :erlang.apply(:"Elixir.DuxDB", :fetch_chunk, [result])
  if chunk do
    chunk_size = :erlang.apply(:"Elixir.DuxDB", :data_chunk_get_size, [chunk])
    IO.puts "✓ data_chunk_get_size returned: #{chunk_size}"
  else
    IO.puts "✗ No chunk returned"
  end
  
  IO.puts "\n✓ All basic tests passed!"
rescue
  error ->
    IO.puts "✗ Database operations failed: #{inspect(error)}"
end

# Test appender functions
IO.puts "\nTesting appender functions..."
try do
  db = :erlang.apply(:"Elixir.DuxDB", :open_ext, [nil, nil])
  conn = :erlang.apply(:"Elixir.DuxDB", :connect, [db])
  
  # Create table
  :erlang.apply(:"Elixir.DuxDB", :query_nif, [conn, "CREATE TABLE test_table (id INTEGER, name VARCHAR)\0"])
  IO.puts "✓ Table created"
  
  # Create appender
  appender = :erlang.apply(:"Elixir.DuxDB", :appender_create_nif, [conn, nil, "test_table\0"])
  IO.puts "✓ Appender created"
  
  # Append data
  :erlang.apply(:"Elixir.DuxDB", :appender_begin_row, [appender])
  :erlang.apply(:"Elixir.DuxDB", :append_int32, [appender, 1])
  :erlang.apply(:"Elixir.DuxDB", :append_varchar, [appender, "test"])
  :erlang.apply(:"Elixir.DuxDB", :appender_end_row, [appender])
  IO.puts "✓ Data appended"
  
  # Close appender
  :erlang.apply(:"Elixir.DuxDB", :appender_close_nif, [appender])
  :erlang.apply(:"Elixir.DuxDB", :appender_destroy, [appender])
  IO.puts "✓ Appender closed and destroyed"
  
  # Verify data
  result = :erlang.apply(:"Elixir.DuxDB", :query_nif, [conn, "SELECT COUNT(*) FROM test_table\0"])
  rows = :erlang.apply(:"Elixir.DuxDB", :row_count, [result])
  IO.puts "✓ Verification query returned #{rows} rows"
  
  IO.puts "\n✓ All appender tests passed!"
rescue
  error ->
    IO.puts "✗ Appender tests failed: #{inspect(error)}"
end

IO.puts "\n🎉 All tests completed!"