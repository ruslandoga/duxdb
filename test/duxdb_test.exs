defmodule DuxDBTest do
  use ExUnit.Case
  use ExUnitProperties

  doctest DuxDB
  doctest DuxDB.Error

  describe "open/2" do
    test "fails on broken config" do
      assert_raise ArgumentError,
                   "Invalid Input Error: The following options were not recognized: sdf",
                   fn -> DuxDB.open(":memory:", %{"sdf" => "asdf"}) end
    end

    test "fails on invalid path" do
      assert_raise ArgumentError,
                   "IO Error: Cannot open file \"tmp/somewhere/test.db\": No such file or directory",
                   fn -> DuxDB.open("tmp/somewhere/test.db", []) end
    end
  end

  describe "close/1" do
    setup do
      {:ok, db: DuxDB.open()}
    end

    test "is no-op if already closed", %{db: db} do
      assert :ok == DuxDB.close(db)
      assert :ok == DuxDB.close(db)
    end

    test "fails future operations", %{db: db} do
      assert :ok == DuxDB.close(db)
      assert_raise ArgumentError, fn -> DuxDB.connect(db) end
    end
  end

  describe "disconnect/1" do
    setup do
      {:ok, conn: DuxDB.connect(DuxDB.open())}
    end

    test "is no-op if already disconnected", %{conn: conn} do
      assert :ok == DuxDB.disconnect(conn)
      assert :ok == DuxDB.disconnect(conn)
    end
  end

  test "reading NULL mask" do
    assert DuxDB.open()
           |> DuxDB.connect()
           |> DuxDB.query(
             "SELECT CASE WHEN i%2=0 THEN NULL ELSE i END res_col FROM range(10) t(i)"
           )
           |> DuxDB.fetch_chunk()
           |> DuxDB.data_chunk_get_vector(0) == [nil, 1, nil, 3, nil, 5, nil, 7, nil, 9]
  end

  test "reading string vector" do
    assert DuxDB.open()
           |> DuxDB.connect()
           |> DuxDB.query(
             "SELECT CASE WHEN i%2=0 THEN CONCAT('short_', i) ELSE CONCAT('longstringprefix', i) END FROM range(10) t(i)"
           )
           |> DuxDB.fetch_chunk()
           |> DuxDB.data_chunk_get_vector(0) == [
             "short_0",
             "longstringprefix1",
             "short_2",
             "longstringprefix3",
             "short_4",
             "longstringprefix5",
             "short_6",
             "longstringprefix7",
             "short_8",
             "longstringprefix9"
           ]
  end

  test "new API functions" do
    conn = DuxDB.connect(DuxDB.open())
    
    # Test vector_size
    vector_size = DuxDB.vector_size()
    assert is_integer(vector_size) and vector_size > 0
    
    # Test with a simple query
    result = DuxDB.query(conn, "SELECT 42::INTEGER AS answer, 'hello'::VARCHAR AS greeting")
    
    # Test column_type
    type0 = DuxDB.column_type(result, 0)
    type1 = DuxDB.column_type(result, 1)
    assert type0 == 4  # DUCKDB_TYPE_INTEGER
    assert type1 == 13  # DUCKDB_TYPE_VARCHAR
    
    # Test row_count
    rows = DuxDB.row_count(result)
    assert rows == 1
    
    # Test data chunk functions
    chunk = DuxDB.fetch_chunk(result)
    assert chunk != nil
    chunk_size = DuxDB.data_chunk_get_size(chunk)
    assert chunk_size == 1
    
    # Test with multiple rows
    result2 = DuxDB.query(conn, "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
    rows2 = DuxDB.row_count(result2)
    assert rows2 == 3
    
    chunk2 = DuxDB.fetch_chunk(result2)
    assert chunk2 != nil
    chunk2_size = DuxDB.data_chunk_get_size(chunk2)
    assert chunk2_size == 3
  end

  test "appender API functions" do
    conn = DuxDB.connect(DuxDB.open())
    
    # Create a test table
    DuxDB.query(conn, "CREATE TABLE test_appender (id INTEGER, name VARCHAR, score DOUBLE)")
    
    # Create an appender
    appender = DuxDB.appender_create(conn, nil, "test_appender")
    assert is_reference(appender)
    
    # Insert some data using the appender
    for i <- 1..3 do
      DuxDB.appender_begin_row(appender)
      DuxDB.append_int32(appender, i)
      DuxDB.append_varchar(appender, "name#{i}")
      DuxDB.append_double(appender, i * 1.5)
      DuxDB.appender_end_row(appender)
    end
    
    # Insert a row with NULL
    DuxDB.appender_begin_row(appender)
    DuxDB.append_int32(appender, 4)
    DuxDB.append_null(appender)  # NULL name
    DuxDB.append_double(appender, 4.5)
    DuxDB.appender_end_row(appender)
    
    # Close and destroy the appender
    DuxDB.appender_close(appender)
    DuxDB.appender_destroy(appender)
    
    # Verify the data was inserted correctly
    result = DuxDB.query(conn, "SELECT * FROM test_appender ORDER BY id")
    rows = DuxDB.row_count(result)
    assert rows == 4
    
    # Test the data content
    chunk = DuxDB.fetch_chunk(result)
    
    ids = DuxDB.data_chunk_get_vector(chunk, 0)
    names = DuxDB.data_chunk_get_vector(chunk, 1)
    scores = DuxDB.data_chunk_get_vector(chunk, 2)
    
    assert ids == [1, 2, 3, 4]
    assert names == ["name1", "name2", "name3", nil]
    assert scores == [1.5, 3.0, 4.5, 4.5]
  end

  test "logical type functions" do
    conn = DuxDB.connect(DuxDB.open())
    
    # Test with different column types
    result = DuxDB.query(conn, "SELECT 42::INTEGER AS int_col, 'hello'::VARCHAR AS str_col, 3.14::DOUBLE AS float_col")
    
    # Test logical type extraction
    logical_type0 = DuxDB.column_logical_type(result, 0)
    logical_type1 = DuxDB.column_logical_type(result, 1)
    logical_type2 = DuxDB.column_logical_type(result, 2)
    
    assert is_reference(logical_type0)
    assert is_reference(logical_type1)
    assert is_reference(logical_type2)
    
    # Test type ID extraction
    type_id0 = DuxDB.get_type_id(logical_type0)
    type_id1 = DuxDB.get_type_id(logical_type1)
    type_id2 = DuxDB.get_type_id(logical_type2)
    
    assert type_id0 == 4   # DUCKDB_TYPE_INTEGER
    assert type_id1 == 13  # DUCKDB_TYPE_VARCHAR
    assert type_id2 == 9   # DUCKDB_TYPE_DOUBLE
    
    # These should match the simple column_type function
    assert DuxDB.column_type(result, 0) == type_id0
    assert DuxDB.column_type(result, 1) == type_id1
    assert DuxDB.column_type(result, 2) == type_id2
    
    # Clean up
    DuxDB.destroy_logical_type(logical_type0)
    DuxDB.destroy_logical_type(logical_type1)
    DuxDB.destroy_logical_type(logical_type2)
  end

  describe "DUCKDB_TYPE" do
    setup do
      conn = DuxDB.connect(DuxDB.open())

      query = fn sql ->
        result = DuxDB.query(conn, sql)

        assert [value] =
                 result
                 |> DuxDB.fetch_chunk()
                 |> DuxDB.data_chunk_get_vector(0)

        refute DuxDB.fetch_chunk(result)

        value
      end

      {:ok, query: query}
    end

    test "DATE", %{query: query} do
      assert query.("select DATE '0000-01-01'") == ~D[0000-01-01]
      assert query.("select DATE '1970-01-01'") == ~D[1970-01-01]
      assert query.("select DATE '2024-01-01'") == ~D[2024-01-01]
    end

    property "DATE", %{query: query} do
      check all(d <- date()) do
        assert query.("select DATE '#{d}'") == d
      end
    end

    test "TIME", %{query: query} do
      assert query.("select TIME '12:34:56'") == ~T[12:34:56.000000]
      assert query.("select TIME '12:34:56.123'") == ~T[12:34:56.123000]
      assert query.("select TIME '12:34:56.123456'") == ~T[12:34:56.123456]
    end

    property "TIME", %{query: query} do
      check all(t <- time()) do
        assert query.("select TIME '#{t}'") == t
      end
    end

    test "TIMESTAMP", %{query: query} do
      assert query.("select TIMESTAMP '2024-01-01 12:00:00'") ==
               ~N[2024-01-01 12:00:00.000000]

      assert query.("select TIMESTAMP '2024-01-01 12:00:00.123'") ==
               ~N[2024-01-01 12:00:00.123000]

      assert query.("select TIMESTAMP '2024-01-01 12:00:00.123456'") ==
               ~N[2024-01-01 12:00:00.123456]

      assert query.("select TIMESTAMP '2024-01-01 12:00:00.123456789'") ==
               ~N[2024-01-01 12:00:00.123456]
    end

    property "TIMESTAMP", %{query: query} do
      check all(t <- naive_datetime()) do
        assert query.("select TIMESTAMP '#{t}'") == t
      end
    end

    test "INTERVAL", %{query: query} do
      assert query.("select interval '1 day'") == Duration.new!(day: 1, microsecond: {0, 6})
      assert query.("select interval '2 months'") == Duration.new!(month: 2, microsecond: {0, 6})
      assert query.("select interval '1 year'") == Duration.new!(month: 12, microsecond: {0, 6})
      assert query.("select interval '3 weeks'") == Duration.new!(day: 21, microsecond: {0, 6})

      assert query.("select interval '1 hour'") ==
               Duration.new!(second: 3600, microsecond: {0, 6})

      assert query.("select interval '1 minute'") ==
               Duration.new!(second: 60, microsecond: {0, 6})

      assert query.("select interval '1 second'") == Duration.new!(second: 1, microsecond: {0, 6})
      assert query.("select interval '1 millisecond'") == Duration.new!(microsecond: {1000, 6})
      assert query.("select interval '123 microseconds'") == Duration.new!(microsecond: {123, 6})

      assert query.("select to_seconds(2000) ") ==
               Duration.new!(second: 2000, microsecond: {0, 6})

      assert query.(
               "select interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 789 milliseconds'"
             ) ==
               Duration.new!(
                 month: 14,
                 day: 3,
                 second: 14706,
                 microsecond: {789_000, 6}
               )

      assert query.("select interval '-1 day -2 hours -3 minutes'") ==
               Duration.new!(day: -1, second: -7380, microsecond: {0, 6})

      assert query.("select interval '0' seconds") == Duration.new!(microsecond: {0, 6})
    end

    property "HUGEINT", %{query: query} do
      check all(i <- integer()) do
        assert query.("select HUGEINT '#{i}'") == i
      end

      check all(i16 <- integer(-32768..32767)) do
        assert query.("select HUGEINT '#{i16}'") == i16
      end

      check all(i32 <- integer(-2_147_483_648..2_147_483_647)) do
        assert query.("select HUGEINT '#{i32}'") == i32
      end

      check all(i64 <- integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807)) do
        assert query.("select HUGEINT '#{i64}'") == i64
      end

      check all(
              i128 <-
                integer(-0x80000000000000000000000000000000..0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
            ) do
        assert query.("select HUGEINT '#{i128}'") == i128
      end

      check all(upper <- integer(), lower <- integer()) do
        i = Bitwise.bsl(upper, 64) + lower
        assert query.("select HUGEINT '#{i}'") == i
      end
    end
  end

  describe "bind and fetch" do
    setup do
      {:ok, conn: DuxDB.connect(DuxDB.open())}
    end

    property "all supported data types", %{conn: conn} do
      stmt =
        DuxDB.prepare(
          conn,
          """
          select
            $date as date,
            $time as time,
            $timestamp as timestamp,
            $bool as bool,
            $text as text,
            $blob as blob,
            $i64 as i64,
            $i128 as i128,
            $u128 as u128,
            $u64 as u64,
            $f64 as f64,
            $null as null
          ;
          """
        )

      check all(
              bool <- boolean(),
              text <- string(:printable),
              blob <- binary(),
              i64 <- integer(-0x8000000000000000..0x7FFFFFFFFFFFFFFF),
              u64 <- integer(0..0xFFFFFFFFFFFFFFFF),
              f64 <- float(),
              date <- date(),
              time <- time(),
              timestamp <- naive_datetime(),
              hugeint <-
                integer(-0x80000000000000000000000000000000..0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF),
              uhugeint <- integer(0..0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
            ) do
        DuxDB.bind_boolean(stmt, DuxDB.bind_parameter_index(stmt, "bool"), bool)
        DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "text"), text)
        DuxDB.bind_blob(stmt, DuxDB.bind_parameter_index(stmt, "blob"), blob)
        DuxDB.bind_int64(stmt, DuxDB.bind_parameter_index(stmt, "i64"), i64)
        DuxDB.bind_hugeint(stmt, DuxDB.bind_parameter_index(stmt, "i128"), hugeint)
        DuxDB.bind_uint64(stmt, DuxDB.bind_parameter_index(stmt, "u64"), u64)
        DuxDB.bind_uhugeint(stmt, DuxDB.bind_parameter_index(stmt, "u128"), uhugeint)
        DuxDB.bind_double(stmt, DuxDB.bind_parameter_index(stmt, "f64"), f64)
        DuxDB.bind_null(stmt, DuxDB.bind_parameter_index(stmt, "null"))
        DuxDB.bind_date(stmt, DuxDB.bind_parameter_index(stmt, "date"), date)
        DuxDB.bind_time(stmt, DuxDB.bind_parameter_index(stmt, "time"), time)
        DuxDB.bind_timestamp(stmt, DuxDB.bind_parameter_index(stmt, "timestamp"), timestamp)

        assert fetch_chunks(DuxDB.execute_prepared(stmt)) == [
                 %{
                   "blob" => [blob],
                   "bool" => [bool],
                   "f64" => [f64],
                   "i64" => [i64],
                   "null" => [nil],
                   "text" => [text],
                   "u64" => [u64],
                   "date" => [date],
                   "time" => [time],
                   "timestamp" => [timestamp],
                   "i128" => [hugeint],
                   "u128" => [uhugeint]
                 }
               ]
      end
    end
  end

  defp fetch_chunks(result) do
    case DuxDB.fetch_chunk(result) do
      nil -> []
      chunk -> [fetch_vectors(chunk, result) | fetch_chunks(result)]
    end
  end

  defp fetch_vectors(chunk, result) do
    Map.new(0..(DuxDB.data_chunk_get_column_count(chunk) - 1), fn idx ->
      {DuxDB.column_name(result, idx), DuxDB.data_chunk_get_vector(chunk, idx)}
    end)
  end

  defp time do
    gen all(
          hour <- StreamData.integer(0..23),
          minute <- StreamData.integer(0..59),
          second <- StreamData.integer(0..59),
          micros <- StreamData.integer(0..999_999)
        ) do
      Time.new!(hour, minute, second, {micros, 6})
    end
  end

  defp date do
    gen all(
          year <- StreamData.integer(0..9999),
          month <- StreamData.integer(1..12),
          day <- StreamData.integer(1..28)
        ) do
      Date.new!(year, month, day)
    end
  end

  defp naive_datetime do
    gen all(
          date <- date(),
          time <- time()
        ) do
      NaiveDateTime.new!(date, time)
    end
  end
end
