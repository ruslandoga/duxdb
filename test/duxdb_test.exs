defmodule DuxDBTest do
  use ExUnit.Case, async: true
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
      {:ok, db: DuxDB.open(":memory:")}
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
      {:ok, conn: DuxDB.connect(DuxDB.open(":memory:"))}
    end

    test "is no-op if already disconnected", %{conn: conn} do
      assert :ok == DuxDB.disconnect(conn)
      assert :ok == DuxDB.disconnect(conn)
    end
  end

  describe "DUCKDB_TYPE" do
    setup do
      conn = DuxDB.connect(DuxDB.open(":memory:"))

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
      {:ok, conn: DuxDB.connect(DuxDB.open(":memory:"))}
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
