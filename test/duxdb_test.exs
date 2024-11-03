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

    test "TIME", %{query: query} do
      assert query.("select TIME '12:34:56'") == ~T[12:34:56.000000]
      assert query.("select TIME '12:34:56.123'") == ~T[12:34:56.123000]
      assert query.("select TIME '12:34:56.123456'") == ~T[12:34:56.123456]
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
              i64 <- integer(),
              u64 <- non_negative_integer(),
              f64 <- float()
            ) do
        date = Date.add(Date.utc_today(), i64)
        time = Time.add(Time.utc_now(), i64)
        timestamp = NaiveDateTime.add(NaiveDateTime.utc_now(), i64)

        DuxDB.bind_boolean(stmt, DuxDB.bind_parameter_index(stmt, "bool"), bool)
        DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "text"), text)
        DuxDB.bind_blob(stmt, DuxDB.bind_parameter_index(stmt, "blob"), blob)
        DuxDB.bind_int64(stmt, DuxDB.bind_parameter_index(stmt, "i64"), i64)
        DuxDB.bind_uint64(stmt, DuxDB.bind_parameter_index(stmt, "u64"), u64)
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
                   "timestamp" => [timestamp]
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
end
