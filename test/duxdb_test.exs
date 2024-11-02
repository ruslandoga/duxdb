defmodule DuxDBTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  doctest DuxDB

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

  describe "bind and fetch" do
    setup do
      {:ok, conn: DuxDB.connect(DuxDB.open(":memory:"))}
    end

    property "bool, text, blob, int, float and null", %{conn: conn} do
      stmt =
        DuxDB.prepare(
          conn,
          """
          select
            $bool as bool,
            -- TODO
            -- $text as text,
            $blob as blob,
            $int as int,
            $uint as uint,
            $f64 as f64,
            $null as null
          ;
          """
        )

      check all(bool <- boolean(), bin <- binary(), int <- integer(), f <- float()) do
        DuxDB.bind_boolean(stmt, DuxDB.bind_parameter_index(stmt, "bool"), bool)
        # TODO
        # DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "text"), bin)
        DuxDB.bind_blob(stmt, DuxDB.bind_parameter_index(stmt, "blob"), bin)
        DuxDB.bind_int64(stmt, DuxDB.bind_parameter_index(stmt, "int"), int)
        DuxDB.bind_uint64(stmt, DuxDB.bind_parameter_index(stmt, "uint"), abs(int))
        DuxDB.bind_double(stmt, DuxDB.bind_parameter_index(stmt, "f64"), f)
        DuxDB.bind_null(stmt, DuxDB.bind_parameter_index(stmt, "null"))

        res = DuxDB.execute_prepared(stmt)

        assert fetch_chunks(res) == [
                 %{
                   "blob" => [bin],
                   "bool" => [bool],
                   "f64" => [f],
                   "int" => [int],
                   "null" => [nil],
                   # TODO
                   # "text" => [bin],
                   "uint" => [abs(int)]
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
