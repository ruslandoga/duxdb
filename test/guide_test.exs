defmodule DuxDB.GuideTest do
  use ExUnit.Case, async: true

  # while getting used to a new keyboard, I'm typing out the examples from DuckDB docs as tests
  # these tests would probably be removed in the future

  # questions to answer:
  # - how to incrementally write to parquet files? maybe temp table, appender, then copy
  # - does appender to temp table spill to disk?

  setup do
    db = DuxDB.open()
    on_exit(fn -> DuxDB.close(db) end)

    conn = DuxDB.connect(db)
    on_exit(fn -> DuxDB.disconnect(conn) end)

    {:ok, conn: conn}
  end

  defp query(conn, sql, params \\ []) do
    stmt = DuxDB.prepare(conn, sql)

    try do
      bind_params(stmt, params)
      result = DuxDB.execute_prepared_dirty_io(stmt)

      try do
        read_result(result)
      after
        DuxDB.destroy_result(result)
      end
    after
      DuxDB.destroy_prepare(stmt)
    end
  end

  defp bind_params(stmt, params) do
    Enum.each(params, fn {name, value} ->
      idx = DuxDB.bind_parameter_index(stmt, name)

      case value do
        text when is_binary(text) -> DuxDB.bind_varchar(stmt, idx, text)
        nil -> DuxDB.bind_null(stmt, idx)
      end
    end)
  end

  defp read_result(result) do
    Stream.repeatedly(fn -> DuxDB.fetch_chunk(result) end)
    |> Stream.take_while(&is_reference/1)
    |> Enum.map(fn chunk ->
      Map.new(0..(DuxDB.column_count(result) - 1), fn i ->
        {DuxDB.column_name(result, i), DuxDB.data_chunk_get_vector(chunk, i)}
      end)
    end)
  end

  # https://duckdb.org/docs/data/csv/overview
  describe "CSV" do
    test "import", %{conn: conn} do
      csv_path = "./test/data/flights.csv"

      assert query(conn, "select * from read_csv($csv)", %{"csv" => csv_path}) == [
               %{
                 "DestCityName" => ["Los Angeles, CA", "Los Angeles, CA", "Los Angeles, CA"],
                 "FlightDate" => [~D[1988-01-01], ~D[1988-01-02], ~D[1988-01-03]],
                 "OriginCityName" => ["New York, NY", "New York, NY", "New York, NY"],
                 "UniqueCarrier" => ["AA", "AA", "AA"]
               }
             ]

      assert query(
               conn,
               """
               select *
               from read_csv($csv,
                 delim = '|',
                 header = true,
                 columns = {
                   'FlightDate': 'DATE',
                   'UniqueCarrier': 'VARCHAR',
                   'OriginCityName': 'VARCHAR',
                   'DestCityName': 'VARCHAR'
                 }
               )
               """,
               %{"csv" => csv_path}
             ) == [
               %{
                 "DestCityName" => ["Los Angeles, CA", "Los Angeles, CA", "Los Angeles, CA"],
                 "FlightDate" => [~D[1988-01-01], ~D[1988-01-02], ~D[1988-01-03]],
                 "OriginCityName" => ["New York, NY", "New York, NY", "New York, NY"],
                 "UniqueCarrier" => ["AA", "AA", "AA"]
               }
             ]

      # TODO SELECT * FROM read_csv('/dev/stdin')

      assert query(conn, """
             create table ontime (
               FlightDate DATE,
               UniqueCarrier VARCHAR,
               OriginCityName VARCHAR,
               DestCityName VARCHAR
             )
             """) == []

      # TODO copy doesn't allow params?
      assert query(conn, "copy ontime from '#{csv_path}'") == [%{"Count" => [3]}]

      assert query(conn, "select * from ontime") == [
               %{
                 "DestCityName" => ["Los Angeles, CA", "Los Angeles, CA", "Los Angeles, CA"],
                 "FlightDate" => [~D[1988-01-01], ~D[1988-01-02], ~D[1988-01-03]],
                 "OriginCityName" => ["New York, NY", "New York, NY", "New York, NY"],
                 "UniqueCarrier" => ["AA", "AA", "AA"]
               }
             ]

      assert query(conn, "drop table ontime")

      assert query(conn, "create table ontime as select * from read_csv($csv)", %{
               "csv" => csv_path
             }) == [%{"Count" => [3]}]

      assert query(conn, "drop table ontime")

      assert query(conn, "create table ontime as from read_csv($csv)", %{"csv" => csv_path}) ==
               [%{"Count" => [3]}]
    end
  end

  describe "JSON" do
    # https://duckdb.org/docs/data/json/overview
    @tag :tmp_dir
    test "overview", %{conn: conn, tmp_dir: tmp_dir} do
      json_path = "./test/data/todos.json"

      assert query(conn, "select * from read_json($json) limit 3", %{"json" => json_path}) == [
               %{
                 "completed" => [false, false, false],
                 "id" => [1, 2, 3],
                 "title" => [
                   "delectus aut autem",
                   "quis ut nam facilis et officia qui",
                   "fugiat veniam minus"
                 ],
                 "userId" => [1, 1, 1]
               }
             ]

      assert query(
               conn,
               """
               select *
               from read_json($json,
                              format = 'array',
                              columns = {userId: 'UBIGINT',
                                         id: 'UBIGINT',
                                         title: 'VARCHAR',
                                         completed: 'BOOLEAN'})
               limit 3
               """,
               %{"json" => json_path}
             ) == [
               %{
                 "completed" => [false, false, false],
                 "id" => [1, 2, 3],
                 "title" => [
                   "delectus aut autem",
                   "quis ut nam facilis et officia qui",
                   "fugiat veniam minus"
                 ],
                 "userId" => [1, 1, 1]
               }
             ]

      # TODO SELECT * FROM read_json('/dev/stdin')

      # TODO https://github.com/duckdb/duckdb-web/issues/4530
      # assert query(
      #          conn,
      #          "create table todos (userId UBIGINT, id UBIGINT, title VARCHAR, completed BOOLEAN)"
      #        ) == []
      # assert query(conn, "copy todos from read_json('#{json_path}')") == nil

      assert query(conn, "create table todos as select * from read_json($json)", %{
               "json" => json_path
             }) == [%{"Count" => [200]}]

      write_json_path = Path.join(tmp_dir, "todos.json")

      assert query(conn, "copy (select * from todos) to '#{write_json_path}'") == [
               %{"Count" => [200]}
             ]

      assert File.stream!(write_json_path) |> Stream.map(&:json.decode/1) |> Enum.take(3) == [
               %{
                 "completed" => false,
                 "id" => 1,
                 "title" => "delectus aut autem",
                 "userId" => 1
               },
               %{
                 "completed" => false,
                 "id" => 2,
                 "title" => "quis ut nam facilis et officia qui",
                 "userId" => 1
               },
               %{
                 "completed" => false,
                 "id" => 3,
                 "title" => "fugiat veniam minus",
                 "userId" => 1
               }
             ]

      assert query(conn, "create table example (j JSON)") == []

      assert query(conn, "insert into example values ($json)", %{
               "json" =>
                 IO.iodata_to_binary(
                   :json.encode(%{
                     "family" => "anatidae",
                     "species" => ["duck", "goose", "swan", nil]
                   })
                 )
             }) == [%{"Count" => [1]}]

      assert query(conn, "select j.family from example") == [%{"family" => ["\"anatidae\""]}]

      assert query(conn, "select j->'$.family' from example") == [
               %{"(j -> '$.family')" => ["\"anatidae\""]}
             ]

      assert query(conn, "select j->>'$.family' from example") == [
               %{"(j ->> '$.family')" => ["anatidae"]}
             ]
    end

    # https://duckdb.org/docs/data/json/creating_json
    test "create", %{conn: conn} do
      assert query(conn, "select to_json($duck)", %{"duck" => "duck"}) ==
               [%{"to_json($duck)" => ["\"duck\""]}]

      # TODO
      # assert query(conn, "select to_json($array)", %{"array" => [1, 2, 3]}) == nil

      assert query(conn, "select to_json([1, 2, 3])") == [
               %{"to_json(main.list_value(1, 2, 3))" => ["[1,2,3]"]}
             ]

      # TODO
      # assert query(conn, "select to_json($map)", %{"map" => %{"duck" => 42}}) == nil

      assert query(conn, "select to_json({duck: 42})") == [
               %{"to_json(main.struct_pack(duck := 42))" => ["{\"duck\":42}"]}
             ]

      assert query(conn, "select to_json(map(['duck'], [42]))") == [
               %{
                 "to_json(\"map\"(main.list_value('duck'), main.list_value(42)))" => [
                   "{\"duck\":42}"
                 ]
               }
             ]

      assert query(conn, "select json_array('duck', 42, 'goose', 123)") == [
               %{"json_array('duck', 42, 'goose', 123)" => ["[\"duck\",42,\"goose\",123]"]}
             ]

      assert query(conn, "select json_object('duck', 42, 'goose', 123)") == [
               %{"json_object('duck', 42, 'goose', 123)" => ["{\"duck\":42,\"goose\":123}"]}
             ]

      assert query(conn, "select json_merge_patch('{\"duck\": 42}', '{\"goose\": 123}')") == [
               %{
                 "json_merge_patch('{\"duck\": 42}', '{\"goose\": 123}')" => [
                   "{\"duck\":42,\"goose\":123}"
                 ]
               }
             ]
    end

    # https://duckdb.org/docs/data/json/loading_json
    @tag :tmp_dir
    test "load", %{conn: conn, tmp_dir: tmp_dir} do
      json_path = "./test/data/todos.json"

      assert query(conn, "select * from read_json($json) limit 5", %{"json" => json_path}) == [
               %{
                 "completed" => [false, false, false, true, false],
                 "id" => [1, 2, 3, 4, 5],
                 "title" => [
                   "delectus aut autem",
                   "quis ut nam facilis et officia qui",
                   "fugiat veniam minus",
                   "et porro tempora",
                   "laboriosam mollitia et enim quasi adipisci quia provident illum"
                 ],
                 "userId" => [1, 1, 1, 1, 1]
               }
             ]

      assert query(conn, "create table todos as select * from read_json($json)", %{
               "json" => json_path
             }) == [%{"Count" => [200]}]

      assert query(conn, "describe todos") == [
               %{
                 "column_name" => ["userId", "id", "title", "completed"],
                 "column_type" => ["BIGINT", "BIGINT", "VARCHAR", "BOOLEAN"],
                 "default" => [nil, nil, nil, nil],
                 "extra" => [nil, nil, nil, nil],
                 "key" => [nil, nil, nil, nil],
                 "null" => ["YES", "YES", "YES", "YES"]
               }
             ]

      assert query(
               conn,
               "select * from read_json($json, columns = {userId: 'UBIGINT', completed: 'BOOLEAN'}) limit 3",
               %{"json" => json_path}
             ) == [%{"completed" => [false, false, false], "userId" => [1, 1, 1]}]

      birds_path = Path.join(tmp_dir, "birds.json")

      File.write!(birds_path, """
      {
        "duck": 42
      }
      {
        "goose": [1, 2, 3]
      }
      """)

      # TODO not the same as docs
      # ┌──────────────────────────────┐
      # │             json             │
      # │             json             │
      # ├──────────────────────────────┤
      # │ {\n    "duck": 42\n}         │
      # │ {\n    "goose": [1, 2, 3]\n} │
      # └──────────────────────────────┘
      # https://github.com/duckdb/duckdb-web/pull/4534
      assert query(
               conn,
               "from read_json($json, format = 'unstructured')",
               %{"json" => birds_path}
             ) == [%{"duck" => [42, nil], "goose" => [nil, [1, 2, 3]]}]

      File.write!(birds_path, """
      {"duck": 42}
      {"goose": [1, 2, 3]}
      """)

      assert query(
               conn,
               "from read_json_objects($json, format = 'newline_delimited')",
               %{"json" => birds_path}
             ) == [%{"json" => ["{\"duck\": 42}", "{\"goose\": [1, 2, 3]}"]}]

      File.write!(birds_path, """
      [
          {
              "duck": 42
          },
          {
              "goose": [1, 2, 3]
          }
      ]
      """)

      assert query(
               conn,
               "from read_json_objects($json, format = 'array')",
               %{"json" => birds_path}
             ) == [
               %{
                 "json" => [
                   "{\n        \"duck\": 42\n    }",
                   "{\n        \"goose\": [1, 2, 3]\n    }"
                 ]
               }
             ]

      File.write!(birds_path, """
      [
        {
          "duck": 42,
          "goose": 4.2
        },
        {
          "duck": 43,
          "goose": 4.3
        }
      ]
      """)

      assert query(conn, "from read_json($json, format = 'array')", %{"json" => birds_path}) ==
               [%{"duck" => [42, 43], "goose" => [4.2, 4.3]}]

      File.write!(birds_path, """
      {
          "duck": 42,
          "goose": 4.2
      }
      {
          "duck": 43,
          "goose": 4.3
      }
      """)

      assert query(conn, "from read_json($json, format = 'unstructured')", %{"json" => birds_path}) ==
               [%{"duck" => [42, 43], "goose" => [4.2, 4.3]}]

      File.write!(birds_path, """
      {"duck": 42, "goose": [1, 2, 3]}
      {"duck": 43, "goose": [4, 5, 6]}
      """)

      # TODO records = false (i.e. support reading structs)
      assert query(conn, "from read_json($json, records = true)", %{
               "json" => birds_path
             }) ==
               [%{"duck" => [42, 43], "goose" => [[1, 2, 3], [4, 5, 6]]}]

      # TODO https://duckdb.org/2023/03/03/json.html

      numbers_path = Path.join(tmp_dir, "numbers.json")

      assert query(conn, "copy (select * from range(5) r(i)) to '#{numbers_path}' (array true)") ==
               [%{"Count" => [5]}]

      assert :json.decode(File.read!(numbers_path)) == [
               %{"i" => 0},
               %{"i" => 1},
               %{"i" => 2},
               %{"i" => 3},
               %{"i" => 4}
             ]

      assert query(conn, "create table numbers as from read_json($json)", %{
               "json" => numbers_path
             }) == [%{"Count" => [5]}]
    end
  end
end
