defmodule DuxDB.ParquetTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: DuxDB.connect(DuxDB.open())}
  end

  @tag :tmp_dir
  test "writes and reads a Parquet file", %{conn: conn, tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "events.parquet")

    DuxDB.query(
      conn,
      """
      COPY (
        SELECT *
        FROM (VALUES
          (1, 'created', DATE '2026-07-24', true),
          (2, 'updated', DATE '2026-07-25', false),
          (3, NULL, DATE '2026-07-26', true)
        ) events(id, name, happened_on, active)
      ) TO '#{path}' (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        PARQUET_VERSION V2
      )
      """
    )

    assert File.exists?(path)

    stmt =
      DuxDB.prepare(
        conn,
        """
        SELECT id, name, happened_on, active
        FROM read_parquet($path)
        ORDER BY id
        """
      )

    DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "path"), path)
    result = DuxDB.execute_prepared(stmt)

    assert fetch_rows(result) == [
             [1, "created", ~D[2026-07-24], true],
             [2, "updated", ~D[2026-07-25], false],
             [3, nil, ~D[2026-07-26], true]
           ]
  end

  @tag :tmp_dir
  test "reads multiple Parquet files with a glob", %{conn: conn, tmp_dir: tmp_dir} do
    for {filename, first_id} <- [{"part-1.parquet", 1}, {"part-2.parquet", 3}] do
      path = Path.join(tmp_dir, filename)

      DuxDB.query(
        conn,
        """
        COPY (
          SELECT id, 'item-' || id AS name
          FROM range(#{first_id}, #{first_id + 2}) rows(id)
        ) TO '#{path}' (
          FORMAT PARQUET,
          COMPRESSION ZSTD,
          PARQUET_VERSION V2
        )
        """
      )
    end

    glob = Path.join(tmp_dir, "part-*.parquet")

    stmt =
      DuxDB.prepare(
        conn,
        """
        SELECT id, name
        FROM read_parquet($glob)
        ORDER BY id
        """
      )

    DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "glob"), glob)
    result = DuxDB.execute_prepared(stmt)

    assert fetch_rows(result) == [
             [1, "item-1"],
             [2, "item-2"],
             [3, "item-3"],
             [4, "item-4"]
           ]
  end

  defp fetch_rows(result) do
    Stream.repeatedly(fn -> DuxDB.fetch_chunk(result) end)
    |> Stream.take_while(&is_reference/1)
    |> Enum.flat_map(fn chunk ->
      columns =
        for idx <- 0..(DuxDB.data_chunk_get_column_count(chunk) - 1) do
          DuxDB.data_chunk_get_vector(chunk, idx)
        end

      Enum.zip_with(columns, & &1)
    end)
  end
end
