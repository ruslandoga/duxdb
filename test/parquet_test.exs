# An executable Parquet guide
#
# This file is meant to be useful in two ways: ExUnit runs it as a regression
# suite, and users can read it from top to bottom as a small Parquet cookbook.
# Each chapter is independent, so the tests remain safe to run asynchronously.
#
# Start with DuckDB's Parquet overview for the complete SQL surface:
# https://duckdb.org/docs/stable/data/parquet/overview
#
# The direct-query examples are also inspired by ClickHouse's technical tour of
# Parquet. It is a particularly good explanation of how projection, filtering,
# aggregation, and parallel file scans fit together:
# https://clickhouse.com/blog/clickhouse-and-parquet-a-foundation-for-fast-lakehouse-analytics
defmodule DuxDB.ParquetTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn: DuxDB.connect(DuxDB.open())}
  end

  describe "chapter 1: writing a Parquet file" do
    @tag :tmp_dir
    test "round-trips typed and nullable data", %{conn: conn, tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "events.parquet")

      # COPY can export a query directly; no intermediate DuckDB table is
      # required. The available writer options are documented here:
      # https://duckdb.org/docs/stable/data/parquet/overview#writing-to-parquet-files
      #
      # Parquet v2 enables newer encodings such as DELTA_BINARY_PACKED and
      # BYTE_STREAM_SPLIT. DuckDB explains their compression benefits here:
      # https://duckdb.org/2025/01/22/parquet-encodings
      #
      # ZSTD is a useful size/speed trade-off. For interoperability with readers
      # that do not support newer encodings, use PARQUET_VERSION V1 instead.
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

      # A table-function argument can be bound instead of interpolated. Besides
      # handling escaping, this lets an application prepare the analytical SQL
      # once and execute it for different files.
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
  end

  describe "chapter 2: querying a Parquet dataset in place" do
    @tag :tmp_dir
    test "filters, aggregates, and sorts across a file glob", %{conn: conn, tmp_dir: tmp_dir} do
      # Real data lakes commonly expose a logical dataset as many Parquet files.
      # DuckDB accepts a glob and scans all matching files as one relation:
      # https://duckdb.org/docs/stable/data/multiple_files/overview
      #
      # This miniature web-analytics query follows the shape of the ClickHouse
      # article's examples: select a useful dimension, filter empty values,
      # aggregate users, and sort the result without ingesting the files first.
      # At production scale, multiple files and row groups also expose more
      # opportunities for parallel work.
      for {filename, rows} <- [
            {"part-1.parquet", "(1, 'Pixel'), (2, 'iPhone'), (3, '')"},
            {"part-2.parquet", "(2, 'Pixel'), (4, 'Pixel'), (5, '')"}
          ] do
        path = Path.join(tmp_dir, filename)

        DuxDB.query(
          conn,
          """
          COPY (
            SELECT *
            FROM (VALUES #{rows}) events(user_id, mobile_phone_model)
          ) TO '#{path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            PARQUET_VERSION V2
          )
          """
        )
      end

      glob = Path.join(tmp_dir, "part-*.parquet")

      # Parquet is columnar and stores statistics, allowing DuckDB to push
      # projections and filters into the scan. This query only needs two source
      # columns and discards empty models before aggregation:
      # https://duckdb.org/docs/stable/data/parquet/overview#partial-reading
      stmt =
        DuxDB.prepare(
          conn,
          """
          SELECT mobile_phone_model, count(DISTINCT user_id) AS users
          FROM read_parquet($glob)
          WHERE mobile_phone_model <> ''
          GROUP BY mobile_phone_model
          ORDER BY users DESC, mobile_phone_model
          """
        )

      DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "glob"), glob)
      result = DuxDB.execute_prepared(stmt)

      assert fetch_rows(result) == [
               ["Pixel", 3],
               ["iPhone", 1]
             ]
    end
  end

  describe "chapter 3: partitioning a Parquet dataset" do
    @tag :tmp_dir
    test "writes and prunes Hive-style partitions", %{conn: conn, tmp_dir: tmp_dir} do
      dataset_path = Path.join(tmp_dir, "orders")

      # PARTITION_BY writes Hive-style directories such as
      # year=2026/month=7/data_0.parquet. DuckDB's partitioned-write guide
      # covers filename patterns, append behavior, and overwrite semantics:
      # https://duckdb.org/docs/stable/data/partitioning/partitioned_writes
      DuxDB.query(
        conn,
        """
        COPY (
          SELECT *
          FROM (VALUES
            (101, 2025, 12, 1500),
            (102, 2026,  7, 2500),
            (103, 2026,  7, 4000),
            (104, 2026,  8, 3000)
          ) orders(order_id, year, month, amount_cents)
        ) TO '#{dataset_path}' (
          FORMAT PARQUET,
          PARTITION_BY (year, month),
          COMPRESSION ZSTD,
          PARQUET_VERSION V2
        )
        """
      )

      glob = Path.join(dataset_path, "**/*.parquet")

      # Hive partition values are inferred from directory names and exposed as
      # columns. Filtering them lets DuckDB skip unrelated directories/files,
      # which is especially useful when Parquet lives in remote object storage:
      # https://duckdb.org/docs/stable/data/partitioning/hive_partitioning
      #
      # DuckDB's performance guide discusses when direct Parquet queries and
      # partition pruning are a good fit:
      # https://duckdb.org/docs/stable/guides/performance/file_formats
      stmt =
        DuxDB.prepare(
          conn,
          """
          SELECT order_id, amount_cents
          FROM read_parquet($glob, hive_partitioning = true)
          WHERE year = 2026 AND month = 7
          ORDER BY order_id
          """
        )

      DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "glob"), glob)
      result = DuxDB.execute_prepared(stmt)

      assert fetch_rows(result) == [
               [102, 2500],
               [103, 4000]
             ]
    end
  end

  # DuxDB mirrors DuckDB's chunk-oriented C API. A result may contain multiple
  # chunks, while each chunk exposes one vector per column. Transposing those
  # vectors produces the row-shaped values that are convenient for assertions.
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
