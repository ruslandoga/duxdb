# An executable Parquet guide
#
# This file is meant to be useful in two ways: ExUnit runs it as a regression
# suite, and users can read it from top to bottom as a small Parquet cookbook.
# Each example is independent, so the tests remain safe to run asynchronously.
#
# Start with DuckDB's Parquet overview for the complete SQL surface:
# https://duckdb.org/docs/stable/data/parquet/overview
#
# The direct-query examples are also inspired by ClickHouse's technical tour of
# Parquet. It is a particularly good explanation of how projection, filtering,
# aggregation, and parallel file scans fit together:
# https://clickhouse.com/blog/clickhouse-and-parquet-a-foundation-for-fast-lakehouse-analytics
defmodule ParquetTest do
  use ExUnit.Case, async: true

  @tag :tmp_dir
  test "round-trips typed and nullable data", %{tmp_dir: tmp_dir} do
    db = DuxDB.open()
    conn = DuxDB.connect(db)
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

    # Each map returned by Help.fetch_chunks/1 represents one DuckDB data
    # chunk, with column names mapped to their decoded vectors.
    assert Help.fetch_chunks(result) == [
             %{
               "active" => [true, false, true],
               "happened_on" => [~D[2026-07-24], ~D[2026-07-25], ~D[2026-07-26]],
               "id" => [1, 2, 3],
               "name" => ["created", "updated", nil]
             }
           ]
  end

  @tag :tmp_dir
  test "filters, aggregates, and sorts across a file glob", %{tmp_dir: tmp_dir} do
    db = DuxDB.open()
    conn = DuxDB.connect(db)

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

    assert Help.fetch_chunks(result) == [
             %{
               "mobile_phone_model" => ["Pixel", "iPhone"],
               "users" => [3, 1]
             }
           ]
  end

  @tag :tmp_dir
  test "writes and prunes Hive-style partitions", %{tmp_dir: tmp_dir} do
    db = DuxDB.open()
    conn = DuxDB.connect(db)
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

    assert Help.fetch_chunks(result) == [
             %{
               "amount_cents" => [2500, 4000],
               "order_id" => [102, 103]
             }
           ]
  end

  @tag :minio
  test "writes a Parquet object and queries it in place" do
    db = DuxDB.open()
    conn = DuxDB.connect(db)
    credentials = Help.s3_credentials(:minio)
    bucket = "duxdb-parquet-#{System.unique_integer([:positive])}"

    Help.setup_bucket(credentials, bucket)
    Help.attach_s3(conn, credentials)

    key = "orders.parquet"
    s3_path = "s3://#{bucket}/#{key}"

    # httpfs gives DuckDB a filesystem abstraction over S3-compatible APIs.
    # It supports reads, multipart writes, and globs for Parquet datasets:
    # https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
    #
    # MinIO stands in for S3 here so the integration test is deterministic and
    # does not require cloud credentials. The same SQL and s3:// URL shape work
    # with AWS S3 and other compatible object stores.
    Help.setup_object(credentials, bucket, key)

    DuxDB.query(
      conn,
      """
      COPY (
        SELECT *
        FROM (VALUES
          (1, 'eu',   'paid',    1200),
          (2, 'us',   'pending', 1800),
          (3, 'eu',   'paid',    2300),
          (4, 'apac', 'paid',     900)
        ) orders(order_id, region, status, amount_cents)
      ) TO '#{s3_path}' (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        PARQUET_VERSION V2
      )
      """
    )

    # Parquet metadata and HTTP range requests let DuckDB fetch relevant
    # columns and row groups instead of downloading the whole remote object:
    # https://duckdb.org/docs/stable/core_extensions/httpfs/https#partial-reading
    stmt =
      DuxDB.prepare(
        conn,
        """
        SELECT region, count(*) AS orders, sum(amount_cents) AS revenue_cents
        FROM read_parquet($path)
        WHERE status = 'paid'
        GROUP BY region
        ORDER BY revenue_cents DESC
        """
      )

    DuxDB.bind_varchar(stmt, DuxDB.bind_parameter_index(stmt, "path"), s3_path)
    result = DuxDB.execute_prepared(stmt)

    assert Help.fetch_chunks(result) == [
             %{
               "orders" => [2, 1],
               "region" => ["eu", "apac"],
               "revenue_cents" => [3500, 900]
             }
           ]
  end
end
