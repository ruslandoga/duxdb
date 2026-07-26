defmodule MinioTest do
  use ExUnit.Case
  @moduletag :minio

  setup do
    credentials = Help.s3_credentials(:minio)
    bucket = "duxdb-#{System.unique_integer([:positive])}"
    Help.create_bucket(credentials, bucket)
    on_exit(fn -> Help.delete_bucket(credentials, bucket) end)

    conn = DuxDB.connect(DuxDB.open())
    Help.attach_s3(conn, credentials)

    {:ok, bucket: bucket, conn: conn, credentials: credentials}
  end

  test "reads and writes objects with DuckDB", %{
    bucket: bucket,
    conn: conn,
    credentials: credentials
  } do
    on_exit(fn -> Help.delete_object(credentials, bucket, "input.csv") end)
    Help.put_object(credentials, bucket, "input.csv", "id,name\n1,one\n2,two\n")
    on_exit(fn -> Help.delete_object(credentials, bucket, "output.parquet") end)

    DuxDB.query(conn, """
    COPY (
      SELECT * FROM read_csv_auto('s3://#{bucket}/input.csv')
    ) TO 's3://#{bucket}/output.parquet' (FORMAT parquet)
    """)

    assert [
             %{
               "id" => [1, 2],
               "name" => ["one", "two"]
             }
           ] =
             conn
             |> DuxDB.query("SELECT * FROM read_parquet('s3://#{bucket}/output.parquet')")
             |> Help.fetch_chunks()
  end
end
