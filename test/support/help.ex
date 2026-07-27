defmodule Help do
  use ExUnitProperties

  def fetch_chunks(result) do
    case DuxDB.fetch_chunk(result) do
      nil -> []
      chunk -> [fetch_vectors(chunk, result) | fetch_chunks(result)]
    end
  end

  def fetch_vectors(chunk, result) do
    Map.new(0..(DuxDB.data_chunk_get_column_count(chunk) - 1), fn idx ->
      {DuxDB.column_name(result, idx), DuxDB.data_chunk_get_vector(chunk, idx)}
    end)
  end

  def time do
    gen all hour <- StreamData.integer(0..23),
            minute <- StreamData.integer(0..59),
            second <- StreamData.integer(0..59),
            micros <- StreamData.integer(0..999_999) do
      Time.new!(hour, minute, second, {micros, 6})
    end
  end

  def date do
    gen all year <- StreamData.integer(0..9999),
            month <- StreamData.integer(1..12),
            day <- StreamData.integer(1..28) do
      Date.new!(year, month, day)
    end
  end

  def naive_datetime do
    gen all date <- date(), time <- time() do
      NaiveDateTime.new!(date, time)
    end
  end

  def attach_s3(conn, credentials) do
    DuxDB.query(conn, """
    CREATE OR REPLACE SECRET secret (
      TYPE s3,
      PROVIDER config,
      ENDPOINT '#{String.replace_prefix(credentials.endpoint_url, "http://", "")}',
      KEY_ID '#{credentials.access_key_id}',
      SECRET '#{credentials.secret_access_key}',
      REGION '#{credentials.region}',
      URL_STYLE path,
      USE_SSL false
    );
    """)
  end

  def s3_credentials(:minio) do
    %{
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID", "minioadmin"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY", "minioadmin"),
      region: System.get_env("AWS_REGION", "us-east-1"),
      endpoint_url: System.get_env("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    }
  end

  def s3_req(credentials) do
    req = Req.new(retry: :transient)

    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      endpoint_url: endpoint_url
    } = credentials

    ReqS3.attach(req,
      aws_sigv4: [
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        region: region
      ],
      aws_endpoint_url_s3: endpoint_url
    )
  end

  def setup_bucket(credentials, bucket) do
    s3_req = s3_req(credentials)
    ExUnit.Callbacks.on_exit(fn -> Req.delete!(s3_req, url: "s3://#{bucket}") end)
    %{status: 200} = Req.put!(s3_req, url: "s3://#{bucket}")
    :ok
  end

  def setup_object(credentials, bucket, key) do
    s3_req = s3_req(credentials)
    url = "s3://#{bucket}/#{key}"
    ExUnit.Callbacks.on_exit(fn -> Req.delete!(s3_req, url: url) end)
    :ok
  end

  def setup_object(credentials, bucket, key, contents) do
    setup_object(credentials, bucket, key)
    s3_req = s3_req(credentials)
    url = "s3://#{bucket}/#{key}"
    %{status: 200} = Req.put!(s3_req, url: url, body: contents)

    :ok
  end
end
