minio_available? =
  case Req.get("http://localhost:9000/minio/health/live", retry: :transient) do
    {:ok, %{status: 200}} -> true
    _ -> false
  end

exclude =
  if minio_available? do
    []
  else
    Mix.shell().error("""
    Minio is not detected so `:minio` tags would be excluded.
    Please start the container with the following command if you want those tests to run:

        docker compose up minio -d
    """)

    [:minio]
  end

ExUnit.start(exclude: exclude)
