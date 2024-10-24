defmodule DuxDB do
  @moduledoc "DuckDB in Elixir"

  @type db :: reference
  @type conn :: reference
  @type config :: reference

  @doc """
  Returns the version of the linked DuckDB library.

      iex> DuxDB.library_version()
      "v1.1.2"

  See https://duckdb.org/docs/api/c/api#duckdb_library_version
  """
  @spec library_version :: String.t()
  def library_version, do: :erlang.nif_error(:undef)

  @doc """
  Initializes an empty configuration object that can be used to provide start-up options for a DuckDB instance.

      iex> config = DuxDB.create_config()
      iex> is_reference(config)
      true

  The configuration object is destroyed on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_create_config
  """
  @spec create_config :: config
  def create_config, do: :erlang.nif_error(:undef)

  @doc """
  Returns the total amount of configuration options available.

      iex> DuxDB.config_count()
      100

  See https://duckdb.org/docs/api/c/api#duckdb_config_count
  """
  @spec config_count :: non_neg_integer
  def config_count, do: :erlang.nif_error(:undef)

  @doc """
  Obtains a human-readable name and description of a specific configuration option.

      iex> DuxDB.get_config_flag(0)
      {"access_mode", "Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)"}

      iex> all_config_flags = Enum.map(0..DuxDB.config_count() - 1, fn idx ->
      ...>   DuxDB.get_config_flag(idx)
      ...> end)
      iex> all_config_flags |> Enum.drop(19) |> Enum.take(3)
      [
        {"allow_unsigned_extensions", "Allow to load extensions with invalid or missing signatures"},
        {"allow_community_extensions", "Allow to load community built extensions"},
        {"allow_extensions_metadata_mismatch", "Allow to load extensions with not compatible metadata"}
      ]

  See https://duckdb.org/docs/api/c/api#duckdb_get_config_flag
  """
  @spec get_config_flag(non_neg_integer) :: {name :: String.t(), description :: String.t()}
  def get_config_flag(_index), do: :erlang.nif_error(:undef)

  @doc """
  Sets the specified option for the specified configuration.

      iex> config = DuxDB.create_config()
      iex> DuxDB.set_config(config, "access_mode", "READ_WRITE")
      :ok

      iex> config = DuxDB.create_config()
      iex> DuxDB.set_config(config, "access_mode", "READ_AND_DREAM")
      :error

  See https://duckdb.org/docs/api/c/api#duckdb_set_config
  """
  @spec set_config(config, String.t(), String.t()) :: :ok | :error
  def set_config(config, name, option) do
    set_config_nif(config, c_str(name), c_str(option))
  end

  defp set_config_nif(_config, _name, _option), do: :erlang.nif_error(:undef)

  @doc """
  Destroys the specified configuration object.

      iex> config = DuxDB.create_config()
      iex> DuxDB.destroy_config(config)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_config
  """
  @spec destroy_config(config) :: :ok
  def destroy_config(_config), do: :erlang.nif_error(:undef)

  @doc """
  Opens a DuckDB database with a configuration.

      iex> path = "test.db"
      iex> config = DuxDB.create_config()
      iex> :ok = DuxDB.set_config(config, "access_mode", "READ_WRITE")
      iex> :ok = DuxDB.set_config(config, "threads", "8")
      iex> :ok = DuxDB.set_config(config, "max_memory", "8GB")
      iex> db = DuxDB.open_ext(path, config)
      iex> is_reference(db)
      true

  The database is closed on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_open_ext
  """
  @spec open_ext(Path.t(), config) :: db
  def open_ext(path, config) do
    case open_ext_nif(c_str(path), config) do
      db when is_reference(db) -> db
      error when is_binary(error) -> raise RuntimeError, message: error
    end
  end

  defp open_ext_nif(_path, _config), do: :erlang.nif_error(:undef)

  @doc """
  Closes a DuckDB database.

      iex> db = DuxDB.open_ext(":memory:", DuxDB.create_config())
      iex> DuxDB.close(db)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_close
  """
  @spec close(db) :: :ok
  def close(_db), do: :erlang.nif_error(:undef)

  @doc """
  Connects to a DuckDB database.

      iex> db = DuxDB.open_ext(":memory:", DuxDB.create_config())
      iex> conn = DuxDB.connect(db)
      iex> is_reference(conn)
      true

  The connection is disconnected on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_connect
  """
  @spec connect(db) :: conn
  def connect(_db), do: :erlang.nif_error(:undef)

  @doc """
  Interrupts a running query.

      iex> db = DuxDB.open_ext(":memory:", DuxDB.create_config())
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.interrupt(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_interrupt
  """
  @spec interrupt(conn) :: :ok
  def interrupt(_conn), do: :erlang.nif_error(:undef)

  @doc """
  Gets progress of the running query.

      iex> db = DuxDB.open_ext(":memory:", DuxDB.create_config())
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.query_progress(conn)
      {_percentage = -1.0, _rows_processed = 0, _total_rows_to_process = 0}

  """
  @spec query_progress(conn) ::
          {
            percentage :: float,
            rows_processed :: non_neg_integer,
            total_rows_to_process :: non_neg_integer
          }
  def query_progress(_conn), do: :erlang.nif_error(:undef)

  @doc """
  Disconnects from a DuckDB database.

      iex> db = DuxDB.open_ext(":memory:", DuxDB.create_config())
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.disconnect(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_disconnect
  """
  @spec disconnect(conn) :: :ok
  def disconnect(_conn), do: :erlang.nif_error(:undef)

  # TODO find a cleaner way
  @compile inline: [c_str: 1]
  defp c_str(v), do: [to_string(v), 0]

  @compile {:autoload, false}
  @on_load {:load_nif, 0}

  @doc false
  def load_nif do
    :code.priv_dir(:duxdb)
    |> :filename.join(~c"duxdb_nif")
    |> :erlang.load_nif(0)
  end
end
