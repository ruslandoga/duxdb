defmodule DuxDB do
  @moduledoc "DuckDB in Elixir"

  @typedoc """
  A reference to a DuckDB database NIF resource.
  """
  @type db :: reference

  @typedoc """
  A reference to a DuckDB connection NIF resource.
  """
  @type conn :: reference

  @typedoc """
  A reference to a DuckDB result NIF resource.
  """
  @type result :: reference

  @typedoc """
  A reference to a DuckDB data chunk NIF resource.
  """
  @type data_chunk :: reference

  @typedoc """
  A reference to a DuckDB prepared statement NIF resource.
  """
  @type stmt :: reference

  @doc """
  Returns the version of the linked DuckDB library.

      iex> DuxDB.library_version()
      "v1.1.2"

  See https://duckdb.org/docs/api/c/api#duckdb_library_version
  """
  @spec library_version :: String.t()
  def library_version, do: :erlang.nif_error(:undef)

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

      iex> DuxDB.get_config_flag(DuxDB.config_count() + 1)
      ** (ArgumentError) argument error: 101

  See https://duckdb.org/docs/api/c/api#duckdb_get_config_flag
  """
  @spec get_config_flag(non_neg_integer) :: {name :: String.t(), description :: String.t()}
  def get_config_flag(_index), do: :erlang.nif_error(:undef)

  defp create_config, do: :erlang.nif_error(:undef)
  defp set_config(_config, _name, _option), do: :erlang.nif_error(:undef)
  defp destroy_config(_config), do: :erlang.nif_error(:undef)

  defp build_config(config, options) do
    Enum.each(options, fn {name, option} ->
      with :error <- set_config(config, c_str(name), c_str(option)) do
        raise ArgumentError,
          message: "invalid config option #{inspect(options)} for #{inspect(name)}"
      end
    end)

    config
  end

  # TODO open/0
  # TODO don't create config in open/1

  @doc """
  Same as `open/2` but without config.
  """
  @spec open(Path.t()) :: db
  def open(path), do: open(path, _config = [])

  @doc """
  Opens a DuckDB database with a configuration.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Opening an on-disk database can take a long time, consider using `open_dirty_io/2` instead.

      iex> path = "test.db"
      iex> db = DuxDB.open(path, _config = %{"max_memory" => "8GB"})
      iex> is_reference(db)
      true

      iex> bad_path = "/this/path/not/exists/test.db "
      iex> DuxDB.open(bad_path, _config = [])
      ** (ArgumentError) IO Error: Cannot open file "/this/path/not/exists/test.db ": No such file or directory

  The database is closed with `close/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_open_ext
  """
  @spec open(Path.t(), Enumerable.t()) :: db
  def open(path, options) do
    config = create_config()

    try do
      case open_ext_nif(c_str(path), build_config(config, options)) do
        db when is_reference(db) -> db
        error when is_binary(error) -> raise ArgumentError, message: error
      end
    after
      destroy_config(config)
    end
  end

  defp open_ext_nif(_path, _config), do: :erlang.nif_error(:undef)

  @doc """
  Same as `open_dirty_io/2` but without config.
  """
  @spec open_dirty_io(Path.t()) :: db
  def open_dirty_io(path), do: open_dirty_io(path, _config = [])

  @doc """
  Same as `open/2` but gets executed on a Dirty IO scheduler.
  """
  @spec open_dirty_io(Path.t(), Enumerable.t()) :: db
  def open_dirty_io(path, options) do
    config = create_config()

    try do
      case open_ext_dirty_io_nif(c_str(path), build_config(config, options)) do
        db when is_reference(db) -> db
        error when is_binary(error) -> raise ArgumentError, message: error
      end
    after
      destroy_config(config)
    end
  end

  defp open_ext_dirty_io_nif(_path, _config), do: :erlang.nif_error(:undef)

  @doc """
  Closes a DuckDB database.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Closing an on-disk database can take a long time, consider using `close_dirty_io/1` instead.

      iex> db = DuxDB.open(":memory:")
      iex> DuxDB.close(db)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_close
  """
  @spec close(db) :: :ok
  def close(_db), do: :erlang.nif_error(:undef)

  @doc """
  Same as `close/1` but gets executed on a Dirty IO scheduler.
  """
  @spec close_dirty_io(db) :: :ok
  def close_dirty_io(_db), do: :erlang.nif_error(:undef)

  @doc """
  Connects to a DuckDB database.

      iex> db = DuxDB.open(":memory:")
      iex> conn = DuxDB.connect(db)
      iex> is_reference(conn)
      true

  The connection is disconnected with `disconnect/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_connect
  """
  @spec connect(db) :: conn
  def connect(_db), do: :erlang.nif_error(:undef)

  @doc """
  Interrupts a running query.

      iex> db = DuxDB.open(":memory:")
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.interrupt(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_interrupt
  """
  @spec interrupt(conn) :: :ok
  def interrupt(_conn), do: :erlang.nif_error(:undef)

  @doc """
  Gets progress of the running query.

      iex> db = DuxDB.open(":memory:")
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.query_progress(conn)
      {_percentage = -1.0, _rows_processed = 0, _total_rows_to_process = 0}

  See https://duckdb.org/docs/api/c/api#duckdb_query_progress
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

      iex> db = DuxDB.open(":memory:")
      iex> conn = DuxDB.connect(db)
      iex> DuxDB.disconnect(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_disconnect
  """
  @spec disconnect(conn) :: :ok
  def disconnect(_conn), do: :erlang.nif_error(:undef)

  defmodule Error do
    @moduledoc """
    Wraps `enum duckdb_error_type`

    See https://duckdb.org/docs/api/c/api#duckdb_result_error_type
    """

    @type t :: %__MODULE__{code: integer, message: String.t()}
    defexception [:code, :message]
  end

  @doc """
  Executes an SQL query within a connection and stores the full (materialized) result in the returned reference.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Executing a lengthy query can degrade the VM's responsiveness.
  > If you expect the query to take longer than 1 millisecond, consider using `query_dirty_cpu/2` or `query_dirty_io/2` instead.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> is_reference(result)
      true

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> DuxDB.query(conn, "SEL 42")
      ** (DuxDB.Error) Parser Error: syntax error at or near "SEL"
      LINE 1: SEL 42
              ^

  The result is destroyed with `destroy_result/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_query
  """
  @spec query(conn, String.t()) :: result
  def query(conn, sql) do
    case query_nif(conn, c_str(sql)) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp query_nif(_conn, _sql), do: :erlang.nif_error(:undef)

  @doc """
  Same as `query/2` but gets executed on a Dirty CPU scheduler.
  """
  @spec query_dirty_cpu(conn, String.t()) :: result
  def query_dirty_cpu(conn, sql) do
    case query_dirty_cpu_nif(conn, c_str(sql)) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp query_dirty_cpu_nif(_conn, _sql), do: :erlang.nif_error(:undef)

  @doc """
  Same as `query/2` but gets executed on a Dirty IO scheduler.
  """
  @spec query_dirty_io(conn, String.t()) :: result
  def query_dirty_io(conn, sql) do
    case query_dirty_io_nif(conn, c_str(sql)) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp query_dirty_io_nif(_conn, _sql), do: :erlang.nif_error(:undef)

  @doc """
  Closes the result and de-allocates all memory allocated for that connection.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> DuxDB.destroy_result(result)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_result
  """
  @spec destroy_result(result) :: :ok
  def destroy_result(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the column name of the specified column. Returns `nil` if the column is out of range.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.column_name(result, 0)
      "answer"

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.column_name(result, 1)
      nil

  See https://duckdb.org/docs/api/c/api#duckdb_column_name
  """
  @spec column_name(result, non_neg_integer) :: String.t() | nil
  def column_name(_result, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Returns the statement type of the statement that was executed.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.result_statement_type(result)
      _select = 1

  See https://duckdb.org/docs/api/c/api#duckdb_result_statement_type
  """
  @spec result_statement_type(result) :: integer
  def result_statement_type(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the number of columns present in a the result object.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer, 'six times seven' AS question")
      iex> DuxDB.column_count(result)
      2

  See https://duckdb.org/docs/api/c/api#duckdb_column_count
  """
  @spec column_count(result) :: non_neg_integer
  def column_count(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the number of rows changed by the query stored in the result.
  This is relevant only for INSERT/UPDATE/DELETE queries.
  For other queries the rows_changed will be 0.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer, 'six times seven' AS question")
      iex> DuxDB.rows_changed(result)
      0

  See https://duckdb.org/docs/api/c/api#duckdb_rows_changed
  """
  @spec rows_changed(result) :: non_neg_integer
  def rows_changed(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the return_type of the given result.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> DuxDB.result_return_type(result)
      _query_result = 3

  See https://duckdb.org/docs/api/c/api#duckdb_result_return_type
  """
  @spec result_return_type(result) :: integer
  def result_return_type(_result), do: :erlang.nif_error(:undef)

  @doc """
  Fetches the next chunk of data from the result.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer"))
      iex> is_reference(chunk)
      true

  The chunk is destroyed with `destroy_data_chunk/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_fetch_chunk
  """
  @spec fetch_chunk(result) :: data_chunk | nil
  def fetch_chunk(_result), do: :erlang.nif_error(:undef)

  # TODO https://duckdb.org/docs/api/c/data_chunk#duckdb_create_data_chunk
  # TODO https://duckdb.org/docs/api/c/data_chunk#duckdb_data_chunk_reset

  @doc """
  Destroys the data chunk and de-allocates all memory allocated for that chunk.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer"))
      iex> DuxDB.destroy_data_chunk(chunk)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_data_chunk
  """
  @spec destroy_data_chunk(data_chunk) :: :ok
  def destroy_data_chunk(_data_chunk), do: :erlang.nif_error(:undef)

  @doc """
  Retrieves the number of columns in a data chunk.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer, 'six times seven' AS question"))
      iex> DuxDB.data_chunk_get_column_count(chunk)
      2

  See https://duckdb.org/docs/api/c/api#duckdb_data_chunk_get_column_count
  """
  @spec data_chunk_get_column_count(data_chunk) :: non_neg_integer
  def data_chunk_get_column_count(_data_chunk), do: :erlang.nif_error(:undef)

  # TODO vector resource + DuxDB.vector_to_list(vector)?

  @doc """
  Retrieves the vector (as a list) at the specified column index in the data chunk.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> sql = \"""
      ...> SELECT * FROM (VALUES
      ...>   (1, 'Alice', DATE '2023-01-01', TRUE),
      ...>   (2, 'Bob', DATE '2023-06-15', FALSE),
      ...>   (3, 'Charlie', DATE '2024-10-29', TRUE)
      ...> ) AS users(id, name, join_date, is_active);
      ...> \"""
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, sql))
      iex> Enum.map(0..DuxDB.data_chunk_get_column_count(chunk) - 1, fn i ->
      ...>   DuxDB.data_chunk_get_vector(chunk, i)
      ...> end)
      [
        _id = [1, 2, 3],
        _name = ["Alice", "Bob", "Charlie"],
        _join_date = [~D[2023-01-01], ~D[2023-06-15], ~D[2024-10-29]],
        _is_active = [true, false, true]
      ]

  See https://duckdb.org/docs/api/c/api#duckdb_data_chunk_get_vector
  """
  @spec data_chunk_get_vector(data_chunk, non_neg_integer) :: [binary | number | boolean | nil]
  def data_chunk_get_vector(_data_chunk, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Creates a prepared statement object from a query.

  > ### Runs on a main scheduler. {: .warning}
  >
  > If you expect `duckdb_prepare` to take a long time, consider using `prepare_dirty_cpu/2` instead.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> is_reference(stmt)
      true

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> DuxDB.prepare(conn, "SEL ?")
      ** (ArgumentError) Parser Error: syntax error at or near "SEL"
      LINE 1: SEL ?
              ^

  The statement is destroyed with `destroy_prepare/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_prepare
  """
  @spec prepare(conn, String.t()) :: stmt
  def prepare(conn, sql) do
    case prepare_nif(conn, c_str(sql)) do
      stmt when is_reference(stmt) -> stmt
      error -> raise ArgumentError, message: error
    end
  end

  defp prepare_nif(_conn, _sql), do: :erlang.nif_error(:undef)

  @doc """
  Same as `prepare/2` but gets executed on a Dirty CPU scheduler.
  """
  @spec prepare_dirty_cpu(conn, String.t()) :: stmt
  def prepare_dirty_cpu(conn, sql) do
    case prepare_dirty_cpu_nif(conn, c_str(sql)) do
      stmt when is_reference(stmt) -> stmt
      error -> raise ArgumentError, message: error
    end
  end

  defp prepare_dirty_cpu_nif(_conn, _sql), do: :erlang.nif_error(:undef)

  @doc """
  Destroys the prepared statement object.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.destroy_prepare(stmt)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_prepare
  """
  @spec destroy_prepare(stmt) :: :ok
  def destroy_prepare(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the number of parameters that can be provided to the given prepared statement.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?, ?")
      iex> DuxDB.nparams(stmt)
      2

  See https://duckdb.org/docs/api/c/api#duckdb_nparams
  """
  @spec nparams(stmt) :: non_neg_integer
  def nparams(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the name used to identify the parameter.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT $a, $b")
      iex> [DuxDB.parameter_name(stmt, 1), DuxDB.parameter_name(stmt, 2)]
      ["a", "b"]

  See https://duckdb.org/docs/api/c/api#duckdb_parameter_name
  """
  @spec parameter_name(stmt, non_neg_integer) :: String.t()
  def parameter_name(_stmt, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Retrieves the index of the parameter for the prepared statement, identified by name.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT $name")
      iex> DuxDB.bind_parameter_index(stmt, "name")
      1

  See https://duckdb.org/docs/api/c/api#duckdb_bind_parameter_index
  """
  @spec bind_parameter_index(stmt, String.t()) :: non_neg_integer
  def bind_parameter_index(stmt, name) do
    bind_parameter_index_nif(stmt, c_str(name))
  end

  defp bind_parameter_index_nif(_stmt, _name), do: :erlang.nif_error(:undef)

  @doc """
  Clears the params bind to the prepared statement.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.clear_bindings(stmt)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_clear_bindings
  """
  @spec clear_bindings(stmt) :: :ok
  def clear_bindings(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the statement type of the statement to be executed.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT 42")
      iex> DuxDB.prepared_statement_type(stmt)
      _select = 1

  See https://duckdb.org/docs/api/c/api#duckdb_prepared_statement_type
  """
  @spec prepared_statement_type(stmt) :: integer
  def prepared_statement_type(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Executes the prepared statement with the given bound parameters, and returns a materialized query result.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Executing a lengthy prepared statement can degrade the VM's responsiveness.
  > If you expect the prepared statement to take longer than 1 millisecond, consider using `execute_prepared_dirty_cpu/2` or `execute_prepared_dirty_io/2` instead.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> result = DuxDB.execute_prepared(DuxDB.prepare(conn, "SELECT 42"))
      iex> is_reference(result)
      true

  The result is destroyed with `destroy_result/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_execute_prepared
  """
  @spec execute_prepared(stmt) :: result
  def execute_prepared(stmt) do
    case execute_prepared_nif(stmt) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp execute_prepared_nif(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Same as `execute_prepared/1` but gets executed on a Dirty CPU scheduler.
  """
  @spec execute_prepared_dirty_cpu(stmt) :: result
  def execute_prepared_dirty_cpu(stmt) do
    case execute_prepared_dirty_cpu_nif(stmt) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp execute_prepared_dirty_cpu_nif(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Same as `execute_prepared/1` but gets executed on a Dirty IO scheduler.
  """
  @spec execute_prepared_dirty_io(stmt) :: result
  def execute_prepared_dirty_io(stmt) do
    case execute_prepared_dirty_io_nif(stmt) do
      result when is_reference(result) -> result
      {err, msg} -> raise Error, code: err, message: msg
    end
  end

  defp execute_prepared_dirty_io_nif(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Binds a boolean value to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_boolean(stmt, 1, true)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_boolean
  """
  @spec bind_boolean(stmt, non_neg_integer, boolean) :: :ok
  def bind_boolean(_stmt, _idx, _bool), do: :erlang.nif_error(:undef)

  @doc """
  Binds a floating point number to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_double(stmt, 1, 3.14)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_double
  """
  @spec bind_double(stmt, non_neg_integer, float) :: :ok
  def bind_double(_stmt, _idx, _float), do: :erlang.nif_error(:undef)

  @doc """
  Binds a signed 64-bit integer to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_int64(stmt, 1, -3)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_int64
  """
  @spec bind_int64(stmt, non_neg_integer, integer) :: :ok
  def bind_int64(_stmt, _idx, _int), do: :erlang.nif_error(:undef)

  @doc """
  Binds an unsigned 64-bit integer to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_uint64(stmt, 1, 3)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_uint64
  """
  @spec bind_uint64(stmt, non_neg_integer, non_neg_integer) :: :ok
  def bind_uint64(_stmt, _idx, _uint), do: :erlang.nif_error(:undef)

  @doc """
  Binds a text value to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_varchar(stmt, 1, "hello, world")
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_varchar_length
  """
  @spec bind_varchar(stmt, non_neg_integer, String.t()) :: :ok
  def bind_varchar(_stmt, _idx, _text), do: :erlang.nif_error(:undef)

  @doc """
  Binds a binary blob to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_blob(stmt, 1, <<1, 2, 3>>)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_blob
  """
  @spec bind_blob(stmt, non_neg_integer, binary) :: :ok
  def bind_blob(_stmt, _idx, _blob), do: :erlang.nif_error(:undef)

  @epoch_gregorian_days Date.to_gregorian_days(~D[1970-01-01])

  @doc """
  Binds a date (as days since 1970-01-01) to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_date(stmt, 1, ~D[2023-01-01])
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_date
  """
  @spec bind_date(stmt, non_neg_integer, Date.t() | integer) :: :ok
  def bind_date(stmt, idx, %Date{} = date) do
    days = Date.to_gregorian_days(date) - @epoch_gregorian_days
    bind_date_nif(stmt, idx, days)
  end

  def bind_date(stmt, idx, days), do: bind_date_nif(stmt, idx, days)
  defp bind_date_nif(_stmt, _idx, _days), do: :erlang.nif_error(:undef)

  @doc """
  Binds a NULL value to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open(":memory:"))
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_null(stmt, 1)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_boolean
  """
  @spec bind_null(stmt, non_neg_integer) :: :ok
  def bind_null(_stmt, _idx), do: :erlang.nif_error(:undef)

  # TODO find a cleaner way
  @compile inline: [c_str: 1]
  defp c_str(b) when is_binary(b), do: [b, 0]

  defp c_str(v) do
    raise ArgumentError, "expected a binary, got: #{inspect(v)}"
  end

  @compile {:autoload, false}
  @on_load {:load_nif, 0}

  @doc false
  def load_nif do
    :code.priv_dir(:duxdb)
    |> :filename.join(~c"duxdb_nif")
    |> :erlang.load_nif(0)
  end
end
