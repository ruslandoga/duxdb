defmodule DuxDB do
  @moduledoc "DuckDB in Elixir"

  @typedoc "A database object."
  @type db :: reference

  @typedoc "A connection to a duckdb database."
  @type conn :: reference

  @typedoc "A query result."
  @type result :: reference

  @typedoc "A data chunk from a result."
  @type data_chunk :: reference

  @typedoc """
  A prepared statement is a parameterized query that allows you to bind parameters to it.
  """
  @type stmt :: reference

  @doc """
  Returns the version of the linked DuckDB library.

      iex> DuxDB.library_version()
      "v1.4.2"

  See https://duckdb.org/docs/api/c/api#duckdb_library_version
  """
  @spec library_version :: String.t()
  def library_version, do: :erlang.nif_error(:undef)

  @doc """
  Returns the total amount of configuration options available.

      iex> DuxDB.config_count()
      199

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
      iex> all_config_flags |> Enum.drop(23) |> Enum.take(6)
      [
        {"custom_user_agent", "Metadata from DuckDB callers"},
        {"debug_asof_iejoin",
         "DEBUG SETTING: force use of IEJoin to implement AsOf joins"},
        {"debug_checkpoint_abort",
         "DEBUG SETTING: trigger an abort while checkpointing for testing purposes"},
        {"debug_force_external",
         "DEBUG SETTING: force out-of-core computation for operators that support it, used for testing"},
        {"debug_force_no_cross_product",
         "DEBUG SETTING: Force disable cross product generation when hyper graph isn't connected, used for testing"},
        {"debug_skip_checkpoint_on_commit",
         "DEBUG SETTING: skip checkpointing on commit"}
      ]

      iex> DuxDB.get_config_flag(DuxDB.config_count() + 1)
      ** (ArgumentError) argument error: 200

  See https://duckdb.org/docs/api/c/api#duckdb_get_config_flag
  """
  @spec get_config_flag(non_neg_integer) :: {name :: String.t(), description :: String.t()}
  def get_config_flag(_index), do: :erlang.nif_error(:undef)

  defp create_config, do: :erlang.nif_error(:undef)
  defp set_config(_config, _name, _option), do: :erlang.nif_error(:undef)
  defp destroy_config(_config), do: :erlang.nif_error(:undef)

  @doc """
  Opens an in-memory DuckDB database.

      iex> db = DuxDB.open()
      iex> is_reference(db)
      true

  """
  @spec open :: db
  def open, do: open(_path = nil, _config = nil)

  @doc """
  Same as `open/2` but without config.
  """
  @spec open(Path.t() | nil) :: db
  def open(maybe_path), do: open(maybe_path, _config = nil)

  @doc """
  Opens a DuckDB database with a configuration.

      iex> path = "test.db"
      iex> db = DuxDB.open(path, _config = %{"max_memory" => "8GB"})
      iex> is_reference(db)
      true

      iex> bad_path = "/this/path/not/exists/test.db "
      iex> DuxDB.open(bad_path)
      ** (ArgumentError) IO Error: Cannot open file "/this/path/not/exists/test.db ": No such file or directory

  The database is closed with `close/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_open_ext
  """
  @spec open(Path.t() | nil, Enumerable.t() | nil) :: db
  def open(maybe_path, maybe_options) do
    path = if maybe_path, do: c_str(maybe_path)
    config = if maybe_options, do: create_config()

    try do
      if config do
        set_config_options(config, maybe_options)
      end

      case open_ext(path, config) do
        db when is_reference(db) -> db
        error when is_binary(error) -> raise ArgumentError, message: error
      end
    after
      if config do
        destroy_config(config)
      end
    end
  end

  defp open_ext(_path, _config), do: :erlang.nif_error(:undef)

  defp set_config_options(config, options) do
    Enum.each(options, fn {name, option} ->
      with :error <- set_config(config, c_str(name), c_str(option)) do
        raise ArgumentError,
          message: "invalid config option #{inspect(options)} for #{inspect(name)}"
      end
    end)
  end

  @doc """
  Closes a DuckDB database.

      iex> db = DuxDB.open(":memory:")
      iex> DuxDB.close(db)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_close
  """
  @spec close(db) :: :ok
  def close(_db), do: :erlang.nif_error(:undef)

  @doc """
  Connects to a DuckDB database.

      iex> db = DuxDB.open()
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> DuxDB.interrupt(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_interrupt
  """
  @spec interrupt(conn) :: :ok
  def interrupt(_conn), do: :erlang.nif_error(:undef)

  @doc """
  Gets progress of the running query.

      iex> conn = DuxDB.connect(DuxDB.open())
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> DuxDB.disconnect(conn)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_disconnect
  """
  @spec disconnect(conn) :: :ok
  def disconnect(_conn), do: :erlang.nif_error(:undef)

  defmodule Error do
    @moduledoc ~S"""
    Wraps DuckDB error.

    Contains `code` that is `duckdb_error_type` (e.g. DUCKDB_ERROR_PARSER = 14)
    and the correspoinding error `message`.

    Example:

        iex> conn = DuxDB.connect(DuxDB.open())
        iex> try do DuxDB.query(conn, "sel 1") rescue e -> e end
        %DuxDB.Error{code: 14, message: "Parser Error: syntax error at or near \"sel\"\n\nLINE 1: sel 1\n        ^\n\nLINE 1: sel 1\n        ^"}

    """

    @type t :: %__MODULE__{code: integer, message: String.t()}
    defexception [:code, :message]
  end

  @doc ~S"""
  Executes an SQL query within a connection and stores the full (materialized) result in the returned reference.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Executing a lengthy query can degrade the VM's responsiveness.
  > If you expect the query to take longer than 1 millisecond,
  > consider using `query_dirty_cpu/2` or `query_dirty_io/2` instead.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> is_reference(result)
      true

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> try do DuxDB.query(conn, "SEL 42") rescue e -> e end
      %DuxDB.Error{code: 14, message: "Parser Error: syntax error at or near \"SEL\"\n\nLINE 1: SEL 42\n        ^\n\nLINE 1: SEL 42\n        ^"}

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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> DuxDB.destroy_result(result)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_result
  """
  @spec destroy_result(result) :: :ok
  def destroy_result(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the column name of the specified column. Returns `nil` if the column is out of range.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.column_name(result, 0)
      "answer"

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.column_name(result, 1)
      nil

  See https://duckdb.org/docs/api/c/api#duckdb_column_name
  """
  @spec column_name(result, non_neg_integer) :: String.t() | nil
  def column_name(_result, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Returns the statement type of the statement that was executed.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer")
      iex> DuxDB.result_statement_type(result)
      _select = 1

  See https://duckdb.org/docs/api/c/api#duckdb_result_statement_type
  """
  @spec result_statement_type(result) :: integer
  def result_statement_type(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the number of columns present in a the result object.

      iex> conn = DuxDB.connect(DuxDB.open())
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42 AS answer, 'six times seven' AS question")
      iex> DuxDB.rows_changed(result)
      0

  See https://duckdb.org/docs/api/c/api#duckdb_rows_changed
  """
  @spec rows_changed(result) :: non_neg_integer
  def rows_changed(_result), do: :erlang.nif_error(:undef)

  @doc """
  Returns the return_type of the given result.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> result = DuxDB.query(conn, "SELECT 42")
      iex> DuxDB.result_return_type(result)
      _query_result = 3

  See https://duckdb.org/docs/api/c/api#duckdb_result_return_type
  """
  @spec result_return_type(result) :: integer
  def result_return_type(_result), do: :erlang.nif_error(:undef)

  @doc """
  Fetches the next chunk of data from the result.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer"))
      iex> is_reference(chunk)
      true

  The chunk is destroyed with `destroy_data_chunk/1` or on garbage collection.

  See https://duckdb.org/docs/api/c/api#duckdb_fetch_chunk
  """
  @spec fetch_chunk(result) :: data_chunk | nil
  def fetch_chunk(_result), do: :erlang.nif_error(:undef)

  @doc """
  Destroys the data chunk and de-allocates all memory allocated for that chunk.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer"))
      iex> DuxDB.destroy_data_chunk(chunk)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_data_chunk
  """
  @spec destroy_data_chunk(data_chunk) :: :ok
  def destroy_data_chunk(_data_chunk), do: :erlang.nif_error(:undef)

  @doc """
  Retrieves the number of columns in a data chunk.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> chunk = DuxDB.fetch_chunk(DuxDB.query(conn, "SELECT 42 AS answer, 'six times seven' AS question"))
      iex> DuxDB.data_chunk_get_column_count(chunk)
      2

  See https://duckdb.org/docs/api/c/api#duckdb_data_chunk_get_column_count
  """
  @spec data_chunk_get_column_count(data_chunk) :: non_neg_integer
  def data_chunk_get_column_count(_data_chunk), do: :erlang.nif_error(:undef)

  @type vector_value :: binary | number | boolean | Date.t() | Time.t() | NaiveDateTime.t()

  @doc """
  Retrieves the vector (as a list) at the specified column index in the data chunk.

      iex> conn = DuxDB.connect(DuxDB.open())
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
  @spec data_chunk_get_vector(data_chunk, non_neg_integer) :: [vector_value | nil]
  def data_chunk_get_vector(data_chunk, idx) do
    case data_chunk_get_vector_nif(data_chunk, idx) do
      vector when is_list(vector) -> vector
      {_DUCKDB_TYPE_HUGEINT = 16, hugeints} -> unwrap_hugeints(hugeints)
      {_DUCKDB_TYPE_UHUGEINT = 32, uhugeints} -> unwrap_hugeints(uhugeints)
    end
  end

  defp data_chunk_get_vector_nif(_data_chunk, _idx), do: :erlang.nif_error(:undef)

  @doc ~S"""
  Creates a prepared statement object from a query.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> is_reference(stmt)
      true

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> try do DuxDB.prepare(conn, "SEL ?") rescue e -> e end
      %ArgumentError{message: "Parser Error: syntax error at or near \"SEL\"\n\nLINE 1: SEL ?\n        ^\n\nLINE 1: SEL ?\n        ^"}

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
  Destroys the prepared statement object.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.destroy_prepare(stmt)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_destroy_prepare
  """
  @spec destroy_prepare(stmt) :: :ok
  def destroy_prepare(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the number of parameters that can be provided to the given prepared statement.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?, ?")
      iex> DuxDB.nparams(stmt)
      2

  See https://duckdb.org/docs/api/c/api#duckdb_nparams
  """
  @spec nparams(stmt) :: non_neg_integer
  def nparams(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the name used to identify the parameter.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT $a, $b")
      iex> [DuxDB.parameter_name(stmt, 1), DuxDB.parameter_name(stmt, 2)]
      ["a", "b"]

  See https://duckdb.org/docs/api/c/api#duckdb_parameter_name
  """
  @spec parameter_name(stmt, non_neg_integer) :: String.t()
  def parameter_name(_stmt, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Returns the parameter type for the parameter at the given index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> DuxDB.query(conn, "CREATE TABLE a (i INTEGER)")
      iex> stmt = DuxDB.prepare(conn, "INSERT INTO a VALUES (?)")
      iex> DuxDB.param_type(stmt, 1)
      4

  See https://duckdb.org/docs/api/c/api#duckdb_param_type
  """
  @spec param_type(stmt, non_neg_integer) :: integer
  def param_type(_stmt, _idx), do: :erlang.nif_error(:undef)

  @doc """
  Retrieves the index of the parameter for the prepared statement, identified by name.

      iex> conn = DuxDB.connect(DuxDB.open())
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.clear_bindings(stmt)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_clear_bindings
  """
  @spec clear_bindings(stmt) :: :ok
  def clear_bindings(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Returns the statement type of the statement to be executed.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT 42")
      iex> DuxDB.prepared_statement_type(stmt)
      _select = 1

  See https://duckdb.org/docs/api/c/api#duckdb_prepared_statement_type
  """
  @spec prepared_statement_type(stmt) :: integer
  def prepared_statement_type(_stmt), do: :erlang.nif_error(:undef)

  @doc """
  Executes the prepared statement with the given bound parameters,
  and returns a materialized query result.

  > ### Runs on a main scheduler. {: .warning}
  >
  > Executing a lengthy prepared statement can degrade the VM's responsiveness.
  > If you expect the prepared statement to take longer than 1 millisecond,
  > consider using `execute_prepared_dirty_cpu/2` or
  > `execute_prepared_dirty_io/2` instead.

      iex> conn = DuxDB.connect(DuxDB.open())
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_boolean(stmt, 1, true)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_boolean
  """
  @spec bind_boolean(stmt, non_neg_integer, boolean) :: :ok
  def bind_boolean(_stmt, _idx, _bool), do: :erlang.nif_error(:undef)

  @doc """
  Binds a floating point number to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_double(stmt, 1, 3.14)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_double
  """
  @spec bind_double(stmt, non_neg_integer, float) :: :ok
  def bind_double(_stmt, _idx, _float), do: :erlang.nif_error(:undef)

  @doc """
  Binds a signed 64-bit integer to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_int64(stmt, 1, -3)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_int64
  """
  @spec bind_int64(stmt, non_neg_integer, integer) :: :ok
  def bind_int64(_stmt, _idx, _int), do: :erlang.nif_error(:undef)

  @doc """
  Binds an unsigned 64-bit integer to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_uint64(stmt, 1, 3)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_uint64
  """
  @spec bind_uint64(stmt, non_neg_integer, non_neg_integer) :: :ok
  def bind_uint64(_stmt, _idx, _uint), do: :erlang.nif_error(:undef)

  @doc """
  Binds a text value to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_varchar(stmt, 1, "hello, world")
      :ok

  Raises `ArgumentError` on invalid `stmt` ref.

      iex> DuxDB.bind_varchar(_not_stmt = :erlang.list_to_ref(~c"#Ref<0.0.0.0>"), 1, "hello, world")
      ** (ArgumentError) argument error: #Reference<0.0.0.0>

  (TODO improve) Raises `ArgumentError` on invalid index:

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_varchar(stmt, _bad_index = 2, "hello, world")
      ** (ArgumentError) argument error: "hello, world"

  Raises `ArgumentError` on invalid text:

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_varchar(stmt, 0, _not_text = 1)
      ** (ArgumentError) argument error: 1

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_varchar(stmt, 0, _not_printable = <<1, 2, 3>>)
      ** (ArgumentError) argument error: <<1, 2, 3>>

  See https://duckdb.org/docs/api/c/api#duckdb_bind_varchar_length
  """
  @spec bind_varchar(stmt, non_neg_integer, String.t()) :: :ok
  def bind_varchar(_stmt, _idx, _text), do: :erlang.nif_error(:undef)

  @doc """
  Binds a binary blob to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
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

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_date(stmt, 1, Date.utc_today())
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_date
  """
  @spec bind_date(stmt, non_neg_integer, Date.t() | (days :: integer)) :: :ok
  def bind_date(stmt, idx, %Date{} = date) do
    days = Date.to_gregorian_days(date) - @epoch_gregorian_days
    bind_date_nif(stmt, idx, days)
  end

  def bind_date(stmt, idx, days), do: bind_date_nif(stmt, idx, days)
  defp bind_date_nif(_stmt, _idx, _days), do: :erlang.nif_error(:undef)

  @doc """
  Binds a time (as microseconds since midnight) to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_time(stmt, 1, Time.utc_now())
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_time
  """
  @spec bind_time(stmt, non_neg_integer, Time.t() | (micros :: integer)) :: :ok
  def bind_time(stmt, idx, %Time{} = time) do
    {seconds, micros} = Time.to_seconds_after_midnight(time)
    bind_time_nif(stmt, idx, seconds * 1_000_000 + micros)
  end

  def bind_time(stmt, idx, micros), do: bind_time_nif(stmt, idx, micros)
  defp bind_time_nif(_stmt, _idx, _micros), do: :erlang.nif_error(:undef)

  {seconds, _} = NaiveDateTime.to_gregorian_seconds(~N[1970-01-01 00:00:00])
  @epoch_gregorian_seconds seconds

  @doc """
  Binds a timestamp (as microseconds since 1970-01-01 00:00:00) to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> :ok = DuxDB.bind_timestamp(stmt, 1, DateTime.utc_now())
      iex> DuxDB.bind_timestamp(stmt, 1, NaiveDateTime.utc_now())
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_timestamp
  """
  @spec bind_timestamp(
          stmt,
          non_neg_integer,
          DateTime.t() | NaiveDateTime.t() | (micros :: integer)
        ) :: :ok
  def bind_timestamp(stmt, idx, %NaiveDateTime{} = naive) do
    {seconds, micros} = NaiveDateTime.to_gregorian_seconds(naive)
    bind_timestamp_nif(stmt, idx, (seconds - @epoch_gregorian_seconds) * 1_000_000 + micros)
  end

  def bind_timestamp(stmt, idx, %DateTime{} = dt) do
    {seconds, micros} = DateTime.to_gregorian_seconds(dt)
    bind_timestamp_nif(stmt, idx, (seconds - @epoch_gregorian_seconds) * 1_000_000 + micros)
  end

  def bind_timestamp(stmt, idx, micros), do: bind_timestamp_nif(stmt, idx, micros)
  defp bind_timestamp_nif(_stmt, _idx, _micros), do: :erlang.nif_error(:undef)

  @doc """
  Binds a duckdb_interval value to the prepared statement at the specified index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_interval(stmt, 1, Duration.new!(second: 42, year: 1999))
      :ok

  See https://duckdb.org/docs/stable/clients/c/api.html#duckdb_bind_interval
  """
  @spec bind_interval(stmt, non_neg_integer, Duration.t()) :: :ok
  def bind_interval(stmt, idx, %Duration{} = duration) do
    months = duration.year * 12 + duration.month
    days = duration.week * 7 + duration.day

    micros =
      (duration.hour * 3_600 +
         duration.minute * 60 +
         duration.second) * 1_000_000 +
        elem(duration.microsecond, 0)

    bind_interval_nif(stmt, idx, months, days, micros)
  end

  defp bind_interval_nif(_stmt, _idx, _months, _days, _micros), do: :erlang.nif_error(:undef)

  @upper_base Bitwise.bsl(2, 63)

  @doc """
  Binds a HUGEINT (128 bit integer) to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_hugeint(stmt, 1, 0xFFFFFFFFFFFFFFFFF)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_hugeint
  """
  @spec bind_hugeint(stmt, non_neg_integer, integer) :: :ok
  def bind_hugeint(stmt, idx, hugeint) when hugeint >= 0 do
    upper = div(hugeint, @upper_base)
    lower = hugeint - upper * @upper_base
    bind_hugeint(stmt, idx, hugeint, upper, lower)
  end

  # TODO simplify
  def bind_hugeint(stmt, idx, hugeint) do
    upper = div(hugeint, @upper_base)
    lower = hugeint - upper * @upper_base

    if lower < 0 do
      upper = upper - 1
      lower = lower + @upper_base
      bind_hugeint(stmt, idx, hugeint, upper, lower)
    else
      bind_hugeint(stmt, idx, hugeint, upper, lower)
    end
  end

  @compile inline: [bind_hugeint: 5]
  defp bind_hugeint(stmt, idx, hugeint, upper, lower) do
    with :error <- bind_hugeint_nif(stmt, idx, upper, lower) do
      :erlang.error({:badarg, hugeint})
    end
  end

  defp bind_hugeint_nif(_stmt, _idx, _upper, _lower), do: :erlang.nif_error(:undef)

  defp unwrap_hugeints([{upper, lower} | rest]) do
    [upper * @upper_base + lower | unwrap_hugeints(rest)]
  end

  defp unwrap_hugeints([] = done), do: done

  @doc """
  Binds an UHUGEINT (128 bit unsigned integer) to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_uhugeint(stmt, 1, 0xFFFFFFFFFFFFFFFFF)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_uhugeint
  """
  @spec bind_hugeint(stmt, non_neg_integer, non_neg_integer) :: :ok
  def bind_uhugeint(stmt, idx, uhugeint) when uhugeint >= 0 do
    upper = div(uhugeint, @upper_base)
    lower = uhugeint - upper * @upper_base

    with :error <- bind_uhugeint_nif(stmt, idx, upper, lower) do
      :erlang.error({:badarg, uhugeint})
    end
  end

  defp bind_uhugeint_nif(_stmt, _idx, _upper, _lower), do: :erlang.nif_error(:undef)

  @doc """
  Binds a NULL value to the specified parameter index.

      iex> conn = DuxDB.connect(DuxDB.open())
      iex> stmt = DuxDB.prepare(conn, "SELECT ?")
      iex> DuxDB.bind_null(stmt, 1)
      :ok

  See https://duckdb.org/docs/api/c/api#duckdb_bind_boolean
  """
  @spec bind_null(stmt, non_neg_integer) :: :ok
  def bind_null(_stmt, _idx), do: :erlang.nif_error(:undef)

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
    |> :filename.join(~c"duxdb")
    |> :erlang.load_nif(0)
  end
end
