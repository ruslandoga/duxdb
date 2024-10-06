defmodule DuxDB do
  @moduledoc "DuxDB in Elixir"

  @type db :: reference
  @type conn :: reference
  @type stmt :: reference
  @type config :: reference
  @type result :: reference
  @type value :: binary | number | nil
  @type row :: [value]

  @spec library_version :: String.t()
  def library_version, do: :erlang.nif_error(:undef)

  @spec open(Path.t()) :: db
  def open(path), do: dirty_io_open_nif(<<path::binary, 0>>)

  # @spec open(Path.t(), config) :: db
  # def open(path, config) do
  # end

  @spec close(db) :: :ok
  def close(db), do: dirty_io_close_nif(db)

  @spec connect(db) :: conn
  def connect(_db), do: :erlang.nif_error(:undef)

  @spec disconnect(conn) :: :ok
  def disconnect(_conn), do: :erlang.nif_error(:undef)

  # def query(conn, sql) do
  #   dirty_io_query_nif(conn, to_charlist(sql))
  # end

  @compile {:autoload, false}
  @on_load {:load_nif, 0}

  @doc false
  def load_nif do
    :code.priv_dir(:duxdb)
    |> :filename.join(~c"duxdb_nif")
    |> :erlang.load_nif(0)
  end

  defp dirty_io_open_nif(_path), do: :erlang.nif_error(:undef)
  defp dirty_io_close_nif(_db), do: :erlang.nif_error(:undef)
end
