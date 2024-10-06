defmodule DuxDB do
  @moduledoc "DuxDB in Elixir"

  @type db :: reference
  @type conn :: reference
  @type stmt :: reference
  @type value :: binary | number | nil
  @type row :: [value]
  @type type ::
          :i8
          | :i16
          | :i32
          | :i64
          | :i128
          | :u8
          | :u16
          | :u32
          | :u64
          | :f32
          | :f64
          | :bool
          | :date
          | :time
          | :interval
          | :uuid
          | :enum

  @spec library_version :: String.t()
  def library_version, do: :erlang.nif_error(:undef)

  @spec open(Path.t()) :: db
  def open(path) do
    dirty_io_open_nif(to_charlist(path))
  end

  # @spec open(Path.t(), Keyword.t()) :: db
  # def open(path, config) do
  # end

  # @spec close(db) :: :ok
  # def close(db) do
  #   dirty_io_close_nif(db)
  # end

  # @spec connect(db) :: conn
  # def connect(_db), do: :erlang.nif_error(:undef)

  # @spec interrupt(conn) :: :ok
  # def interrupt(_conn), do: :erlang.nif_error(:undef)

  # def query_progress(_conn), do: :erlang.nif_error(:undef)

  # @spec disconnect(conn) :: :ok
  # def disconnect(_conn), do: :erlang.nif_error(:undef)

  @compile {:autoload, false}
  @on_load {:load_nif, 0}

  @doc false
  def load_nif do
    :code.priv_dir(:duxdb)
    |> :filename.join(~c"duxdb_nif")
    |> :erlang.load_nif(0)
  end

  defp dirty_io_open_nif(_path), do: :erlang.nif_error(:undef)
  # defp dirty_io_close_nif(_db), do: :erlang.nif_error(:undef)
end
