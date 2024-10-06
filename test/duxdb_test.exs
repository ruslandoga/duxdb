defmodule DuxDBTest do
  use ExUnit.Case, async: true

  test "library_version/0" do
    assert DuxDB.library_version() == "v1.1.1"
  end

  # test "it works" do
  #   db = DuxDB.open(":memory:")
  #   on_exit(fn -> DuxDB.close(db) end)
  #   conn = DuxDB.connect(db)
  #   on_exit(fn -> DuxDB.disconnect(conn) end)
  #   assert is_reference(conn)
  # end
end
