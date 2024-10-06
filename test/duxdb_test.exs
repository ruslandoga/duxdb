defmodule DuxDBTest do
  use ExUnit.Case, async: true

  test "it works" do
    db = DuxDB.open(":memory:")
    on_exit(fn -> DuxDB.close(db) end)
    conn = DuxDB.connect(db)
    on_exit(fn -> DuxDB.disconnect(conn) end)
    assert is_reference(conn)
  end
end
