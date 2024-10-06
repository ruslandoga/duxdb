defmodule DuxDBTest do
  use ExUnit.Case, async: true
  doctest DuxDB

  describe "destroy_config/1" do
    setup do
      {:ok, config: DuxDB.create_config()}
    end

    test "is no-op if already destroyed", %{config: config} do
      assert :ok == DuxDB.destroy_config(config)
      assert :ok == DuxDB.destroy_config(config)
    end

    test "fails future operations", %{config: config} do
      assert :ok == DuxDB.destroy_config(config)
      assert_raise ArgumentError, fn -> DuxDB.set_config(config, "access_mode", "READ_WRITE") end
      assert_raise ArgumentError, fn -> DuxDB.open_ext(":memory:", config) end
    end
  end

  describe "open_ext/2" do
    test "fails on broken config" do
      config = DuxDB.create_config()
      assert :ok == DuxDB.set_config(config, "sdf", "asdf")

      assert_raise RuntimeError,
                   "Invalid Input Error: The following options were not recognized: sdf",
                   fn -> DuxDB.open_ext(":memory:", config) end
    end

    test "fails on invalid path" do
      assert_raise RuntimeError,
                   "IO Error: Cannot open file \"tmp/somewhere/test.db\": No such file or directory",
                   fn -> DuxDB.open_ext("tmp/somewhere/test.db", DuxDB.create_config()) end
    end
  end

  describe "close/1" do
    setup do
      {:ok, db: DuxDB.open_ext(":memory:", DuxDB.create_config())}
    end

    test "is no-op if already closed", %{db: db} do
      assert :ok == DuxDB.close(db)
      assert :ok == DuxDB.close(db)
    end

    test "fails future operations", %{db: db} do
      assert :ok == DuxDB.close(db)
      assert_raise ArgumentError, fn -> DuxDB.connect(db) end
    end
  end

  describe "disconnect/1" do
    setup do
      {:ok, conn: DuxDB.connect(DuxDB.open_ext(":memory:", DuxDB.create_config()))}
    end

    test "is no-op if already disconnected", %{conn: conn} do
      assert :ok == DuxDB.disconnect(conn)
      assert :ok == DuxDB.disconnect(conn)
    end
  end
end
