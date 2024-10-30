defmodule DuxDB.MixProject do
  use Mix.Project

  def project do
    [
      app: :duxdb,
      version: "0.1.0",
      elixir: "~> 1.17",
      compilers: [:elixir_make | Mix.compilers()],
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.8", runtime: false},
      {:stream_data, "~> 1.1", only: :test},
      {:ex_doc, "~> 0.34", only: :docs}
    ]
  end
end
