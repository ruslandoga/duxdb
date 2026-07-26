defmodule DuxDB.MixProject do
  use Mix.Project

  @version "0.1.0"
  @github_url "https://github.com/ruslandoga/duxdb"

  def project do
    [
      app: :duxdb,
      version: @version,
      elixir: "~> 1.17",
      compilers: [:elixir_make | Mix.compilers()],
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url: "#{@github_url}/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "duxdb",
      make_precompiler_priv_paths: ["duxdb.*", "libduckdb.*"],
      make_precompiler_nif_versions: [versions: ["2.17"]],
      cc_precompiler: [
        cleanup: "clean",
        only_listed_targets: true,
        compilers: %{
          {:unix, :darwin} => %{
            "aarch64-apple-darwin" => {"/usr/bin/clang", "/usr/bin/clang++"}
          },
          {:unix, :linux} => %{
            "aarch64-linux-gnu" => {"aarch64-linux-gnu-gcc", "aarch64-linux-gnu-g++"},
            "x86_64-linux-gnu" => {"x86_64-linux-gnu-gcc", "x86_64-linux-gnu-g++"}
          }
        }
      ],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # dialyzer
      dialyzer: [
        plt_local_path: "plts",
        plt_core_path: "plts"
      ]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.8", runtime: false},
      {:cc_precompiler, "~> 0.1.8 or ~> 0.2", runtime: false},
      {:stream_data, "~> 1.1", only: :test},
      {:ex_doc, "~> 0.34", only: :docs},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
