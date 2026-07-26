defmodule DuxDB.MixProject do
  use Mix.Project

  @version "0.1.0-rc.1"
  @github_url "https://github.com/ruslandoga/duxdb"

  def project do
    [
      app: :duxdb,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:elixir_make | Mix.compilers()],
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url: "#{@github_url}/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "duxdb",
      make_precompiler_priv_paths: ["duxdb.*", "libduckdb.*"],
      make_precompiler_nif_versions: [versions: ["2.18"]],
      cc_precompiler: [
        cleanup: "clean",
        only_listed_targets: false,
        compilers: %{
          {:unix, :darwin} => %{
            "aarch64-apple-darwin" => {"/usr/bin/clang", "/usr/bin/clang++"}
          },
          {:unix, :linux} => %{
            "aarch64-linux-gnu" => {"aarch64-linux-gnu-gcc", "aarch64-linux-gnu-g++"},
            "aarch64-linux-musl" =>
              {"aarch64-alpine-linux-musl-gcc", "aarch64-alpine-linux-musl-g++"},
            "x86_64-linux-gnu" => {"x86_64-linux-gnu-gcc", "x86_64-linux-gnu-g++"},
            "x86_64-linux-musl" =>
              {"x86_64-alpine-linux-musl-gcc", "x86_64-alpine-linux-musl-g++"}
          }
        }
      ],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [ignore_modules: [Help]],
      dialyzer: [plt_local_path: "plts", plt_core_path: "plts"]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:elixir_make, "~> 0.8", runtime: false},
      {:cc_precompiler, "~> 0.1.8 or ~> 0.2", runtime: false},
      {:stream_data, "~> 1.1", only: :test},
      {:ex_doc, "~> 0.34", only: :docs},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:req, "~> 0.6.3", only: [:dev, :test]},
      {:req_s3, "~> 0.2.4", only: [:dev, :test]}
    ]
  end
end
