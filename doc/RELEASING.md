# Releasing DuxDB

This guide covers GitHub prereleases, native artifacts, checksums, and Hex
publishing. Run release commands from a clean checkout of `master`.

## Release model

A release has three related versions that must agree:

- `@version` in `mix.exs`
- the Git tag, prefixed with `v`
- the version in every precompiled artifact filename

For example, `@version "0.1.0-rc.1"` must use the tag
`v0.1.0-rc.1`.

The release workflow builds these artifacts:

- `aarch64-apple-darwin`
- `aarch64-linux-gnu`
- `aarch64-linux-musl`
- `x86_64-linux-gnu`
- `x86_64-linux-musl`

ARM artifacts are built on native ARM runners. AMD64 artifacts are built on
native AMD64 runners. The artifacts bundle DuckDB's shared library.

## Important rules

1. Bump the DuxDB version whenever native NIF code or its Elixir interface
   changes. Otherwise, users may download an older binary that does not match
   the current Elixir modules.
2. Do not move or replace a published tag.
3. Do not pin GitHub installations to the release tag. The tag is created
   before `checksum.exs` is generated, so the tagged source does not contain
   the checksum manifest. During development, install from `master`. Published
   users should install the immutable Hex package.
4. Do not publish to Hex until all five GitHub artifacts exist and
   `checksum.exs` has been generated.

## 1. Prepare the release

Create a branch from the latest `master`:

```sh
git checkout master
git pull --ff-only origin master
git checkout -b release/v0.1.0
```

Update `@version` in `mix.exs`:

```elixir
@version "0.1.0"
```

For a release candidate, use a SemVer prerelease:

```elixir
@version "0.1.0-rc.1"
```

If upgrading DuckDB, update `DUCKDB_VERSION` in both GitHub workflows and
update DuckDB-specific doctest expectations. Confirm the upstream DuckDB
release contains the five library archives required by the build matrix.

Run the project checks against the intended DuckDB release:

```sh
mix format --check-formatted
mix test
git diff --check
```

Commit, push, open a pull request, and merge it. Do not release from an
unmerged branch.

## 2. Create the GitHub release

Refresh the local `master` and verify the version:

```sh
git checkout master
git pull --ff-only origin master
grep '@version' mix.exs
```

For a prerelease:

```sh
gh release create v0.1.0-rc.1 \
  --target master \
  --prerelease \
  --title "DuxDB v0.1.0-rc.1" \
  --generate-notes
```

For a stable release:

```sh
gh release create v0.1.0 \
  --target master \
  --title "DuxDB v0.1.0" \
  --generate-notes
```

Creating the release also creates the tag and triggers the `precompile`
workflow.

## 3. Verify native artifacts and checksums

Open the workflow run:

```sh
gh run list --workflow precompile.yml --limit 5
```

All five platform jobs must pass. The GitHub release must contain five
`.tar.gz` files and five matching `.sha256` files:

```sh
gh release view v0.1.0
```

After all native jobs pass, the workflow's `checksums` job:

1. verifies that the tag version matches `@version`;
2. runs `mix elixir_make.checksum --all`;
3. commits the resulting `checksum.exs` to `master`.

Wait for that bot commit, then update the local checkout:

```sh
git pull --ff-only origin master
git log -1 --oneline
git diff HEAD^ -- checksum.exs
```

If checksum automation fails, generate and commit the file manually:

```sh
MIX_ENV=prod mix deps.get
MIX_ENV=prod mix elixir_make.checksum --all
git add checksum.exs
git commit -m "Add v0.1.0 artifact checksums"
git push origin master
```

## 4. Build and inspect the Hex package

The project must have Hex package metadata, including its description,
license, links, and included files, before the first Hex release.

Build and inspect the exact package:

```sh
MIX_ENV=prod mix deps.get
MIX_ENV=prod mix hex.build --unpack
MIX_ENV=prod mix hex.publish --dry-run
```

Confirm that the package contains at least:

- `lib`
- `c_src`
- `Makefile`
- `mix.exs`
- `README.md`
- the license file
- `checksum.exs`

It must not contain compiled NIFs, downloaded DuckDB libraries, `_build`,
`deps`, or local credentials.

## 5. Publish to Hex

For the first release, publish manually so warnings can be reviewed:

```sh
MIX_ENV=prod mix hex.publish
```

Two-factor authentication may be requested during interactive account
operations. CI publishing does not require an interactive 2FA prompt. It uses
a dedicated key with `api:write` permission through `HEX_API_KEY`.

Once manual publishing is proven, GitHub Actions may run:

```sh
MIX_ENV=prod mix hex.publish --yes
```

Store `HEX_API_KEY` in a protected GitHub environment such as `hex-release`.
Require manual environment approval before the publish job.

## 6. Verify installation

Test in a new temporary directory so no local build or dependency cache masks
problems.

For a prerelease:

```sh
cd "$(mktemp -d)"
elixir -e '
Mix.install([{:duxdb, "0.1.0-rc.1"}])
IO.puts(DuxDB.library_version())
'
```

For a stable release:

```sh
cd "$(mktemp -d)"
elixir -e '
Mix.install([{:duxdb, "~> 0.1.0"}])
IO.puts(DuxDB.library_version())
'
```

Also test the development installation from `master`:

```sh
cd "$(mktemp -d)"
elixir -e '
Mix.install([{:duxdb, github: "ruslandoga/duxdb"}])
IO.puts(DuxDB.library_version())
'
```

The reported DuckDB version must match the version bundled by the release
workflow.

## Recovery

If native artifact creation fails, do not publish to Hex. Fix the build,
increment the prerelease version, and create a new tag. Do not replace an
existing tag or artifact.

If a Hex package was published incorrectly, consult `mix help hex.publish`
immediately. Hex only permits replacing or reverting public releases for a
limited time.
