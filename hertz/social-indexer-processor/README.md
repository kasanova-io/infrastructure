# Social processor profile ordering

This build applies the profile ordering patch to clean upstream
`917863f49623aa6802e5ad4c0e246581470d93cb`. It reuses the native payload parser,
signature verification, schema and runtime. Networks and endpoints remain
configured by the separate DEV and PROD Compose projects.

A profile replaces its incumbent only when its chain block time is strictly
later. Exact transaction duplicates do nothing. Different transaction IDs with
equal block time retain the incumbent because their order is unknown. This is
an explicit conservative tie policy. The transaction takes the table lock
before the owner lock and reads again after any lock wait; the additive profile
importer takes only its table lock.

Run `python3 prepare-processor-build.py` from this directory. It requires the
actual accepted `Cargo.lock`; no dependency lock is invented or resolved during
context preparation. It refuses an occupied destination, changed upstream pin
or tracked source, and symlink inputs. Only tracked processor sources, the lock,
patch and public test assets enter the context. Untracked files and credentials
are excluded. The emitted `source-binding.json` records every file's hash.

The immutable Rust and Alpine bases use the same upstream build convention.
All Cargo builds use `--locked`. The builder compiles the six production-method
profile tests and the 24 upstream hashtag tests. Run the exact compiled test
artifact against an isolated PostgreSQL fixture with `--test-threads=1` and no
test selector; profile tests share a table and must run serially. Require all
30 tests to pass with zero ignored or filtered tests. Bind these results, actual
lock/source hashes and peer preservation to the candidate image before activation. The final runtime copies
only the production binary; fixtures, tests, dependency lock and source stay in
the builder/evidence.

Select the tested immutable runtime image ID through `SOCIAL_PROCESSOR_IMAGE`
for both DEV and PROD. Keep their current network, endpoint, database volume,
worker count and resource settings. A processor activation recreates only that
service with `--no-deps --no-build`; it requires separate reviewed runtime and
data-boundary checks. An isolated build is not a deployed feature or proof that
historical profiles were imported.

The actual generated dependency lock is retained and source preparation passed.
The original isolated build passed six profile tests but filtered 24 upstream
tests, so its acceptance failed. A separately reviewed supplement subsequently
passed all 30 tests without ignored or filtered cases and packaged the exact
compiled binary; its five-peer database/runtime preservation checks passed.
Those results remain bound to that retained native source, lock and binary.
This source-only integration has not undergone a fresh integrated `--locked`
build, processor activation or historical profile import.
