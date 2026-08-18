# CLAUDE.md

Guidance for AI assistants working in this repository.

## What this project is

Pathway (the "Live Data Framework") is a data processing framework for streaming
and batch pipelines. It is a **two-layer system**:

- A **Rust engine** (`src/`, ~73k LOC) built on vendored forks of
  [timely-dataflow](external/timely-dataflow) and
  [differential-dataflow](external/differential-dataflow). It does incremental
  computation, connector I/O, persistence, and external indexing.
- A **Python API** (`python/pathway/`, ~129k LOC) that users write pipelines
  against. It builds a computation graph, then hands it to the engine to run.

The two layers are bridged by PyO3. The Rust crate is `pathway_engine` and is
exposed to Python as the `pathway.engine` extension module (`[tool.maturin]
module-name = "pathway.engine"`). The Python-side type stub for that module is
`python/pathway/engine.pyi` — it is hand-maintained and must be kept in sync
with `src/python_api.rs`.

Version lives in **`Cargo.toml`** (`version = "0.31.1"`); the Python
`__version__` is generated at build time (`python/pathway/internals/version.py`
is a placeholder reading `"UNKNOWN"` in a source checkout). License is BUSL-1.1.

## Repository layout

```
src/                          Rust engine
  lib.rs                      crate root; global lint config; jemalloc allocator
  python_api.rs               PyO3 boundary (~8.5k lines): Scope, Table, Pointer, expressions
  python_api/                 logging, threads, external index wrappers
  engine/
    graph.rs                  the `Graph` trait — the engine's operator surface
    dataflow.rs               timely/differential implementation of `Graph` (~7.5k lines)
    dataflow/operators/       custom operators (stateful_reduce, prev_next, time_column, ...)
    expression.rs, value.rs   expression AST and the `Value`/`Type` model
    reduce.rs                 reducers
    telemetry.rs, license.rs  monitoring, licensing/entitlements
    http_server.rs, timestamp.rs, error.rs
  connectors/
    data_storage/             per-backend readers/writers (kafka, postgres, s3, delta, iceberg, ...)
    data_format/              parsers/formatters (dsv, json, debezium, bson, ...)
    metadata/                 per-source metadata columns
    synchronization.rs, backlog.rs, offset.rs, monitoring.rs
  persistence/                snapshots, frontiers, checkpointing; backends/ (file, s3, azure, mock)
  external_integration/       vector/text indices (usearch, tantivy, qdrant, brute-force KNN)

python/pathway/
  __init__.py                 the public `pw.*` namespace (re-exports; keep `__all__` in sync)
  _engine_finder.py           dev-time magic: importing pathway runs `cargo build` (see below)
  engine.pyi                  stub for the Rust extension module
  internals/                  the core: graph building and evaluation
    parse_graph.py            `G`, the global ParseGraph of operators and scopes
    operator.py               Operator classes (Input/Output/Contextualized/Iterate/RowTransformer)
    decorators.py             `@contextualized_operator`, transformer decorators
    table.py, expression.py, column.py, schema.py, dtype.py
    graph_runner/             ParseGraph -> engine: operator_handler, expression_evaluator,
                              path_evaluator, storage_graph, scope_context, state
    udfs/, expressions/, sql/, shadows/, utils/
    api.py                    re-exports from pathway.engine (flake8 F403/F405 exempt)
    config.py                 PathwayConfig — all PATHWAY_* env vars
  io/                         ~45 connector packages (fs, kafka, s3, postgres, deltalake, ...)
  stdlib/                     temporal, indexing, ml, graphs, ordered, stateful, statistical, viz, utils
  xpacks/llm/                 LLM/RAG toolkit: llms, embedders, parsers, splitters, rerankers,
                              document_store, question_answering, servers, mcp_server
  xpacks/connectors/          sharepoint
  debug/                      `pw.debug.compute_and_print`, `table_from_markdown`, ...
  tests/                      unit tests (in-package, shipped in the wheel)
  conftest.py                 autouse fixtures (graph teardown, fake credentials, config isolation)

tests/integration/            Rust integration tests (single `main.rs` test target, 30+ modules)
tests/data/                   fixtures for Rust tests
integration_tests/            Python integration tests needing real services (db_connectors,
                              kafka, s3, gdrive, iceberg, sharepoint, wordcount, rag_evals, ...)
external/                     vendored timely-dataflow and differential-dataflow forks
examples/                     notebooks, templates, showcases — AUTO-REFRESHED, see Gotchas
docs/                         website content (many files are generated / gitignored)
library_licenses/             third-party license texts copied into wheels at release
.github/workflows/            pull.yml (lint+test gates), package_test.yml, release.yml
```

## Architecture: how a pipeline runs

Understanding this flow is essential for almost any non-trivial change.

1. **User code builds a graph, it does not compute.** `pw.Table` operations
   append operators to a global `ParseGraph` (`internals/parse_graph.py`,
   instance `G`). Python-level operators are ordinary functions decorated with
   `@contextualized_operator` (`internals/decorators.py`), which wraps the call
   into a `ContextualizedIntermediateOperator` node. Expressions
   (`ColumnExpression`) are ASTs, not values.
2. **`pw.run()` / `pw.run_all()` invokes `GraphRunner`**
   (`internals/graph_runner/__init__.py`). It resolves config
   (`internals/config.py`), plans column storage (`storage_graph.py`,
   `path_evaluator.py`), and walks operators in dependency order.
3. **`OperatorHandler` subclasses dispatch each operator type** to engine calls
   (`graph_runner/operator_handler.py`; subclasses self-register via
   `__init_subclass__(operator_type=...)`). Expressions are lowered by
   `expression_evaluator.py` into engine expressions.
4. **The engine side is the `Scope` object** (`src/python_api.rs`, stubbed in
   `engine.pyi`). Its methods correspond to the `Graph` trait
   (`src/engine/graph.rs`), implemented for real by
   `src/engine/dataflow.rs` on top of timely/differential.
5. **Data flows as `(Key, Value, Timestamp, diff)`** — differential dataflow
   collections. `Key` is `pw.Pointer`; row identity matters everywhere
   (`.ix`, `join` with `id=...`, `with_universe_of` all carry key contracts).
   Connectors run on their own threads and push into the dataflow; persistence
   snapshots operator state and input offsets so a restart resumes consistently.

Consequences worth remembering:

- A change in engine behavior usually needs edits in **four places**:
  `src/engine/graph.rs` (trait), `src/engine/dataflow.rs` (implementation),
  `src/python_api.rs` (PyO3 wrapper), `python/pathway/engine.pyi` (stub) —
  plus the Python operator and its `graph_runner` handler.
- Time consistency is a correctness property, not a nicety. Many past bug
  fixes (see CHANGELOG) are about all inputs entering at one shared timestamp,
  or checkpoints not certifying another run's partial state. Be careful when
  touching timestamps, frontiers, or persistence commits.

## Development environment

Required: **Rust 1.96** (pinned in `rust-toolchain.toml`), **Python ≥ 3.10**
(CI lints on 3.11, builds wheels on 3.10, tests 3.10 + 3.11). Build backend is
maturin. `cargo` is available in this container; `maturin` and an installed
`pathway` wheel typically are **not** — check before assuming you can `import
pathway`.

### The `_engine_finder` trick (important)

`python/pathway/_engine_finder.py` installs a meta path finder: importing
`pathway` from a source checkout runs `cargo --locked build --lib` and loads the
freshly built `.so` as `pathway.engine`. So you do **not** need maturin to run
Python tests against local Rust changes — but the first import compiles the
whole engine (slow). Controlled by env vars:

- `PATHWAY_PROFILE` — cargo profile (default `dev`; `dev` uses `opt-level = 3`,
  so it is not fast to build; other profiles: `release`, `profiling`, `debugging`)
- `PATHWAY_FEATURES` — cargo features (e.g. `enterprise`, `unlimited-workers`)
- `PATHWAY_QUIET` — pass `--quiet` to cargo

For a real wheel: `maturin develop` (dev install) or `maturin build --release`.

## Commands

### Lint gates (exactly what CI `pull.yml` runs — all must pass)

```bash
black . --check          # black >=24,<25, line-length 88 (excludes docs/, target/)
flake8                   # flake8 >=7,<8, max-line-length 119, google docstrings
isort --check .          # black profile, known_first_party = pathway
mypy .                   # mypy >=1.10,<1.11, python_version 3.11
cargo fmt -- --check
cargo clippy --locked --all-targets -- --deny warnings
cargo test --locked
```

Note the two different Python line lengths: black formats to **88**, flake8
only fails above **119**. Format with black; don't hand-wrap to 119.

`mypy` excludes `target/`, `examples/`, and `tests/**/test_*.py`, but is
otherwise strict (`check_untyped_defs`, `warn_unused_ignores`,
`strict_equality`, `warn_redundant_casts`). Some modules opt into stricter
checks with a file-level `# mypy: disallow-untyped-defs, extra-checks,
disallow-any-generics, warn-return-any` comment — keep those satisfied.
To reproduce the mypy job's dependency set: `python ./.github/ci/get-dependencies.py
&& pip install -r requirements.txt`.

### Tests

```bash
# Python unit tests (from the repo root; triggers a cargo build on first import)
python -m pytest python/pathway/tests
python -m pytest python/pathway/tests/test_common.py -k some_test

# Doctests are part of the suite — CI runs the installed package like this:
python -m pytest --doctest-modules --pyargs pathway

# Parallel, as CI does
PYTEST_ADDOPTS="--dist worksteal -n auto --timeout=900" python -m pytest --pyargs pathway

# Rust: unit tests are disabled on the lib target ([lib] test = false);
# real tests live in tests/integration/ (auto-discovered target `integration`)
cargo test --locked
cargo test --locked --test integration test_dsv

# Python integration tests need live services / credentials — do not expect
# these to pass locally without them
python -m pytest integration_tests/db_connectors -k postgres
```

Useful test env vars: `PATHWAY_THREADS` (some tests are
`xfail_on_multiple_threads`), `PATHWAY_IGNORE_ASSERTS`,
`PATHWAY_RUNTIME_TYPECHECKING`, `PATHWAY_TERMINATE_ON_ERROR`,
`PATHWAY_PERSISTENCE_MODE`/`PATHWAY_REPLAY_STORAGE`/`PATHWAY_SNAPSHOT_ACCESS`
(replay), `PATHWAY_MONITORING_SERVER`, `PATHWAY_LICENSE_KEY`. The full list of
Python-visible ones is `PathwayConfig` in `python/pathway/internals/config.py`.

### Running a pipeline with multiple workers

```bash
pathway spawn -n 4 python my_pipeline.py     # CLI entry point: pathway.cli:main
```

## Conventions

### Testing style (follow it closely — the suite is very uniform)

Tests are written against `python/pathway/tests/utils.py`. The dominant idiom:

```python
import pathway as pw
from pathway.tests.utils import T, assert_table_equality

def test_something():
    t = T(
        """
        a | b
        1 | 2
        3 | 4
        """
    )
    result = t.select(c=t.a + t.b)
    assert_table_equality(result, T("""c\n3\n7"""))
```

- `T(...)` builds a table from a markdown block (`format="pandas"` for a
  DataFrame). Use `T` in tests, not `pw.debug.table_from_markdown`.
- Pick the right assertion: `assert_table_equality` (most common, checks types
  and row ids), `assert_table_equality_wo_index`, `assert_table_equality_wo_types`,
  `assert_table_equality_wo_index_types`. The `wo_types` variants **assert that
  they are needed** — don't use them where the strict one would pass.
- Streaming tests use `assert_stream_equal` / `DiffEntry`,
  `assert_key_entries_in_stream_consistent`, `assert_split_into_time_groups`,
  or `wait_result_with_checker` with a checker class
  (`FileLinesNumberChecker`, `CsvPathwayChecker`, `LogicChecker`).
- To execute a graph, use `run` / `run_all` from `pathway.tests.utils` rather
  than `pw.run` (they default to `debug=True` and monitoring off), and markers
  from the same module:
  `needs_multiprocessing_fork`, `xfail_on_multiple_threads`,
  `only_standard_build`, `xfail_on_arg`.
- `conftest.py` autouse fixtures clear the global `ParseGraph` after each test,
  inject fake credentials, and **assert the environment did not change during
  the test** — mutate `os.environ` only via `monkeypatch`, or mark the test
  `@pytest.mark.environment_changes`.
- Public API docstrings contain doctests that CI executes
  (`pytest --doctest-modules`). When you change a signature or user-visible
  output, fix the docstring examples too.

### Rust

- `src/lib.rs` sets `#![warn(clippy::pedantic)]` and `#![warn(clippy::cargo)]`,
  and CI runs clippy with `--deny warnings`. Write pedantic-clean code.
- `clippy.toml` **disallows several differential-dataflow methods** in favor of
  Pathway wrappers — e.g. use `crate::dataflow::operators::MaybeTotal::count`
  instead of `Count::count`, `MapWrapped::map_ex`/`map_named` instead of
  `Collection::map`, `ArrangeWithTypes::arrange` instead of `Arrange::arrange`,
  and `JoinCore::join_core` instead of any `Join::join*`. The named wrappers
  exist because they handle non-total orders and operator naming; the clippy
  error message tells you the replacement.
- `cargo deny` restricts dependency licenses (`deny.toml`); vendored forks are
  excluded. Adding a dependency with an unusual license will fail that check.
- Keep `Cargo.lock` committed and consistent — CI builds with `--locked`.

### Both languages

- Files start with a copyright header: `# Copyright © 2026 Pathway` (Python) or
  `// Copyright © 2026 Pathway` (Rust). Follow the convention in new files.
- Python modules use `from __future__ import annotations`.
- Public connector/API functions are decorated with `@check_arg_types` and
  `@trace_user_frame` (see `python/pathway/io/fs/__init__.py` for the canonical
  shape) so user errors point at user code.

### CHANGELOG

`CHANGELOG.md` follows [Keep a Changelog] with `### Added` / `### Changed` /
`### Fixed` under `## [Unreleased]`. **Every user-visible change gets an entry**,
and the house style is a full, self-contained paragraph: what the API is, its
parameters, the behavior, and — for a fix — what went wrong and why. Breaking
changes are prefixed `**BREAKING**:` and include a migration line. Match the
surrounding entries' depth; one-liners are the exception, not the rule.

## Common tasks

**Adding an input/output connector.** Rust side: a reader/writer in
`src/connectors/data_storage/<backend>.rs` (register in the module's
`mod.rs`, plus `metadata/` if it exposes metadata), then a PyO3-visible
settings class and a `storage_type` string in `src/python_api.rs`, mirrored in
`engine.pyi`. Python side: `python/pathway/io/<name>/__init__.py` exposing
`read`/`write` that build `api.DataStorage(storage_type=...)` plus a
`datasource.GenericDataSource` / `datasink.GenericDataSink`. Add unit tests in
`python/pathway/tests/test_io*.py`, service-dependent tests in
`integration_tests/db_connectors/`, and a CHANGELOG entry.

**Adding a table operator.** Usually a `@contextualized_operator` method on
`Table` in `internals/table.py` (or a `stdlib/` module), lowered by
`graph_runner/expression_evaluator.py` or an existing engine call. Only if no
engine primitive fits do you add one — that is the four-place change described
in Architecture above, plus a custom operator under
`src/engine/dataflow/operators/`.

**Adding an LLM/RAG component.** `python/pathway/xpacks/llm/` — UDF-based
wrappers (`llms.py`, `embedders.py`, `parsers.py`, `splitters.py`,
`rerankers.py`) and pipelines (`document_store.py`, `question_answering.py`,
`servers.py`). Heavy dependencies must be imported lazily via
`pathway.optional_import.optional_imports` so that `import
pathway.xpacks.llm` works with only the `xpack-llm` extra installed; declare
the dependency in the right `pyproject.toml` extra
(`xpack-llm`, `xpack-llm-local`, `xpack-llm-docs`, `twelvelabs`, ...).

**Exposing something new as `pw.<name>`.** Add it to the imports **and** the
`__all__` list in `python/pathway/__init__.py`; isort/flake8 will complain
otherwise.

## Gotchas

- **`examples/` is auto-refreshed.** The recent commit history is dominated by
  bot commits titled "Daily Pathway examples refresh" that regenerate notebooks
  under `examples/notebooks/`. Don't hand-edit generated notebooks; change the
  source under `docs/` instead.
- **`docs/` is partly generated and gitignored** (`docs/**/article.md`,
  `?.API-docs`, `?.documentation`, template `_readmes`). Check `.gitignore`
  before creating files there.
- **`external/` holds vendored forks** of timely and differential dataflow, used
  via `path` dependencies. Treat them as upstream code: don't refactor them
  casually, and don't assume upstream APIs match.
- **Enterprise / licensing.** Cargo features `enterprise` and
  `unlimited-workers` gate worker counts; some functionality checks
  entitlements against `PATHWAY_LICENSE_KEY`
  (`internals/config.py::_check_entitlements`, `src/engine/license.rs`). Tests
  that only work on the open build use the `only_standard_build` marker.
- **Two chrono-tz majors coexist on purpose** (`chrono-tz` 0.10 and the aliased
  `chrono-tz-0_8` for the ClickHouse client) — see the comment in `Cargo.toml`;
  don't "clean this up".
- **`tantivy` is pinned** with a comment not to bump before a RAG integration
  test failure is investigated. Respect pins that carry comments.
- Some `pyproject.toml` pins are deliberate workarounds for upstream bugs
  (`sqlglot == 10.6.1`, `tenacity != 8.4.0`, capped `deltalake`, `beartype`).
  Check for a comment before relaxing a bound.
- The dev cargo profile uses `opt-level = 3`, so builds are slow. Expect the
  first `import pathway` in a fresh checkout to take a long time; don't
  interpret it as a hang.

## Contribution workflow

- Fork + PR against `main`. Discuss non-trivial changes via an issue or Discord
  first (see `CONTRIBUTING.md`); a CLA is required.
- `.github/pull_request_template.md` asks for context, how it was tested,
  the type of change, related issues, and confirmation that
  **`CHANGELOG.md` was updated**.
- All the lint/test gates in `.github/workflows/pull.yml` run on every PR;
  run them locally before pushing.
