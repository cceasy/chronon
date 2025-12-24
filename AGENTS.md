# Repository Guidelines

## Project Structure & Modules
- Scala services live in top-level modules such as `api`, `aggregator`, `service`, `spark`, and `flink`; shared utilities sit in `service_commons` and `online`.
- Python tooling is under `python/` with packaging and entrypoints managed by Mill.
- Infrastructure and deployment assets reside in `docker/`, `cloud_aws/`, `cloud_gcp/`, and helper scripts in `scripts/`.
- Docs and design notes live in `docs/` and `devnotes.md`; build definitions are in `build.mill` plus per-module `package.mill` files.

## Build, Test, and Development Commands
- Install prerequisites: Thrift 0.22, Java 11, Scala 2.12, Python 3.11+.
- Compile all Scala modules: `./mill __.compile`.
- Format Scala code: `./mill __.reformat`.
- Build Scala assemblies: `./mill __.assembly`.
- Run Scala tests (pattern match): `./mill spark.test.testOnly "ai.chronon.spark.analyzer.*"`; run module tests: `./mill api.test`.
- Python lint: `./mill python.ruffCheck --fix`; Python tests: `./mill python.test`; coverage: `./mill python.test.coverageReport --omit='**/test/**,**/out/**'`.
- Build Python artifacts: `./mill python.wheel`, `./mill python.bundle`, or `./mill python.installEditable` for local development.

## Coding Style & Naming Conventions
- Follow existing Scala package naming (e.g., `ai.chronon.*`) and Python module names mirroring directory paths.
- Prefer descriptive identifiers; avoid single-letter names outside short loops.
- Maintain two-space indentation for Scala and standard PEP8-compatible formatting for Python (enforced by ruff).
- Run `./mill __.reformat` before commits to satisfy scalafmt checks; keep imports organized automatically by the formatter.

## Testing Guidelines
- Keep tests close to their modules (e.g., `spark/test`, `api/test`, `python/test`).
- Name Scala tests with `*Test` suffix and Python tests as `test_*.py`.
- When adding features, provide targeted unit tests and run the closest module test target (`./mill <module>.test` or `./mill python.test`).

## Commit & Pull Request Guidelines
- Write clear commit messages summarizing the change and scope; align with existing style (imperative mood, concise subject).
- Always format code (`./mill __.reformat` and `./mill python.ruffCheck --fix`) and rerun relevant tests before pushing.
- Include context in PRs: purpose, impacted modules, test commands executed, and any deployment or data implications; attach screenshots only when UI artifacts are affected.
- Optional helper: define `alias mill_scalafmt='./mill __.reformat'` and use the `zpush` function from `devnotes.md` to format, commit, and push in one step.

## Security & Configuration Tips
- Prefer provided dependencies where possible (see `package.mill` examples) to keep assemblies slim and avoid bundling cluster-provided libraries.
- When integrating cloud connectors (AWS/GCP), keep credentials out of the repo and rely on environment-based auth; validate configs in `cloud_aws/` and `cloud_gcp/` before deployment.
