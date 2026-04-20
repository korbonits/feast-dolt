# feast-dolt

A [Feast](https://github.com/feast-dev/feast) plugin that uses [Dolt](https://github.com/dolthub/dolt) — the version-controlled SQL database — as an offline store and registry backend.

**Status:** pre-alpha scaffold. Nothing works yet.

## Why

Feature stores need point-in-time correctness, training-set reproducibility, and lineage on feature definitions. Existing offline stores bolt these on with timestamp-join logic and external metadata. Dolt provides them natively:

- **`AS OF` queries** → point-in-time joins without bespoke dedupe logic.
- **Branches** → per-experiment feature pipelines without polluting `main`.
- **Diff + merge** → review feature changes like code; promote dev → prod via merge.
- **Tags** → permanently pin the exact data a model was trained on.

Prior art: [Flock Safety runs a Dolt-backed versioned feature store in production](https://www.dolthub.com/blog/2024-03-07-dolt-flock/) (without Feast). This plugin generalizes that pattern as a reusable Feast backend.

## Scope

- `DoltOfflineStore` — implements the Feast `OfflineStore` interface against a Dolt SQL server.
- `DoltRegistry` *(planned)* — SQL registry backed by Dolt, giving versioned feature definitions.
- **Online serving is out of scope.** Keep using Redis / Milvus / DynamoDB / etc.

## Install (once published)

```bash
pip install feast-dolt
```

For local development:

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Configure

In your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: feast_dolt.DoltOfflineStore
  host: localhost
  port: 3306
  database: my_features
  user: root
  password: ""
  branch: main          # optional — Dolt branch to read from
  as_of: null           # optional — Dolt revision spec for AS OF reads
online_store:
  type: redis           # or any Feast online store
```

## License

Apache-2.0.
