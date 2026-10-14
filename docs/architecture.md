# Architecture and phased learning path

## Standalone pipeline

API pages or a JSONL fixture → immutable raw snapshot → validation → Spark local[2] → versioned Parquet → latest-success pointer → DuckDB + Streamlit.

The CLI owns extraction, validation, transformation, and publication boundaries. Each step accepts an explicit source configuration, run identifier, and data directory. A run identifier identifies a snapshot; use a new identifier to capture a later source revision. Failed work must not replace the latest successful publication.

Raw source records and extraction metadata make a run inspectable and replayable. The normalized permit grain is one jurisdiction/source/permit identifier. This composite identity prevents Seattle's A001 and the synthetic city's A001 from colliding. Source-specific field names belong in configuration; common transformations consume normalized columns. Preserve source status and building classification, including unknowns.

Monthly measures count events by their own dates. An application and an issuance for the same permit are separate lifecycle events, not separate permits to add together. Reported units associated with a permit are not total city housing stock or proof of completed homes. Null units are unknown, not zero. Nonresidential permits remain in the normalized layer without requiring housing-unit fields.

## Phases and parallel waves

1. **Source verification:** independently inspect the local runtime, Seattle schema, and completion-source grain. Record gaps rather than infer dates.
2. **Standalone MVP:** extraction, fixture-driven Spark work, and dashboard scaffolding can proceed together after agreeing on the normalized contract. Integrate them into a single manual run and verify repeatability and failed-publication recovery.
3. **Optional Airflow:** add metadata PostgreSQL and an orchestration service. Tasks invoke the existing CLI stages. Schedule at 06:00 America/Los_Angeles with one active run; the computer and Docker must be running.
4. **Housing and economic enrichment:** completion reconciliation, FRED ingestion, and fixture-driven dashboard views can proceed independently. Integrate only after join-cardinality and monthly-alignment checks.
5. **Extensibility:** the synthetic second jurisdiction proves mapping and identifier separation; commercial records prove that processing does not require residential units. It is a contract demonstration, not a verified live second-city integration.

## Completion data and comparisons

Seattle's building permit API provides a documented completeddate field for inspection completions. The separate quarterly residential issued-and-final dataset is available through an optional diagnostic reconciliation command. Its multi-row-per-permit records are aggregated before comparison, date conflicts are flagged, and discrepancies are never automatically merged into primary totals. No missing completion date may be inferred from a status or issuance date.

Mortgage-rate comparisons show associations, not causal findings. Potential supply is not a completion forecast. Historical event charts use the latest extracted records; observed revision history begins with saved snapshots.

## Operational limits

This is a local educational system, not a production service. Full snapshots capture revisions to older permits but will eventually need retention and extraction-cost controls. Spark runs inside the task container rather than a dedicated cluster. Data stored under a OneDrive-managed folder can be affected by synchronization, eviction, or concurrent edits; keep Docker volumes and large data on a local nonsynced disk when possible.
