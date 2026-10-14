# Validation — October 14, 2026

- 13 automated tests passed, including actual Spark transformations, repeat runs, rates, supplemental reconciliation, failed-publication preservation, source revisions, pagination, rate-limit retries, and overlapping identifiers in a second jurisdiction.
- Streamlit AppTest rendered both the synthetic fixture publication and the live Seattle publication with no exceptions.
- Live Seattle full extraction: 193,613 source rows; 167,649 normalized records retained for activity since 2010 or undated coverage; 3,462 monthly aggregation rows.
- Live FRED enrichment: 202 monthly mortgage-rate averages. Latest month may be incomplete.
- Docker Compose YAML parsed and standalone build target checked. Docker image builds were not verified during initial implementation.
- Local execution used Apple Silicon, Python 3.12, Spark 3.5.3 and project-local Java 17. Docker targets use Python 3.11 and Java 17; dependency resolution inside those images remains to be verified by the first Docker build.

The live publication is `data/versions/seattle-live-20261014`; raw records are under `data/raw/seattle-live-20261014`. These generated data and local runtimes are ignored by version control.
