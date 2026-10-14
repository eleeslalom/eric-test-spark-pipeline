"""Read-only dashboard for successfully published permit snapshots."""
import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="City permit explorer", layout="wide")
st.title("City permit explorer")
st.caption("Residential permitting and reported housing units. Permit counts and unit counts describe different things.")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))


def read_table(con, path):
    files = list(path.rglob("*.parquet")) if path.is_dir() else [path] if path.exists() else []
    if not files:
        return pd.DataFrame()
    return con.read_parquet([str(p) for p in files], union_by_name=True).df()


pointer = DATA_DIR / "latest.json"
if not pointer.exists():
    st.info("No successful snapshot has been published yet. Run the pipeline, then refresh this page.")
    st.stop()

try:
    latest = json.loads(pointer.read_text())
    version = Path(latest["path"])
    if not version.is_absolute():
        version = DATA_DIR / version
    with duckdb.connect() as con:
        permits = read_table(con, version / "permits")
        monthly = read_table(con, version / "monthly")
        rates = read_table(con, version / "rates.parquet")
    if permits.empty or monthly.empty:
        st.info("The latest snapshot has no permit activity to display.")
        st.stop()
except (OSError, ValueError, KeyError, duckdb.Error) as exc:
    st.error(f"Unable to read the published snapshot: {exc}")
    st.stop()

if latest.get("fixture"):
    st.warning("Synthetic demonstration data — these are not Seattle observations.")
if (version / "reconciliation.json").exists():
    with st.expander("Quarterly housing-source reconciliation"):
        st.json(json.loads((version / "reconciliation.json").read_text()))
st.caption(f"Published run: {latest.get('run_id', version.name)}")
for metadata_path in [version / "manifest.json", DATA_DIR / "raw" / str(latest.get("run_id", "")) / "metadata.json"]:
    if metadata_path.is_file():
        try:
            with st.expander("Source freshness and run metadata"):
                st.json(json.loads(metadata_path.read_text()))
        except (OSError, ValueError):
            pass
        break

monthly["month"] = pd.to_datetime(monthly["month"])
all_types = sorted(permits["building_type"].dropna().astype(str).unique())
residential = [x for x in all_types if x.lower() == "residential"]
selected = st.sidebar.multiselect("Building types", all_types, default=residential or all_types)
permit_types = sorted(permits['permit_type'].dropna().unique()) if 'permit_type' in permits else []
selected_permit_types = st.sidebar.multiselect('Permit types', permit_types, default=[t for t in permit_types if t in ('Building','Demolition')] or permit_types)
if permit_types:
    permits = permits[permits['permit_type'].isin(selected_permit_types)]
    monthly = monthly[monthly['permit_type'].isin(selected_permit_types)]
if monthly.empty:
    st.info('No dated activity matches the selected permit types.')
    st.stop()
start, end = monthly["month"].min().date(), monthly["month"].max().date()
window = st.sidebar.date_input("Activity dates", value=(start, end), min_value=start, max_value=end)
if len(window) != 2 or not selected:
    st.info("Select building types and a complete date range.")
    st.stop()

# Build a continuous series before calculating rolling and annual comparisons.
filtered = monthly[monthly["building_type"].isin(selected)].copy()
metrics = ["permit_count", "units_added", "units_removed", "net_units", "unknown_units"]
for col in metrics:
    filtered[col] = pd.to_numeric(filtered[col], errors="coerce")
by_event = filtered.groupby(["month", "event"])[metrics].sum(min_count=1).reset_index()
visible = by_event[by_event["month"].between(pd.Timestamp(window[0]), pd.Timestamp(window[1]))]
st.subheader("Recorded lifecycle activity")
st.caption("The newest calendar month is partial. Completion means recorded completion of permit inspections.")
st.caption("A permit can appear in more than one lifecycle event. Do not sum application, issuance, and completion counts together.")
counts = visible.groupby("event")["permit_count"].sum()
columns = st.columns(max(1, len(counts)))
for column, (event, value) in zip(columns, counts.items()):
    column.metric(str(event).replace("_", " ").title(), f"{value:,.0f} permits")
if visible.empty:
    st.info("No activity matches these filters.")
else:
    event = st.selectbox("Lifecycle event", sorted(by_event["event"].unique()))
    measure = st.selectbox("Measure", metrics, format_func=lambda x: x.replace("_", " ").title())
    series = by_event[by_event["event"] == event].set_index("month")[[measure]].sort_index()
    series = series.reindex(pd.date_range(series.index.min(), series.index.max(), freq="MS"), fill_value=0)
    series.columns = ["Monthly"]
    series["Rolling 12 months"] = series["Monthly"].rolling(12, min_periods=12).sum()
    series["Year-over-year change (%)"] = series["Monthly"].pct_change(12, fill_method=None).replace([float("inf"), -float("inf")], float("nan")) * 100
    shown = series.loc[str(window[0]):str(window[1])]
    left, right = st.columns(2)
    left.line_chart(shown[["Monthly", "Rolling 12 months"]])
    right.line_chart(shown[["Year-over-year change (%)"]])
    st.dataframe(visible, hide_index=True, use_container_width=True)

st.subheader("Potential supply in the latest snapshot")
st.caption("Current open applications and issued permits, across all dates. These are reported units, not predicted completions; date filters above apply to lifecycle activity only.")
pipeline = permits[permits["building_type"].isin(selected) & permits["stage"].isin(["application", "issued"])].copy()
if pipeline.empty:
    st.info("No open applications or issued permits match the selected building types.")
else:
    pipeline["unknown_units"] = (pipeline["units_added"].isna() | pipeline["units_removed"].isna()).astype(int)
    supply = pipeline.groupby("stage", as_index=False).agg(permits=("permit_id", "size"), reported_units_added=("units_added", "sum"), reported_units_removed=("units_removed", "sum"), unknown_unit_records=("unknown_units", "sum"))
    st.dataframe(supply, hide_index=True, use_container_width=True)

st.subheader("Mortgage-rate comparison")
if rates.empty:
    st.info("Mortgage rates are unavailable in this snapshot. Enable rate ingestion to add the comparison.")
else:
    rates["month"] = pd.to_datetime(rates["month"])
    rate_series = rates.set_index("month")[["mortgage_rate"]].sort_index().loc[str(window[0]):str(window[1])]
    st.line_chart(rate_series)
    st.caption("Monthly average of the reported 30-year mortgage rate, in percent. Comparisons with permit activity show associations and do not establish causation.")

with st.expander("Metric definitions and limitations"):
    st.markdown("""
- **Applications / issuances / completions:** permits with the corresponding source event date in each month; completions are shown only when the source reports them.
- **Units added / removed:** source-reported housing-unit quantities, not numbers of permits or buildings. Net units are added minus removed where both are known.
- **Unknown units:** records missing a reported addition or removal quantity; a missing value is not evidence of zero units. Totals include only reported quantities.
- Historical activity reflects the latest extracted records and can change when the city revises old permits. Status-change observation begins with collected snapshots.
- Partial months remain partial. These data do not measure total city housing stock or homes available for sale or rent.
""")
